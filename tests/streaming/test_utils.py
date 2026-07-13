import json
import os
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from src.streaming.utils import (
    get_clickhouse_client,
    heal_corrupt_delta_checkpoints,
    read_delta_table,
    write_delta_table,
)


@patch("clickhouse_connect.get_client")
def test_get_clickhouse_client(mock_get_client):
    """
    Test that the ClickHouse client is initialized with configurations from environment variables.
    """
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    env_vars = {
        "CLICKHOUSE_HOST": "localhost",
        "CLICKHOUSE_PORT": "9000",
        "CLICKHOUSE_USER": "test_user",
        "CLICKHOUSE_PASSWORD": "test_password",
        "CLICKHOUSE_DB": "test_db",
    }

    with patch.dict(os.environ, env_vars):
        client = get_clickhouse_client()

    assert client == mock_client
    mock_get_client.assert_called_once_with(
        host="localhost", port="9000", username="test_user", password="test_password", database="test_db"
    )


def test_read_delta_table():
    """
    Test that reading a Delta table invokes Spark's read API with the correct format and path.
    """
    mock_spark = MagicMock()
    mock_df = MagicMock()

    mock_spark.read.format.return_value.load.return_value = mock_df

    res = read_delta_table(mock_spark, "dummy_path")

    assert res == mock_df
    mock_spark.read.format.assert_called_once_with("delta")
    mock_spark.read.format.return_value.load.assert_called_once_with("dummy_path")


def test_write_delta_table_append():
    """
    Test that writing a Delta table in append mode configures the mergeSchema option.
    """
    mock_df = MagicMock()
    mock_writer = MagicMock()

    mock_df.write.format.return_value.mode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer

    write_delta_table(mock_df, "dummy_path", mode="append")

    mock_df.write.format.assert_called_once_with("delta")
    mock_df.write.format.return_value.mode.assert_called_once_with("append")
    mock_writer.option.assert_called_once_with("mergeSchema", "true")
    mock_writer.save.assert_called_once_with("dummy_path")


def test_write_delta_table_overwrite():
    """
    Test that writing a Delta table in overwrite mode configures the overwriteSchema option.
    """
    mock_df = MagicMock()
    mock_writer = MagicMock()

    mock_df.write.format.return_value.mode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer

    write_delta_table(mock_df, "dummy_path", mode="overwrite")

    mock_df.write.format.assert_called_once_with("delta")
    mock_df.write.format.return_value.mode.assert_called_once_with("overwrite")
    mock_writer.option.assert_called_once_with("overwriteSchema", "true")
    mock_writer.save.assert_called_once_with("dummy_path")


def test_heal_corrupt_delta_checkpoints_no_corruption(tmp_path):
    """Test that a healthy checkpoint is not modified."""
    log_dir = tmp_path / "_delta_log"
    log_dir.mkdir()

    # Write a valid checkpoint file
    checkpoint_file = log_dir / "00000000000000000100.checkpoint.parquet"
    table = pa.table({"col": [1, 2, 3]})
    pq.write_table(table, str(checkpoint_file))

    # Write _last_checkpoint
    last_checkpoint_file = log_dir / "_last_checkpoint"
    checkpoint_info = {
        "version": 100,
        "size": 12,
        "sizeInBytes": checkpoint_file.stat().st_size,
        "numOfAddFiles": 1,
    }
    with open(last_checkpoint_file, "w") as f:
        json.dump(checkpoint_info, f)

    # Run healing
    heal_corrupt_delta_checkpoints(tmp_path)

    # Assert nothing was deleted
    assert checkpoint_file.exists()
    assert last_checkpoint_file.exists()
    with open(last_checkpoint_file) as f:
        data = json.load(f)
    assert data["version"] == 100


def test_heal_corrupt_delta_checkpoints_heals_to_previous(tmp_path):
    """Test healing when current checkpoint is corrupt but a previous valid one exists."""
    log_dir = tmp_path / "_delta_log"
    log_dir.mkdir()

    # Write a valid v100 checkpoint file
    prev_checkpoint = log_dir / "00000000000000000100.checkpoint.parquet"
    table = pa.table({"col": [1, 2, 3]})
    pq.write_table(table, str(prev_checkpoint))

    # Write a corrupted v110 checkpoint file
    corrupt_checkpoint = log_dir / "00000000000000000110.checkpoint.parquet"
    with open(corrupt_checkpoint, "wb") as f:
        f.write(b"corrupt header thrift data")

    # Write _last_checkpoint pointing to corrupt v110
    last_checkpoint_file = log_dir / "_last_checkpoint"
    checkpoint_info = {"version": 110, "size": 12, "sizeInBytes": 100, "numOfAddFiles": 1}
    with open(last_checkpoint_file, "w") as f:
        json.dump(checkpoint_info, f)

    # Run healing
    heal_corrupt_delta_checkpoints(tmp_path)

    # Assert corrupt file was deleted
    assert not corrupt_checkpoint.exists()
    # Assert previous valid file still exists
    assert prev_checkpoint.exists()
    # Assert _last_checkpoint was updated to v100
    with open(last_checkpoint_file) as f:
        data = json.load(f)
    assert data["version"] == 100


def test_heal_corrupt_delta_checkpoints_deletes_last_checkpoint_when_no_prev(tmp_path):
    """Test healing when current checkpoint is corrupt and no valid previous checkpoint exists."""
    log_dir = tmp_path / "_delta_log"
    log_dir.mkdir()

    # Write a corrupted v110 checkpoint file
    corrupt_checkpoint = log_dir / "00000000000000000110.checkpoint.parquet"
    with open(corrupt_checkpoint, "wb") as f:
        f.write(b"corrupt header thrift data")

    # Write _last_checkpoint pointing to corrupt v110
    last_checkpoint_file = log_dir / "_last_checkpoint"
    checkpoint_info = {"version": 110, "size": 12, "sizeInBytes": 100, "numOfAddFiles": 1}
    with open(last_checkpoint_file, "w") as f:
        json.dump(checkpoint_info, f)

    # Run healing
    heal_corrupt_delta_checkpoints(tmp_path)

    # Assert corrupt file was deleted
    assert not corrupt_checkpoint.exists()
    # Assert _last_checkpoint was deleted entirely to trigger log replay fallback
    assert not last_checkpoint_file.exists()
