import json
import os
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from src.streaming.utils import (
    check_and_heal_corrupt_data_file,
    extract_corrupt_parquet_filename,
    find_version_introducing_file,
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


def test_extract_corrupt_parquet_filename():
    error_msg = "Encountered error while reading file file:/opt/airflow/data/bronze/prices/part-00000-e9315c03-ba78-44f0-8b12-2d7dfd694672-c000.snappy.parquet. SQLSTATE: KD001"
    fn = extract_corrupt_parquet_filename(error_msg)
    assert fn == "part-00000-e9315c03-ba78-44f0-8b12-2d7dfd694672-c000.snappy.parquet"

    # Test no match
    assert extract_corrupt_parquet_filename("some other error") is None


def test_find_version_introducing_file(tmp_path):
    log_dir = tmp_path / "_delta_log"
    log_dir.mkdir()

    # Create dummy commit files
    # Commit 126
    c126 = log_dir / "00000000000000000126.json"
    with open(c126, "w", encoding="utf-8") as f:
        f.write('{"add":{"path":"part-00000-abc.parquet","size":123}}\n')

    # Commit 127
    c127 = log_dir / "00000000000000000127.json"
    with open(c127, "w", encoding="utf-8") as f:
        f.write('{"add":{"path":"part-00000-corrupt.parquet","size":456}}\n')

    v = find_version_introducing_file(tmp_path, "part-00000-corrupt.parquet")
    assert v == 127

    v_missing = find_version_introducing_file(tmp_path, "missing.parquet")
    assert v_missing is None


def test_check_and_heal_corrupt_data_file(tmp_path):
    # Setup log dir and files
    log_dir = tmp_path / "_delta_log"
    log_dir.mkdir()

    corrupt_fn = "part-00000-corrupt.parquet"
    corrupt_file = tmp_path / corrupt_fn
    with open(corrupt_file, "w") as f:
        f.write("bad binary data")

    # Create .crc file too
    crc_file = tmp_path / f".{corrupt_fn}.crc"
    with open(crc_file, "w") as f:
        f.write("crc data")

    # Write commit JSON adding the file
    with open(log_dir / "00000000000000000127.json", "w", encoding="utf-8") as f:
        f.write(f'{{"add":{{"path":"{corrupt_fn}","size":456}}}}\n')

    # Mock Spark and DeltaTable
    mock_spark = MagicMock()
    mock_dt = MagicMock()

    error_msg = f"Encountered error reading file {corrupt_fn}"

    with patch("delta.tables.DeltaTable.forPath", return_value=mock_dt):
        healed = check_and_heal_corrupt_data_file([tmp_path], error_msg, mock_spark)

    assert healed is True
    # Assert it called restoreToVersion with prev version (126)
    mock_dt.restoreToVersion.assert_called_once_with(126)
    # Assert the physical files were deleted
    assert not corrupt_file.exists()
    assert not crc_file.exists()
