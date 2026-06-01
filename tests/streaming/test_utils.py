import os
from unittest.mock import MagicMock, patch

from src.streaming.utils import get_clickhouse_client, read_delta_table, write_delta_table


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

    # Mock OS environment variables during client creation
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

    # Mock the Spark format and load calls
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

    # Mock the DataFrame write path
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

    # Mock the DataFrame write path
    mock_df.write.format.return_value.mode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer

    write_delta_table(mock_df, "dummy_path", mode="overwrite")

    mock_df.write.format.assert_called_once_with("delta")
    mock_df.write.format.return_value.mode.assert_called_once_with("overwrite")
    mock_writer.option.assert_called_once_with("overwriteSchema", "true")
    mock_writer.save.assert_called_once_with("dummy_path")
