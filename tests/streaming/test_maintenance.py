from unittest.mock import MagicMock, patch

import pytest

from src.streaming.maintenance import main, run_maintenance


@patch("src.streaming.maintenance.create_spark_session")
@patch("src.streaming.maintenance.DeltaTable")
@patch("src.streaming.maintenance.Path.exists")
def test_run_maintenance_success(mock_exists, mock_delta_table_cls, mock_create_spark_session):
    """
    Test that run_maintenance optimizes and vacuums all 4 tables when they exist.
    """
    mock_spark = MagicMock()
    mock_create_spark_session.return_value = mock_spark
    mock_exists.return_value = True

    mock_dt = MagicMock()
    mock_delta_table_cls.forPath.return_value = mock_dt

    run_maintenance(24.0)

    # Assert spark session is created and stopped
    mock_create_spark_session.assert_called_once()
    mock_spark.stop.assert_called_once()

    # Assert retention config is set
    mock_spark.conf.set.assert_called_once_with("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    # 2 Bronze tables, 3 Silver tables and their 3 quarantine tables
    expected_tables = 8

    assert mock_delta_table_cls.forPath.call_count == expected_tables

    # Assert compaction and vacuum are called for every table
    assert mock_dt.optimize.return_value.executeCompaction.call_count == expected_tables
    assert mock_dt.vacuum.call_count == expected_tables
    mock_dt.vacuum.assert_called_with(24.0)


@patch("src.streaming.maintenance.create_spark_session")
@patch("src.streaming.maintenance.DeltaTable")
@patch("src.streaming.maintenance.Path.exists")
def test_run_maintenance_skips_when_no_delta_log(mock_exists, mock_delta_table_cls, mock_create_spark_session):
    """
    Test that run_maintenance skips optimization and vacuuming if Delta tables do not exist.
    """
    mock_spark = MagicMock()
    mock_create_spark_session.return_value = mock_spark
    mock_exists.return_value = False

    run_maintenance(168.0)

    # Assert spark session is created and stopped
    mock_create_spark_session.assert_called_once()
    mock_spark.stop.assert_called_once()

    # Assert DeltaTable.forPath is not called
    mock_delta_table_cls.forPath.assert_not_called()


@patch("src.streaming.maintenance.create_spark_session")
def test_run_maintenance_handles_exception(mock_create_spark_session):
    """
    Test that run_maintenance handles exceptions and stops the Spark session.
    """
    mock_spark = MagicMock()
    mock_create_spark_session.return_value = mock_spark

    # Force an exception during execution (e.g. mock_spark.conf.set raises exception)
    mock_spark.conf.set.side_effect = Exception("Test Spark configuration error")

    with pytest.raises(SystemExit) as excinfo:
        run_maintenance(168.0)

    assert excinfo.value.code == 1
    mock_spark.stop.assert_called_once()


@patch("src.streaming.maintenance.run_maintenance")
@patch("sys.argv", ["maintenance.py", "--retention", "48"])
def test_main(mock_run_maintenance):
    """
    Test the main function parsing argument and calling run_maintenance.
    """
    main()
    mock_run_maintenance.assert_called_once_with(48.0)


@patch("src.streaming.maintenance.check_and_heal_corrupt_data_file")
@patch("src.streaming.maintenance.DeltaTable")
@patch("pathlib.Path.exists")
@patch("src.streaming.maintenance.create_spark_session")
def test_run_maintenance_isolates_a_failing_table(
    mock_create_spark_session, mock_exists, mock_delta_table_cls, mock_heal
):
    """Test that one corrupt table does not abort maintenance for the remaining tables.

    Regression test: the loop previously shared a single try/except, so the first failure skipped
    OPTIMIZE and VACUUM for every table after it. That went unnoticed only because the corrupt
    table happened to be last in the list.
    """
    mock_spark = MagicMock()
    mock_create_spark_session.return_value = mock_spark
    mock_exists.return_value = True
    mock_heal.return_value = True

    healthy = MagicMock()
    corrupt = MagicMock()
    corrupt.optimize.return_value.executeCompaction.side_effect = Exception(
        "[FAILED_READ_FILE.NO_HINT] Encountered error while reading file "
        "part-00000-0913d207-ad5c-4111-b1eb-30e3ed00412b-c000.snappy.parquet"
    )
    # Third table in the list is the corrupt one; the rest must still be processed.
    mock_delta_table_cls.forPath.side_effect = [healthy, healthy, corrupt, healthy, healthy, healthy, healthy, healthy]

    with pytest.raises(RuntimeError, match="Delta maintenance failed for 1 table"):
        run_maintenance(24.0, raise_on_error=True)

    # Every table was still visited despite the failure in the middle
    assert mock_delta_table_cls.forPath.call_count == 8
    # The corrupt table triggered a healing attempt
    mock_heal.assert_called_once()
    # Healthy tables were still vacuumed (7 of the 8 tables)
    assert healthy.vacuum.call_count == 7
