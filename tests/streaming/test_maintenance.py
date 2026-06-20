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

    # Assert DeltaTable.forPath is called for all 4 tables
    assert mock_delta_table_cls.forPath.call_count == 4

    # Assert compaction and vacuum are called for all 4 tables
    assert mock_dt.optimize.return_value.executeCompaction.call_count == 4
    assert mock_dt.vacuum.call_count == 4
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
