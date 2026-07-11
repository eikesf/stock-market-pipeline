from unittest.mock import patch

import pandas as pd
import pytest
from loguru import logger

from src.streaming.bronze import main


def test_bronze_success_path(spark_session, tmp_path):
    """
    Test successful Bronze prices pipeline execution.
    """
    landing_dir = tmp_path / "landing"
    landing_dir.mkdir(parents=True, exist_ok=True)

    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    archive_dir = tmp_path / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    df_dummy = pd.DataFrame(
        {
            "date": ["2026-05-28"],
            "ticker": ["AAPL"],
            "open": [150.0],
            "high": [152.0],
            "low": [149.0],
            "close": [151.0],
            "adj_close": [151.0],
            "volume": [1000],
            "dividends": [0.0],
            "stock_splits": [0.0],
        }
    )

    df_dummy.to_parquet(landing_dir / "tickers_2026-05-28.parquet", index=False)

    with (
        patch("src.streaming.bronze.LANDING_PRICES_DIR", landing_dir),
        patch("src.streaming.bronze.BRONZE_PRICES_DIR", bronze_dir),
        patch("src.streaming.bronze.ARCHIVE_PRICES_DIR", archive_dir),
        patch("src.streaming.utils.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        main()

    assert len(list(landing_dir.glob("*.parquet"))) == 0
    assert len(list(archive_dir.glob("*.parquet"))) == 1

    df_bronze = spark_session.read.format("delta").load(str(bronze_dir))
    assert df_bronze.count() == 1
    assert df_bronze.filter(df_bronze.ticker == "AAPL").count() == 1
    assert "ingestion_timestamp" in df_bronze.columns
    assert df_bronze.filter(df_bronze.ingestion_timestamp.isNotNull()).count() == 1


def test_bronze_empty_landing(spark_session, tmp_path):
    """
    Test clean exit (code 0) when landing directory is empty.
    """
    landing_dir = tmp_path / "landing"
    landing_dir.mkdir(parents=True, exist_ok=True)

    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    archive_dir = tmp_path / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    with (
        patch("src.streaming.bronze.LANDING_PRICES_DIR", landing_dir),
        patch("src.streaming.bronze.BRONZE_PRICES_DIR", bronze_dir),
        patch("src.streaming.bronze.ARCHIVE_PRICES_DIR", archive_dir),
        patch("src.streaming.utils.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        main()

    assert len(list(archive_dir.glob("*.parquet"))) == 0
    assert len(list(landing_dir.glob("*.parquet"))) == 0
    assert len(list(bronze_dir.glob("**/*.parquet"))) == 0


def test_bronze_processing_failure(spark_session, tmp_path):
    """
    Test exit code 1 when writing to the Delta table fails.
    """
    landing_dir = tmp_path / "landing"
    landing_dir.mkdir(parents=True, exist_ok=True)

    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    archive_dir = tmp_path / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    df_dummy = pd.DataFrame(
        {
            "date": ["2026-05-28"],
            "ticker": ["AAPL"],
            "open": [150.0],
            "high": [152.0],
            "low": [149.0],
            "close": [151.0],
            "adj_close": [151.0],
            "volume": [1000],
            "dividends": [0.0],
            "stock_splits": [0.0],
        }
    )

    df_dummy.to_parquet(landing_dir / "tickers_2026-05-28.parquet", index=False)

    # Capture logs to assert expected error messages on pipeline failure
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="ERROR")

    try:
        with (
            patch("src.streaming.bronze.LANDING_PRICES_DIR", landing_dir),
            patch("src.streaming.bronze.BRONZE_PRICES_DIR", bronze_dir),
            patch("src.streaming.bronze.ARCHIVE_PRICES_DIR", archive_dir),
            patch("src.streaming.utils.create_spark_session", return_value=spark_session),
            patch("src.streaming.utils.write_delta_table", side_effect=Exception("Simulated writing failure")),
            patch.object(spark_session, "stop"),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()
    finally:
        logger.remove(sink_id)

    assert exc_info.value.code == 1
    assert len(list(landing_dir.glob("*.parquet"))) == 1
    assert len(list(archive_dir.glob("*.parquet"))) == 0
    assert len(list(bronze_dir.glob("**/*.parquet"))) == 0

    log_content = "".join(captured_logs)
    assert "Failed during Bronze prices pipeline execution" in log_content
    assert "Simulated writing failure" in log_content


def test_bronze_date_from_arguments(spark_session, tmp_path):
    """
    Test that Bronze prices pipeline parses --date from CLI arguments correctly.
    """
    landing_dir = tmp_path / "landing"
    landing_dir.mkdir(parents=True, exist_ok=True)

    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    archive_dir = tmp_path / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    df_dummy = pd.DataFrame(
        {
            "date": ["2026-05-28"],
            "ticker": ["AAPL"],
            "open": [150.0],
            "high": [152.0],
            "low": [149.0],
            "close": [151.0],
            "adj_close": [151.0],
            "volume": [1000],
            "dividends": [0.0],
            "stock_splits": [0.0],
        }
    )

    df_dummy.to_parquet(landing_dir / "tickers_2026-05-28.parquet", index=False)

    with (
        patch("src.streaming.bronze.LANDING_PRICES_DIR", landing_dir),
        patch("src.streaming.bronze.BRONZE_PRICES_DIR", bronze_dir),
        patch("src.streaming.bronze.ARCHIVE_PRICES_DIR", archive_dir),
        patch("src.streaming.utils.create_spark_session", return_value=spark_session),
        patch("sys.argv", ["bronze.py", "--date", "2026-05-28"]),
        patch.object(spark_session, "stop"),
    ):
        main()

    assert len(list(landing_dir.glob("*.parquet"))) == 0
    assert len(list(archive_dir.glob("*.parquet"))) == 1


def test_bronze_invalid_date_format(spark_session, tmp_path):
    """
    Test that an invalid date format passed to --date exits with code 1.
    """
    landing_dir = tmp_path / "landing"
    landing_dir.mkdir(parents=True, exist_ok=True)

    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    archive_dir = tmp_path / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    # Capture logs to assert expected error messages on pipeline failure
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="ERROR")

    try:
        with (
            patch("src.streaming.bronze.LANDING_PRICES_DIR", landing_dir),
            patch("src.streaming.bronze.BRONZE_PRICES_DIR", bronze_dir),
            patch("src.streaming.bronze.ARCHIVE_PRICES_DIR", archive_dir),
            patch("src.streaming.utils.create_spark_session", return_value=spark_session),
            patch("sys.argv", ["bronze.py", "--date", "invalid_date_format"]),
            patch.object(spark_session, "stop"),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()
    finally:
        logger.remove(sink_id)

    assert exc_info.value.code == 1
    log_content = "".join(captured_logs)
    assert "Invalid date format" in log_content
