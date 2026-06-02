import importlib
import sys
from unittest.mock import patch

import pandas as pd
import pytest


def test_bronze_metadata_success_path(spark_session, tmp_path):
    """
    Test successful Bronze metadata pipeline execution.
    """
    # Set up isolated temporary directories
    landing_metadata_dir = tmp_path / "landing"
    landing_metadata_dir.mkdir(parents=True, exist_ok=True)

    bronze_metadata_dir = tmp_path / "bronze"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    archive_metadata_dir = tmp_path / "archive"
    archive_metadata_dir.mkdir(parents=True, exist_ok=True)

    # Prepare dummy company metadata
    df_dummy = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "short_name": ["Apple Inc."],
            "sector": ["Technology"],
            "industry": ["Consumer Electronics"],
            "country": ["United States"],
            "isin": ["US0378331005"],
            "full_time_employees": [160000],
            "exchange": ["NASDAQ"],
            "market_cap": [2600000000000],
            "currency": ["USD"],
            "dividend_yield": [0.005],
            "extraction_date": ["2026-05-28"],
        }
    )

    # Save to landing directory to test raw ingestion
    df_dummy.to_parquet(landing_metadata_dir / "metadata_2026-05-28.parquet", index=False)

    # Mock environment configuration directories and bypass Spark stop during tests
    with (
        patch("src.producer.config.LANDING_METADATA_DIR", landing_metadata_dir),
        patch("src.producer.config.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.producer.config.ARCHIVE_METADATA_DIR", archive_metadata_dir),
        patch("src.streaming.spark_session.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        # Reload or import the script to trigger module-level execution
        if "src.streaming.bronze_metadata" in sys.modules:
            importlib.reload(sys.modules["src.streaming.bronze_metadata"])
        else:
            importlib.import_module("src.streaming.bronze_metadata")

    # Assert raw parquet files were moved to archive and Delta table was written
    assert len(list(landing_metadata_dir.glob("*.parquet"))) == 0
    assert len(list(archive_metadata_dir.glob("*.parquet"))) == 1

    df_bronze = spark_session.read.format("delta").load(str(bronze_metadata_dir))
    assert df_bronze.count() == 1
    assert df_bronze.filter(df_bronze.ticker == "AAPL").count() == 1
    assert "ingestion_timestamp" in df_bronze.columns
    assert df_bronze.filter(df_bronze.ingestion_timestamp.isNotNull()).count() == 1


def test_bronze_metadata_empty_landing(spark_session, tmp_path):
    """
    Test clean exit (code 0) when metadata directory is empty.
    """
    # Set up isolated temporary directories
    landing_metadata_dir = tmp_path / "landing"
    landing_metadata_dir.mkdir(parents=True, exist_ok=True)

    bronze_metadata_dir = tmp_path / "bronze"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    archive_metadata_dir = tmp_path / "archive"
    archive_metadata_dir.mkdir(parents=True, exist_ok=True)

    # Mock config paths and Spark session
    with (
        patch("src.producer.config.LANDING_METADATA_DIR", landing_metadata_dir),
        patch("src.producer.config.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.producer.config.ARCHIVE_METADATA_DIR", archive_metadata_dir),
        patch("src.streaming.spark_session.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        # Capture SystemExit 0 when running script against empty landing directory
        with pytest.raises(SystemExit) as exc_info:
            if "src.streaming.bronze_metadata" in sys.modules:
                importlib.reload(sys.modules["src.streaming.bronze_metadata"])
            else:
                importlib.import_module("src.streaming.bronze_metadata")

    # Assert clean exit code and that no files were written or archived
    assert exc_info.value.code == 0
    assert len(list(archive_metadata_dir.glob("*.parquet"))) == 0
    assert len(list(landing_metadata_dir.glob("*.parquet"))) == 0
    assert len(list(bronze_metadata_dir.glob("**/*.parquet"))) == 0


def test_bronze_metadata_processing_failure(spark_session, tmp_path):
    """
    Test exit code 1 when writing to the bronze metadata directory fails.
    """
    # Set up isolated temporary directories
    landing_metadata_dir = tmp_path / "landing"
    landing_metadata_dir.mkdir(parents=True, exist_ok=True)

    bronze_metadata_dir = tmp_path / "bronze"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    archive_metadata_dir = tmp_path / "archive"
    archive_metadata_dir.mkdir(parents=True, exist_ok=True)

    # Prepare dummy company metadata
    df_dummy = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "short_name": ["Apple Inc."],
            "sector": ["Technology"],
            "industry": ["Consumer Electronics"],
            "country": ["United States"],
            "isin": ["US0378331005"],
            "full_time_employees": [160000],
            "exchange": ["NASDAQ"],
            "market_cap": [2600000000000],
            "currency": ["USD"],
            "dividend_yield": [0.005],
            "extraction_date": ["2026-05-28"],
        }
    )

    df_dummy.to_parquet(landing_metadata_dir / "metadata_2026-05-28.parquet", index=False)

    from loguru import logger

    # Add a dynamic sink to loguru to capture ERROR logs
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="ERROR")

    try:
        # Inject a write exception to simulate a writing failure
        with (
            patch("src.producer.config.LANDING_METADATA_DIR", landing_metadata_dir),
            patch("src.producer.config.BRONZE_METADATA_DIR", bronze_metadata_dir),
            patch("src.producer.config.ARCHIVE_METADATA_DIR", archive_metadata_dir),
            patch("src.streaming.spark_session.create_spark_session", return_value=spark_session),
            patch("src.streaming.utils.write_delta_table", side_effect=Exception("Simulated writing failure")),
            patch.object(spark_session, "stop"),
        ):
            with pytest.raises(SystemExit) as exc_info:
                if "src.streaming.bronze_metadata" in sys.modules:
                    importlib.reload(sys.modules["src.streaming.bronze_metadata"])
                else:
                    importlib.import_module("src.streaming.bronze_metadata")
    finally:
        logger.remove(sink_id)

    # Assert exit code 1 and transactional safety (input preserved, target folders empty)
    assert exc_info.value.code == 1
    assert len(list(landing_metadata_dir.glob("*.parquet"))) == 1
    assert len(list(archive_metadata_dir.glob("*.parquet"))) == 0
    assert len(list(bronze_metadata_dir.glob("**/*.parquet"))) == 0

    # Verify that the exception is logged to our loguru sink
    log_content = "".join(captured_logs)
    assert "Failed during Bronze metadata pipeline execution" in log_content
    assert "Simulated writing failure" in log_content
