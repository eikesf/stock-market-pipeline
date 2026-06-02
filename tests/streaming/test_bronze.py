import importlib
import sys
from unittest.mock import patch

import pandas as pd
import pytest


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
        patch("src.producer.config.LANDING_PRICES_DIR", landing_dir),
        patch("src.producer.config.BRONZE_PRICES_DIR", bronze_dir),
        patch("src.producer.config.ARCHIVE_PRICES_DIR", archive_dir),
        patch("src.streaming.spark_session.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        if "src.streaming.bronze" in sys.modules:
            importlib.reload(sys.modules["src.streaming.bronze"])
        else:
            importlib.import_module("src.streaming.bronze")

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
        patch("src.producer.config.LANDING_PRICES_DIR", landing_dir),
        patch("src.producer.config.BRONZE_PRICES_DIR", bronze_dir),
        patch("src.producer.config.ARCHIVE_PRICES_DIR", archive_dir),
        patch("src.streaming.spark_session.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        with pytest.raises(SystemExit) as exc_info:
            if "src.streaming.bronze" in sys.modules:
                importlib.reload(sys.modules["src.streaming.bronze"])
            else:
                importlib.import_module("src.streaming.bronze")

    assert exc_info.value.code == 0
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

    from loguru import logger

    # Capture logs to assert expected error messages on pipeline failure
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="ERROR")

    try:
        with (
            patch("src.producer.config.LANDING_PRICES_DIR", landing_dir),
            patch("src.producer.config.BRONZE_PRICES_DIR", bronze_dir),
            patch("src.producer.config.ARCHIVE_PRICES_DIR", archive_dir),
            patch("src.streaming.spark_session.create_spark_session", return_value=spark_session),
            patch("src.streaming.utils.write_delta_table", side_effect=Exception("Simulated writing failure")),
            patch.object(spark_session, "stop"),
        ):
            with pytest.raises(SystemExit) as exc_info:
                if "src.streaming.bronze" in sys.modules:
                    importlib.reload(sys.modules["src.streaming.bronze"])
                else:
                    importlib.import_module("src.streaming.bronze")
    finally:
        logger.remove(sink_id)

    assert exc_info.value.code == 1
    assert len(list(landing_dir.glob("*.parquet"))) == 1
    assert len(list(archive_dir.glob("*.parquet"))) == 0
    assert len(list(bronze_dir.glob("**/*.parquet"))) == 0

    log_content = "".join(captured_logs)
    assert "Failed during Bronze prices pipeline execution" in log_content
    assert "Simulated writing failure" in log_content
