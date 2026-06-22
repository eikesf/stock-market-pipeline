from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

import pandas as pd
import pytest
from loguru import logger
from pyspark.sql.types import DateType, DecimalType, LongType, StringType, TimestampType

from src.streaming.silver import main


def test_silver_prices_cleaning_and_casting(spark_session, tmp_path):
    """
    Test that the Silver prices pipeline correctly cleans and casts columns.
    """
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    silver_dir = tmp_path / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    df_bronze = pd.DataFrame(
        {
            "date": ["2026-05-28"],
            "ticker": ["   msft   "],
            "open": [170.5],
            "high": [172.5],
            "low": [168.5],
            "close": [171.55],
            "adj_close": [171.55],
            "volume": [10000],
            "dividends": [0.5],
            "stock_splits": [0.12345],
            "ingestion_timestamp": ["2026-05-28 10:00:00"],
        }
    )

    df_bronze_spark = spark_session.createDataFrame(df_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_dir))

    with (
        patch("src.streaming.silver.BRONZE_PRICES_DIR", bronze_dir),
        patch("src.streaming.silver.SILVER_PRICES_DIR", silver_dir),
        patch("src.streaming.silver.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        main()

    df_silver = spark_session.read.format("delta").load(str(silver_dir))
    row = df_silver.collect()[0]

    assert row.ticker == "MSFT"
    assert row.date == date(2026, 5, 28)
    assert row.open == Decimal("170.50")
    assert row.high == Decimal("172.50")
    assert row.low == Decimal("168.50")
    assert row.close == Decimal("171.55")
    assert row.adj_close == Decimal("171.55")
    assert row.volume == 10000
    assert row.dividends == Decimal("0.50")
    assert row.stock_splits == Decimal("0.1235")
    assert row.ingestion_timestamp == datetime(2026, 5, 28, 10, 0, 0)
    assert df_silver.count() == 1

    assert isinstance(df_silver.schema["date"].dataType, DateType)
    assert isinstance(df_silver.schema["ticker"].dataType, StringType)
    assert isinstance(df_silver.schema["ingestion_timestamp"].dataType, TimestampType)
    assert isinstance(df_silver.schema["volume"].dataType, LongType)

    decimal_2_cols = ["open", "high", "low", "close", "adj_close", "dividends"]
    for col_name in decimal_2_cols:
        assert isinstance(df_silver.schema[col_name].dataType, DecimalType)
        assert df_silver.schema[col_name].dataType.precision == 10
        assert df_silver.schema[col_name].dataType.scale == 2

    assert isinstance(df_silver.schema["stock_splits"].dataType, DecimalType)
    assert df_silver.schema["stock_splits"].dataType.precision == 10
    assert df_silver.schema["stock_splits"].dataType.scale == 4


def test_silver_prices_null_dropping(spark_session, tmp_path):
    """
    Test that rows containing null values in critical columns are filtered out.
    """
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    silver_dir = tmp_path / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    df_bronze = pd.DataFrame(
        {
            "date": ["2026-05-28", "2026-05-28"],
            "ticker": [" aapl ", " MSFT "],
            "open": [None, 350.0],
            "high": [172.5, 355.0],
            "low": [168.5, 348.0],
            "close": [None, 352.0],
            "adj_close": [171.5, 352.0],
            "volume": [10000, 20000],
            "dividends": [0.5, 0.0],
            "stock_splits": [0.0, 0.0],
            "ingestion_timestamp": ["2026-05-28 10:00:00", "2026-05-28 10:00:00"],
        }
    )

    df_bronze_spark = spark_session.createDataFrame(df_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_dir))

    with (
        patch("src.streaming.silver.BRONZE_PRICES_DIR", bronze_dir),
        patch("src.streaming.silver.SILVER_PRICES_DIR", silver_dir),
        patch("src.streaming.silver.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        main()

    df_silver = spark_session.read.format("delta").load(str(silver_dir))
    assert df_silver.count() == 1
    assert df_silver.collect()[0].ticker == "MSFT"


def test_silver_prices_deduplication(spark_session, tmp_path):
    """
    Test that multiple records with the same ticker and date are deduplicated.
    """
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    silver_dir = tmp_path / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    df_bronze = pd.DataFrame(
        {
            "date": ["2026-05-28", "2026-05-28"],
            "ticker": ["AAPL", "AAPL"],
            "open": [170.5, 171.5],
            "high": [172.5, 355.0],
            "low": [168.5, 348.0],
            "close": [171.5, 352.0],
            "adj_close": [171.5, 352.0],
            "volume": [10000, 20000],
            "dividends": [0.5, 0.0],
            "stock_splits": [0.0, 0.0],
            "ingestion_timestamp": ["2026-05-28 10:00:00", "2026-05-28 10:15:00"],
        }
    )

    df_bronze_spark = spark_session.createDataFrame(df_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_dir))

    with (
        patch("src.streaming.silver.BRONZE_PRICES_DIR", bronze_dir),
        patch("src.streaming.silver.SILVER_PRICES_DIR", silver_dir),
        patch("src.streaming.silver.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        main()

    df_silver = spark_session.read.format("delta").load(str(silver_dir))
    assert df_silver.count() == 1
    assert df_silver.collect()[0].ticker == "AAPL"
    assert df_silver.collect()[0].close == 352.0
    assert df_silver.collect()[0].ingestion_timestamp.strftime("%Y-%m-%d %H:%M:%S") == "2026-05-28 10:15:00"


def test_silver_prices_failure(spark_session, tmp_path):
    """
    Test exit code 1 when writing to the Silver prices table fails.
    """
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    silver_dir = tmp_path / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    df_bronze = pd.DataFrame(
        {
            "date": ["2026-05-28"],
            "ticker": [" aapl "],
            "open": [170.5],
            "high": [172.5],
            "low": [168.5],
            "close": [171.5],
            "adj_close": [171.5],
            "volume": [10000],
            "dividends": [0.5],
            "stock_splits": [0.0],
            "ingestion_timestamp": ["2026-05-28 10:00:00"],
        }
    )

    df_bronze_spark = spark_session.createDataFrame(df_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_dir))

    # Capture logs to assert expected error messages on pipeline failure
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="ERROR")

    try:
        with (
            patch("src.streaming.silver.BRONZE_PRICES_DIR", bronze_dir),
            patch("src.streaming.silver.SILVER_PRICES_DIR", silver_dir),
            patch("src.streaming.silver.create_spark_session", return_value=spark_session),
            patch("src.streaming.silver.write_delta_table", side_effect=Exception("Simulated writing failure")),
            patch.object(spark_session, "stop"),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()
    finally:
        logger.remove(sink_id)

    assert exc_info.value.code == 1
    assert len(list(silver_dir.glob("**/*.parquet"))) == 0

    log_content = "".join(captured_logs)
    assert "Failed to process Silver layer" in log_content
    assert "Simulated writing failure" in log_content


def test_silver_date_from_arguments(spark_session, tmp_path):
    """
    Test that Silver prices pipeline parses --date from CLI arguments correctly.
    """
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    silver_dir = tmp_path / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    df_bronze = pd.DataFrame(
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
            "ingestion_timestamp": ["2026-05-28 10:00:00"],
        }
    )

    df_bronze_spark = spark_session.createDataFrame(df_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_dir))

    with (
        patch("src.streaming.silver.BRONZE_PRICES_DIR", bronze_dir),
        patch("src.streaming.silver.SILVER_PRICES_DIR", silver_dir),
        patch("src.streaming.silver.create_spark_session", return_value=spark_session),
        patch("sys.argv", ["silver.py", "--date", "2026-05-28"]),
        patch.object(spark_session, "stop"),
    ):
        main()

    df_silver = spark_session.read.format("delta").load(str(silver_dir))
    assert df_silver.count() == 1


def test_silver_invalid_date_format(spark_session, tmp_path):
    """
    Test that an invalid date format passed to --date exits with code 1.
    """
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    silver_dir = tmp_path / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    # Capture logs to assert expected error messages on pipeline failure
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="ERROR")

    try:
        with (
            patch("src.streaming.silver.BRONZE_PRICES_DIR", bronze_dir),
            patch("src.streaming.silver.SILVER_PRICES_DIR", silver_dir),
            patch("src.streaming.silver.create_spark_session", return_value=spark_session),
            patch("sys.argv", ["silver.py", "--date", "invalid_date_format"]),
            patch.object(spark_session, "stop"),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()
    finally:
        logger.remove(sink_id)

    assert exc_info.value.code == 1
    log_content = "".join(captured_logs)
    assert "Invalid date format" in log_content
