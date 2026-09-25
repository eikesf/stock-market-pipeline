from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

import pandas as pd
import pytest
from loguru import logger
from pyspark.sql.types import DateType, DecimalType, LongType, StringType, TimestampType

from src.streaming.quality_rules import RequiredColumnMissingError
from src.streaming.silver import main, run_silver


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
            "date": ["2026-05-28", "2026-05-28"],
            "ticker": ["   msft   ", "  usdbrl=x  "],
            "open": [170.5, 5.20],
            "high": [172.5, 5.22],
            "low": [168.5, 5.18],
            "close": [171.55, 5.21],
            "adj_close": [171.55, 5.21],
            "volume": [10000, 0],
            "dividends": [0.5, 0.0],
            "stock_splits": [0.12345, 0.0],
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
    assert df_silver.count() == 2

    rows = df_silver.orderBy("ticker").collect()
    row_msft = rows[0]
    row_usdbrl = rows[1]

    assert row_msft.ticker == "MSFT"
    assert row_msft.date == date(2026, 5, 28)
    assert row_msft.open == Decimal("170.50")
    assert row_msft.high == Decimal("172.50")
    assert row_msft.low == Decimal("168.50")
    assert row_msft.close == Decimal("171.55")
    assert row_msft.adj_close == Decimal("171.55")
    assert row_msft.volume == 10000
    assert row_msft.dividends == Decimal("0.50")
    assert row_msft.stock_splits == Decimal("0.1235")
    assert row_msft.ingestion_timestamp == datetime(2026, 5, 28, 10, 0, 0)

    assert row_usdbrl.ticker == "USDBRL"
    assert row_usdbrl.date == date(2026, 5, 28)
    assert row_usdbrl.open == Decimal("5.20")
    assert row_usdbrl.close == Decimal("5.21")
    assert row_usdbrl.volume == 0

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
    """Test that rows with nulls in Gold-required columns are quarantined rather than dropped."""
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    silver_dir = tmp_path / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    rejected_dir = tmp_path / "silver_rejected"
    rejected_dir.mkdir(parents=True, exist_ok=True)

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
        patch("src.streaming.silver.SILVER_PRICES_REJECTED_DIR", rejected_dir),
        patch("src.streaming.silver.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        main()

    df_silver = spark_session.read.format("delta").load(str(silver_dir))
    assert df_silver.count() == 1
    assert df_silver.collect()[0].ticker == "MSFT"

    # The AAPL row is not silently discarded: it is quarantined with a reason per missing column
    df_rejected = spark_session.read.format("delta").load(str(rejected_dir))
    rejected_rows = df_rejected.collect()
    assert len(rejected_rows) == 1
    assert rejected_rows[0].ticker == "AAPL"
    reasons = rejected_rows[0].rejection_reasons
    assert any("open is missing" in reason for reason in reasons)
    assert any("close is missing" in reason for reason in reasons)


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
            "open": [170.5, 350.0],
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


def _prices_bronze_row(ticker, **overrides):
    """Build a fully-populated, internally consistent Bronze prices row."""
    row = {
        "date": "2026-05-28",
        "ticker": ticker,
        "open": 170.0,
        "high": 172.5,
        "low": 168.5,
        "close": 171.5,
        "adj_close": 171.5,
        "volume": 10000,
        "dividends": 0.5,
        "stock_splits": 0.0,
        "ingestion_timestamp": "2026-05-28 10:00:00",
    }
    row.update(overrides)
    return row


def _run_silver_prices(spark_session, tmp_path, rows, exec_date="2026-05-28"):
    """Run the prices pipeline over `rows` and return the (valid, rejected) DataFrames."""
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    silver_dir = tmp_path / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    rejected_dir = tmp_path / "silver_rejected"
    rejected_dir.mkdir(parents=True, exist_ok=True)

    spark_session.createDataFrame(rows).write.format("delta").mode("overwrite").save(str(bronze_dir))

    with (
        patch("src.streaming.silver.BRONZE_PRICES_DIR", bronze_dir),
        patch("src.streaming.silver.SILVER_PRICES_DIR", silver_dir),
        patch("src.streaming.silver.SILVER_PRICES_REJECTED_DIR", rejected_dir),
        patch("src.streaming.silver.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        run_silver(exec_date)

    return (
        spark_session.read.format("delta").load(str(silver_dir)),
        spark_session.read.format("delta").load(str(rejected_dir)),
    )


def test_silver_prices_missing_ticker_fails_the_run(spark_session, tmp_path):
    """Test that a null ticker fails the whole run instead of being silently dropped."""
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    silver_dir = tmp_path / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    rows = [_prices_bronze_row("AAPL"), _prices_bronze_row(None)]
    spark_session.createDataFrame(rows).write.format("delta").mode("overwrite").save(str(bronze_dir))

    with (
        patch("src.streaming.silver.BRONZE_PRICES_DIR", bronze_dir),
        patch("src.streaming.silver.SILVER_PRICES_DIR", silver_dir),
        patch("src.streaming.silver.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
        pytest.raises(RequiredColumnMissingError, match="ticker"),
    ):
        run_silver("2026-05-28", raise_on_error=True)


def test_silver_prices_quarantines_negative_values(spark_session, tmp_path):
    """Test that negative prices, volumes and dividends are quarantined, not written to Silver."""
    rows = [
        _prices_bronze_row("AAPL"),
        _prices_bronze_row("NEGOPEN", open=-1.0),
        _prices_bronze_row("NEGVOL", volume=-5),
        _prices_bronze_row("NEGDIV", dividends=-0.5),
    ]

    df_silver, df_rejected = _run_silver_prices(spark_session, tmp_path, rows)

    assert [row.ticker for row in df_silver.collect()] == ["AAPL"]

    rejected = {row.ticker: row.rejection_reasons for row in df_rejected.collect()}
    assert set(rejected) == {"NEGOPEN", "NEGVOL", "NEGDIV"}
    assert any("open outside expected range" in reason for reason in rejected["NEGOPEN"])
    assert any("volume outside expected range" in reason for reason in rejected["NEGVOL"])
    assert any("dividends outside expected range" in reason for reason in rejected["NEGDIV"])


def test_silver_prices_quarantines_ohlc_inconsistency(spark_session, tmp_path):
    """Test that structurally impossible OHLC rows are quarantined on a traded day."""
    rows = [
        _prices_bronze_row("AAPL"),
        _prices_bronze_row("HIGHLOW", high=100.0, low=200.0, open=150.0, close=150.0),
        _prices_bronze_row("LOWABOVEOPEN", low=180.0, open=170.0, high=185.0, close=182.0),
    ]

    df_silver, df_rejected = _run_silver_prices(spark_session, tmp_path, rows)

    assert [row.ticker for row in df_silver.collect()] == ["AAPL"]

    rejected = {row.ticker: row.rejection_reasons for row in df_rejected.collect()}
    assert any("high is not the highest" in reason for reason in rejected["HIGHLOW"])
    assert any("low is not the lowest" in reason for reason in rejected["LOWABOVEOPEN"])


def test_silver_prices_keeps_untraded_days_with_zero_prices(spark_session, tmp_path):
    """Test that the OHLC rules skip non-trading days, where zeroed prices are expected."""
    rows = [_prices_bronze_row("HALTED", open=0.0, high=0.0, low=0.0, close=0.0, adj_close=0.0, volume=0)]

    df_silver, df_rejected = _run_silver_prices(spark_session, tmp_path, rows)

    assert [row.ticker for row in df_silver.collect()] == ["HALTED"]
    assert df_rejected.count() == 0


def test_silver_prices_quarantines_invalid_ticker_format(spark_session, tmp_path):
    """Test that a non-null but malformed ticker is quarantined."""
    rows = [_prices_bronze_row("AAPL"), _prices_bronze_row("bad ticker!")]

    df_silver, df_rejected = _run_silver_prices(spark_session, tmp_path, rows)

    assert [row.ticker for row in df_silver.collect()] == ["AAPL"]
    rejected = df_rejected.collect()
    assert len(rejected) == 1
    assert any("ticker does not match required format" in reason for reason in rejected[0].rejection_reasons)


def test_silver_prices_skips_non_trading_days_without_quarantining(spark_session, tmp_path):
    """Test that a row with no OHLC data at all is skipped rather than quarantined.

    yfinance emits a row per calendar date per ticker, with every OHLC value null when no session
    took place. Quarantining those would bury real defects under tens of thousands of rows.
    """
    rows = [
        _prices_bronze_row("AAPL"),
        _prices_bronze_row(
            "HOLIDAY",
            open=None,
            high=None,
            low=None,
            close=None,
            adj_close=None,
            volume=0,
            dividends=None,
            stock_splits=None,
        ),
    ]

    df_silver, df_rejected = _run_silver_prices(spark_session, tmp_path, rows)

    assert [row.ticker for row in df_silver.collect()] == ["AAPL"]
    assert df_rejected.count() == 0


def test_silver_prices_quarantines_partial_nulls_on_a_traded_row(spark_session, tmp_path):
    """Test that a row with only *some* OHLC values missing is still quarantined.

    Unlike a non-trading day, a row that traded but lost a required value is a real defect and
    must not reach the Gold schema, which declares those columns non-Nullable.
    """
    rows = [_prices_bronze_row("AAPL"), _prices_bronze_row("PARTIAL", open=None, close=None)]

    df_silver, df_rejected = _run_silver_prices(spark_session, tmp_path, rows)

    assert [row.ticker for row in df_silver.collect()] == ["AAPL"]
    rejected = df_rejected.collect()
    assert len(rejected) == 1
    assert rejected[0].ticker == "PARTIAL"
    reasons = rejected[0].rejection_reasons
    assert any("open is missing" in reason for reason in reasons)
    assert any("close is missing" in reason for reason in reasons)
