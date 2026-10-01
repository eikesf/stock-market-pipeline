from datetime import date, datetime
from unittest.mock import patch

import pandas as pd
import pytest
from loguru import logger
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, DecimalType, IntegerType, LongType, StringType, TimestampType

from src.streaming.quality_rules import RequiredColumnMissingError
from src.streaming.silver_metadata import main, run_silver_metadata, run_silver_metrics


def test_silver_metadata_cleaning_and_casting(spark_session, tmp_path):
    """
    Test that the Silver metadata pipeline correctly cleans and casts columns.
    """
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    df_bronze = pd.DataFrame(
        {
            "ticker": [" aapl "],
            "short_name": [" Apple Inc. "],
            "sector": [" Technology "],
            "industry": [" Consumer Electronics "],
            "country": [" United States "],
            "isin": [" US0378331005 "],
            "full_time_employees": [160000],
            "exchange": [" sao "],
            "market_cap": [2600000000000],
            "currency": [" usd "],
            "dividend_yield": [0.005],
            "extraction_date": ["2026-05-28"],
            "ingestion_timestamp": ["2026-05-28 10:00:00"],
        }
    )

    df_bronze_spark = spark_session.createDataFrame(df_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_metadata_dir))

    with (
        patch("src.streaming.silver_metadata.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.streaming.silver_metadata.SILVER_METADATA_DIR", silver_metadata_dir),
        patch("src.streaming.silver_metadata.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        run_silver_metadata("2026-05-28")

    df_silver_metadata = spark_session.read.format("delta").load(str(silver_metadata_dir))
    assert df_silver_metadata.count() == 1
    row = df_silver_metadata.collect()[0]

    assert row.ticker == "AAPL"
    assert row.short_name == "Apple Inc."
    assert row.sector == "Technology"
    assert row.industry == "Consumer Electronics"
    assert row.country == "United States"
    assert row.isin == "US0378331005"
    assert row.currency == "usd"
    assert row.exchange == "B3"
    assert row.start_date == date(2026, 5, 28)
    assert row.end_date is None
    assert row.is_active == 1

    assert isinstance(df_silver_metadata.schema["ticker"].dataType, StringType)
    assert isinstance(df_silver_metadata.schema["short_name"].dataType, StringType)
    assert isinstance(df_silver_metadata.schema["sector"].dataType, StringType)
    assert isinstance(df_silver_metadata.schema["exchange"].dataType, StringType)
    assert isinstance(df_silver_metadata.schema["extraction_date"].dataType, DateType)
    assert isinstance(df_silver_metadata.schema["ingestion_timestamp"].dataType, TimestampType)
    assert isinstance(df_silver_metadata.schema["full_time_employees"].dataType, IntegerType)

    assert isinstance(df_silver_metadata.schema["start_date"].dataType, DateType)
    assert isinstance(df_silver_metadata.schema["end_date"].dataType, DateType)
    assert isinstance(df_silver_metadata.schema["is_active"].dataType, IntegerType)


def _metadata_bronze_row(ticker, **overrides):
    """Build a fully-populated Bronze metadata row, overriding individual fields as needed."""
    row = {
        "ticker": ticker,
        "short_name": f"{ticker} Inc.",
        "sector": "Technology",
        "industry": "Software",
        "country": "USA",
        "isin": "US5949181045",
        "full_time_employees": 220000,
        "exchange": "NASDAQ",
        "market_cap": 3000000000000,
        "currency": "USD",
        "dividend_yield": 0.007,
        "extraction_date": "2026-05-28",
        "ingestion_timestamp": "2026-05-28 10:00:00",
    }
    row.update(overrides)
    return row


def test_silver_metadata_missing_ticker_fails_the_run(spark_session, tmp_path):
    """Test that a null ticker fails the whole run instead of being silently dropped."""
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    data_bronze = [_metadata_bronze_row("MSFT"), _metadata_bronze_row(None)]

    df_bronze_spark = spark_session.createDataFrame(data_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_metadata_dir))

    with (
        patch("src.streaming.silver_metadata.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.streaming.silver_metadata.SILVER_METADATA_DIR", silver_metadata_dir),
        patch("src.streaming.silver_metadata.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
        pytest.raises(RequiredColumnMissingError, match="ticker"),
    ):
        run_silver_metadata("2026-05-28", raise_on_error=True)


def test_silver_metadata_quarantines_incomplete_rows(spark_session, tmp_path):
    """Test that rows missing a non-essential column are quarantined instead of silently dropped."""
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_rejected_dir = tmp_path / "silver_metadata_rejected"
    silver_metadata_rejected_dir.mkdir(parents=True, exist_ok=True)

    # Using a list of dicts directly prevents Pandas from converting None to string "nan" or float NaN.
    data_bronze = [
        _metadata_bronze_row("MSFT"),
        _metadata_bronze_row("AAPL", short_name=None),
        _metadata_bronze_row("GOOGL", sector=None),
        _metadata_bronze_row("AMZN", exchange=None),
        _metadata_bronze_row("TSLA", isin="US123"),
    ]

    df_bronze_spark = spark_session.createDataFrame(data_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_metadata_dir))

    with (
        patch("src.streaming.silver_metadata.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.streaming.silver_metadata.SILVER_METADATA_DIR", silver_metadata_dir),
        patch("src.streaming.silver_metadata.SILVER_METADATA_REJECTED_DIR", silver_metadata_rejected_dir),
        patch("src.streaming.silver_metadata.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        run_silver_metadata("2026-05-28")

    df_silver_metadata = spark_session.read.format("delta").load(str(silver_metadata_dir))
    assert df_silver_metadata.count() == 1
    assert df_silver_metadata.collect()[0].ticker == "MSFT"

    df_rejected = spark_session.read.format("delta").load(str(silver_metadata_rejected_dir))
    rejected = {row.ticker: row.rejection_reasons for row in df_rejected.collect()}
    assert set(rejected) == {"AAPL", "GOOGL", "AMZN", "TSLA"}
    assert any("short_name is missing" in reason for reason in rejected["AAPL"])
    assert any("sector is missing" in reason for reason in rejected["GOOGL"])
    assert any("exchange is missing" in reason for reason in rejected["AMZN"])
    assert any("isin does not match required format" in reason for reason in rejected["TSLA"])


def test_silver_metadata_exchange_standardization(spark_session, tmp_path):
    """
    Test that exchange names are normalized correctly:
    - 'SAO' to 'B3'
    - 'NYQ' to 'NYSE'
    - 'NMS', 'NGM', 'NCM', 'NASDAQ' to 'NASDAQ'
    - other exchange names should be kept as is.
    """
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    data_bronze = [
        {
            "ticker": "PETR4",
            "short_name": "Petrobras",
            "sector": "Energy",
            "industry": "Oil & Gas",
            "country": "Brazil",
            "isin": "BRPETRACNPR6",
            "full_time_employees": 40000,
            "exchange": "  sao  ",
            "market_cap": 500000000000,
            "currency": "BRL",
            "dividend_yield": 0.08,
            "extraction_date": "2026-05-28",
            "ingestion_timestamp": "2026-05-28 10:00:00",
        },
        {
            "ticker": "IBM",
            "short_name": "IBM Corp.",
            "sector": "Technology",
            "industry": "IT Services",
            "country": "USA",
            "isin": "US4592001014",
            "full_time_employees": 250000,
            "exchange": "nyq",
            "market_cap": 150000000000,
            "currency": "USD",
            "dividend_yield": 0.04,
            "extraction_date": "2026-05-28",
            "ingestion_timestamp": "2026-05-28 10:00:00",
        },
        {
            "ticker": "AAPL",
            "short_name": "Apple Inc.",
            "sector": "Technology",
            "industry": "Electronics",
            "country": "USA",
            "isin": "US0378331005",
            "full_time_employees": 160000,
            "exchange": "nms",
            "market_cap": 2600000000000,
            "currency": "USD",
            "dividend_yield": 0.005,
            "extraction_date": "2026-05-28",
            "ingestion_timestamp": "2026-05-28 10:00:00",
        },
        {
            "ticker": "MSFT",
            "short_name": "Microsoft",
            "sector": "Technology",
            "industry": "Software",
            "country": "USA",
            "isin": "US5949181045",
            "full_time_employees": 220000,
            "exchange": "ngm",
            "market_cap": 3000000000000,
            "currency": "USD",
            "dividend_yield": 0.007,
            "extraction_date": "2026-05-28",
            "ingestion_timestamp": "2026-05-28 10:00:00",
        },
        {
            "ticker": "GOOGL",
            "short_name": "Google",
            "sector": "Technology",
            "industry": "Software",
            "country": "USA",
            "isin": "US02079K3059",
            "full_time_employees": 180000,
            "exchange": "ncm",
            "market_cap": 1700000000000,
            "currency": "USD",
            "dividend_yield": 0.0,
            "extraction_date": "2026-05-28",
            "ingestion_timestamp": "2026-05-28 10:00:00",
        },
        {
            "ticker": "AMZN",
            "short_name": "Amazon",
            "sector": "Technology",
            "industry": "Retail",
            "country": "USA",
            "isin": "US0231351067",
            "full_time_employees": 1500000,
            "exchange": "  nasdaq  ",
            "market_cap": 1800000000000,
            "currency": "USD",
            "dividend_yield": 0.0,
            "extraction_date": "2026-05-28",
            "ingestion_timestamp": "2026-05-28 10:00:00",
        },
        {
            "ticker": "BP",
            "short_name": "BP plc",
            "sector": "Energy",
            "industry": "Oil & Gas",
            "country": "UK",
            "isin": "GB0007980591",
            "full_time_employees": 60000,
            "exchange": "  lse  ",
            "market_cap": 100000000000,
            "currency": "GBP",
            "dividend_yield": 0.05,
            "extraction_date": "2026-05-28",
            "ingestion_timestamp": "2026-05-28 10:00:00",
        },
    ]

    df_bronze_spark = spark_session.createDataFrame(data_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_metadata_dir))

    with (
        patch("src.streaming.silver_metadata.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.streaming.silver_metadata.SILVER_METADATA_DIR", silver_metadata_dir),
        patch("src.streaming.silver_metadata.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        run_silver_metadata("2026-05-28")

    df_silver_metadata = spark_session.read.format("delta").load(str(silver_metadata_dir))
    assert df_silver_metadata.count() == 7

    rows = {r.ticker: r.exchange for r in df_silver_metadata.collect()}
    assert rows["PETR4"] == "B3"
    assert rows["IBM"] == "NYSE"
    assert rows["AAPL"] == "NASDAQ"
    assert rows["MSFT"] == "NASDAQ"
    assert rows["GOOGL"] == "NASDAQ"
    assert rows["AMZN"] == "NASDAQ"
    assert rows["BP"] == "LSE"


def test_silver_metadata_deduplication_by_ticker(spark_session, tmp_path):
    """
    Test that metadata is deduplicated per ticker, keeping only the most recent record.
    """
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    df_bronze = pd.DataFrame(
        {
            "ticker": ["MSFT", "AAPL", "MSFT", "AAPL", "MSFT", "GOOGL", "GOOGL"],
            "short_name": [
                "Microsoft Corp.",
                "Apple Inc.",
                "Microsoft Corp.",
                "Apple Inc.",
                "Microsoft Corp.",
                "Google Inc.",
                "Google Inc.",
            ],
            "sector": ["Technology"] * 7,
            "industry": ["Software"] * 7,
            "country": ["USA"] * 7,
            "isin": ["US5949181045"] * 7,
            "full_time_employees": [220000] * 7,
            "exchange": ["NASDAQ"] * 7,
            "market_cap": [
                3000000000000,
                2600000000000,
                3100000000000,
                2500000000000,
                3200000000000,
                1700000000000,
                1600000000000,
            ],
            "currency": ["USD"] * 7,
            "dividend_yield": [0.007] * 7,
            "extraction_date": ["2026-05-28"] * 7,
            "ingestion_timestamp": [
                "2026-05-28 10:00:00",
                "2026-05-28 11:00:00",
                "2026-05-28 12:00:00",
                "2026-05-28 13:00:00",
                "2026-05-28 14:00:00",
                "2026-05-28 15:00:00",
                "2026-05-28 16:00:00",
            ],
        }
    )

    df_bronze_spark = spark_session.createDataFrame(df_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_metadata_dir))

    with (
        patch("src.streaming.silver_metadata.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.streaming.silver_metadata.SILVER_METADATA_DIR", silver_metadata_dir),
        patch("src.streaming.silver_metadata.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        run_silver_metadata("2026-05-28")

    df_silver_metadata = spark_session.read.format("delta").load(str(silver_metadata_dir))
    assert df_silver_metadata.count() == 3

    rows = {r.ticker: r.ingestion_timestamp for r in df_silver_metadata.collect()}
    assert rows["MSFT"] == datetime(2026, 5, 28, 14, 0, 0)
    assert rows["AAPL"] == datetime(2026, 5, 28, 13, 0, 0)
    assert rows["GOOGL"] == datetime(2026, 5, 28, 16, 0, 0)


def test_silver_metadata_failure(spark_session, tmp_path):
    """
    Test Silver metadata pipeline exit code 1 when processing fails.
    """
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    data_bronze = [
        {
            "ticker": "AAPL",
            "short_name": "Apple Inc.",
            "sector": "Technology",
            "industry": "Electronics",
            "country": "USA",
            "isin": "US0378331005",
            "full_time_employees": 160000,
            "exchange": "NASDAQ",
            "market_cap": 2600000000000,
            "currency": "USD",
            "dividend_yield": 0.005,
            "extraction_date": "2026-05-28",
            "ingestion_timestamp": "2026-05-28 10:00:00",
        }
    ]

    df_bronze_spark = spark_session.createDataFrame(data_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_metadata_dir))

    # Capture logs to assert expected error messages on pipeline failure
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="ERROR")

    try:
        with (
            patch("src.streaming.silver_metadata.BRONZE_METADATA_DIR", bronze_metadata_dir),
            patch("src.streaming.silver_metadata.SILVER_METADATA_DIR", silver_metadata_dir),
            patch("src.streaming.silver_metadata.create_spark_session", return_value=spark_session),
            patch(
                "src.streaming.silver_metadata.write_delta_table", side_effect=Exception("Simulated writing failure")
            ),
            patch.object(spark_session, "stop"),
            pytest.raises(SystemExit) as exc_info,
        ):
            run_silver_metadata("2026-05-28")
    finally:
        logger.remove(sink_id)

    assert exc_info.value.code == 1
    assert len(list(silver_metadata_dir.glob("**/*.parquet"))) == 0

    log_content = "".join(captured_logs)
    assert "Failed to process Silver layer metadata" in log_content
    assert "Simulated writing failure" in log_content


def test_silver_metadata_date_from_arguments(spark_session, tmp_path):
    """
    Test that Silver metadata pipeline parses --date from CLI arguments correctly.
    """
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    df_bronze = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "short_name": ["Apple Inc."],
            "sector": ["Technology"],
            "industry": ["Electronics"],
            "country": ["USA"],
            "isin": ["US0378331005"],
            "full_time_employees": [160000],
            "exchange": ["NASDAQ"],
            "market_cap": [2600000000000],
            "currency": ["USD"],
            "dividend_yield": [0.005],
            "extraction_date": ["2026-05-28"],
            "ingestion_timestamp": ["2026-05-28 10:00:00"],
        }
    )

    df_bronze_spark = spark_session.createDataFrame(df_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_metadata_dir))

    with (
        patch("src.streaming.silver_metadata.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.streaming.silver_metadata.SILVER_METADATA_DIR", silver_metadata_dir),
        patch("src.streaming.silver_metadata.create_spark_session", return_value=spark_session),
        patch("sys.argv", ["silver_metadata.py", "--date", "2026-05-28", "--table", "metadata"]),
        patch.object(spark_session, "stop"),
    ):
        main()

    df_silver = spark_session.read.format("delta").load(str(silver_metadata_dir))
    assert df_silver.count() == 1


def test_silver_metadata_invalid_date_format(spark_session, tmp_path):
    """
    Test that an invalid date format passed to --date exits with code 1.
    """
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    # Capture logs to assert expected error messages on pipeline failure
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="ERROR")

    try:
        with (
            patch("src.streaming.silver_metadata.BRONZE_METADATA_DIR", bronze_metadata_dir),
            patch("src.streaming.silver_metadata.SILVER_METADATA_DIR", silver_metadata_dir),
            patch("src.streaming.silver_metadata.create_spark_session", return_value=spark_session),
            patch("sys.argv", ["silver_metadata.py", "--date", "invalid_date_format"]),
            patch.object(spark_session, "stop"),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()
    finally:
        logger.remove(sink_id)

    assert exc_info.value.code == 1
    log_content = "".join(captured_logs)
    assert "Invalid date format" in log_content


def _run_silver_metadata_step(spark_session, bronze_dir, silver_dir, df_bronze, date_str):
    spark_session.createDataFrame(df_bronze).write.format("delta").mode("overwrite").save(str(bronze_dir))
    with (
        patch("src.streaming.silver_metadata.BRONZE_METADATA_DIR", bronze_dir),
        patch("src.streaming.silver_metadata.SILVER_METADATA_DIR", silver_dir),
        patch("src.streaming.silver_metadata.create_spark_session", return_value=spark_session),
        patch("sys.argv", ["silver_metadata.py", "--date", date_str]),
        patch.object(spark_session, "stop"),
    ):
        run_silver_metadata(date_str)
    return spark_session.read.format("delta").load(str(silver_dir))


def _get_bronze_data(ticker_list, sector_list, extraction_date_list, short_name_list=None):
    if short_name_list is None:
        short_name_list = "Apple Inc."
    return pd.DataFrame(
        {
            "ticker": ticker_list,
            "short_name": short_name_list,
            "sector": sector_list,
            "industry": "Electronics",
            "country": "USA",
            "isin": "US0378331005",
            "full_time_employees": 160000,
            "exchange": "NASDAQ",
            "market_cap": 2600000000000,
            "currency": "USD",
            "dividend_yield": 0.005,
            "extraction_date": extraction_date_list,
            "ingestion_timestamp": [f"{d} 10:00:00" for d in extraction_date_list],
        }
    )


def _assert_row_values(row, expected):
    for col_name, val in expected.items():
        assert getattr(row, col_name) == val


def test_silver_metadata_scd_type_2_history(spark_session, tmp_path):
    """
    Test SCD Type 2 history tracking in the Silver metadata pipeline:
    - Initial run (cold start): creates the first active record.
    - Unchanged run: keeps the existing record and doesn't insert duplicates.
    - Changed run (SCD 2 event): deactivates the old version and inserts a new active version.
    - New company run: appends the new company as active without modifying existing active ones.
    """
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    # Cold Start Run (2026-05-28)
    df_bronze_1 = _get_bronze_data(["AAPL"], ["Technology"], ["2026-05-28"])
    df_silver = _run_silver_metadata_step(
        spark_session, bronze_metadata_dir, silver_metadata_dir, df_bronze_1, "2026-05-28"
    )

    assert df_silver.count() == 1
    row = df_silver.collect()[0]
    _assert_row_values(
        row,
        {
            "ticker": "AAPL",
            "sector": "Technology",
            "is_active": 1,
            "start_date": date(2026, 5, 28),
            "end_date": None,
        },
    )

    # Run with Unchanged attributes (2026-05-29)
    df_bronze_2 = _get_bronze_data(["AAPL"], ["Technology"], ["2026-05-29"])
    df_silver = _run_silver_metadata_step(
        spark_session, bronze_metadata_dir, silver_metadata_dir, df_bronze_2, "2026-05-29"
    )

    assert df_silver.count() == 1
    row = df_silver.collect()[0]
    _assert_row_values(
        row,
        {
            "ticker": "AAPL",
            "sector": "Technology",
            "is_active": 1,
            "start_date": date(2026, 5, 28),
            "end_date": None,
        },
    )

    # Run with Changed attributes (SCD Type 2 event - 2026-05-30)
    # sector changes from "Technology" to "Consumer Electronics"
    df_bronze_3 = _get_bronze_data(["AAPL"], ["Consumer Electronics"], ["2026-05-30"])
    df_silver = _run_silver_metadata_step(
        spark_session, bronze_metadata_dir, silver_metadata_dir, df_bronze_3, "2026-05-30"
    )

    assert df_silver.count() == 2

    # Verify history
    history = df_silver.orderBy("start_date").collect()
    _assert_row_values(
        history[0],
        {
            "ticker": "AAPL",
            "sector": "Technology",
            "is_active": 0,
            "start_date": date(2026, 5, 28),
            "end_date": date(2026, 5, 30),
        },
    )
    _assert_row_values(
        history[1],
        {
            "ticker": "AAPL",
            "sector": "Consumer Electronics",
            "is_active": 1,
            "start_date": date(2026, 5, 30),
            "end_date": None,
        },
    )

    # Run with New Company Added (2026-05-31)
    df_bronze_4 = _get_bronze_data(
        ["AAPL", "MSFT"],
        ["Consumer Electronics", "Software"],
        ["2026-05-31", "2026-05-31"],
        ["Apple Inc.", "Microsoft Corp."],
    )
    df_silver = _run_silver_metadata_step(
        spark_session, bronze_metadata_dir, silver_metadata_dir, df_bronze_4, "2026-05-31"
    )

    assert df_silver.count() == 3

    # Check MSFT active
    msft_rows = df_silver.filter(col("ticker") == "MSFT").collect()
    assert len(msft_rows) == 1
    _assert_row_values(
        msft_rows[0],
        {
            "ticker": "MSFT",
            "sector": "Software",
            "is_active": 1,
            "start_date": date(2026, 5, 31),
            "end_date": None,
        },
    )

    # Check AAPL active is still the one from 2026-05-30
    aapl_active = df_silver.filter((col("ticker") == "AAPL") & (col("is_active") == 1)).collect()
    assert len(aapl_active) == 1
    _assert_row_values(
        aapl_active[0],
        {
            "ticker": "AAPL",
            "sector": "Consumer Electronics",
            "is_active": 1,
            "start_date": date(2026, 5, 30),
            "end_date": None,
        },
    )


def test_silver_metrics_cleaning_and_casting(spark_session, tmp_path):
    """Test that the Silver metrics pipeline correctly cleans and casts columns."""
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metrics_dir = tmp_path / "silver_metrics"
    silver_metrics_dir.mkdir(parents=True, exist_ok=True)

    silver_metrics_rejected_dir = tmp_path / "silver_metrics_rejected"
    silver_metrics_rejected_dir.mkdir(parents=True, exist_ok=True)

    df_bronze = pd.DataFrame(
        {
            "ticker": [" aapl "],
            "dividend_yield": [0.0051],
            "trailing_pe": [15.42],
            "market_cap": [2600000000000],
            "peg_ratio": [1.5],
            "price_to_book": [2.5],
            "enterprise_to_ebitda": [12.3],
            "enterprise_to_ebit": [14.1],
            "book_value": [35.2],
            "trailing_eps": [6.5],
            "price_to_sales": [7.2],
            "operating_margins": [0.25],
            "asset_turnover": [0.8],
            "shares_outstanding": [15000000000],
            "ebitda": [100000000000],
            "total_debt": [120000000000],
            "total_cash": [80000000000],
            "debt_to_equity": [1.5],
            "roa": [0.12],
            "roe": [0.28],
            "current_ratio": [1.8],
            "gross_margins": [0.42],
            "ebitda_margins": [0.32],
            "profit_margins": [0.21],
            "net_income_to_common": [80000000000],
            "extraction_date": ["2026-05-28"],
            "ingestion_timestamp": ["2026-05-28 10:00:00"],
        }
    )

    df_bronze_spark = spark_session.createDataFrame(df_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_metadata_dir))

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    with (
        patch("src.streaming.silver_metadata.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.streaming.silver_metadata.SILVER_METRICS_DIR", silver_metrics_dir),
        patch("src.streaming.silver_metadata.SILVER_METRICS_REJECTED_DIR", silver_metrics_rejected_dir),
        patch("src.streaming.silver_metadata.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        run_silver_metrics("2026-05-28")

    df_silver_metrics = spark_session.read.format("delta").load(str(silver_metrics_dir))
    assert df_silver_metrics.count() == 1
    row = df_silver_metrics.collect()[0]

    assert row.ticker == "AAPL"
    assert row.shares_outstanding == 15000000000
    assert row.market_cap == 2600000000000
    assert row.extraction_date == date(2026, 5, 28)

    # Check decimal datatypes
    assert isinstance(df_silver_metrics.schema["dividend_yield"].dataType, DecimalType)
    assert df_silver_metrics.schema["dividend_yield"].dataType.precision == 10
    assert df_silver_metrics.schema["dividend_yield"].dataType.scale == 4

    assert isinstance(df_silver_metrics.schema["trailing_pe"].dataType, DecimalType)
    assert df_silver_metrics.schema["trailing_pe"].dataType.precision == 10
    assert df_silver_metrics.schema["trailing_pe"].dataType.scale == 4

    assert isinstance(df_silver_metrics.schema["market_cap"].dataType, LongType)


def test_silver_metrics_nulls_implausible_trailing_eps(spark_session, tmp_path):
    """Test that implausible trailing_eps values (e.g. bad upstream yfinance data) are nulled out."""
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metrics_dir = tmp_path / "silver_metrics"
    silver_metrics_dir.mkdir(parents=True, exist_ok=True)

    silver_metrics_rejected_dir = tmp_path / "silver_metrics_rejected"
    silver_metrics_rejected_dir.mkdir(parents=True, exist_ok=True)

    df_bronze = pd.DataFrame(
        {
            "ticker": ["TTEN3.SA", "AAPL"],
            "dividend_yield": [0.0, 0.0051],
            "trailing_pe": [None, 15.42],
            "market_cap": [4969374000, 2600000000000],
            "peg_ratio": [None, 1.5],
            "price_to_book": [None, 2.5],
            "enterprise_to_ebitda": [None, 12.3],
            "enterprise_to_ebit": [None, 14.1],
            "book_value": [None, 35.2],
            "trailing_eps": [-105102.71, 6.5],
            "price_to_sales": [None, 7.2],
            "operating_margins": [None, 0.25],
            "asset_turnover": [None, 0.8],
            "shares_outstanding": [500440447, 15000000000],
            "ebitda": [None, 100000000000],
            "total_debt": [None, 120000000000],
            "total_cash": [None, 80000000000],
            "debt_to_equity": [None, 1.5],
            "roa": [None, 0.12],
            "roe": [None, 0.28],
            "current_ratio": [None, 1.8],
            "gross_margins": [None, 0.42],
            "ebitda_margins": [None, 0.32],
            "profit_margins": [None, 0.21],
            "net_income_to_common": [583464000, 80000000000],
            "extraction_date": ["2026-08-16", "2026-08-16"],
            "ingestion_timestamp": ["2026-08-16 00:00:00", "2026-08-16 00:00:00"],
        }
    )

    df_bronze_spark = spark_session.createDataFrame(df_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_metadata_dir))

    with (
        patch("src.streaming.silver_metadata.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.streaming.silver_metadata.SILVER_METRICS_DIR", silver_metrics_dir),
        patch("src.streaming.silver_metadata.SILVER_METRICS_REJECTED_DIR", silver_metrics_rejected_dir),
        patch("src.streaming.silver_metadata.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        run_silver_metrics("2026-08-16")

    df_silver_metrics = spark_session.read.format("delta").load(str(silver_metrics_dir))
    rows = {row.ticker: row for row in df_silver_metrics.collect()}

    # The whole breaching row is withheld from Silver, not just the offending column nulled
    assert "TTEN3.SA" not in rows
    assert rows["AAPL"].trailing_eps == 6.5

    df_rejected = spark_session.read.format("delta").load(str(silver_metrics_rejected_dir))
    rejected_rows = df_rejected.collect()
    assert len(rejected_rows) == 1
    assert rejected_rows[0].ticker == "TTEN3.SA"
    assert any("trailing_eps outside expected range" in reason for reason in rejected_rows[0].rejection_reasons)
    # The quarantined row keeps its original value for investigation
    assert float(rejected_rows[0].trailing_eps) == pytest.approx(-105102.71)
    assert rejected_rows[0].pipeline_exec_date.isoformat() == "2026-08-16"
    assert rejected_rows[0].rejected_at is not None


def test_silver_metrics_nulls_implausible_price_to_sales_and_operating_margins(spark_session, tmp_path):
    """Test that a row breaching several rules is quarantined once, carrying every reason."""
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metrics_dir = tmp_path / "silver_metrics"
    silver_metrics_dir.mkdir(parents=True, exist_ok=True)

    silver_metrics_rejected_dir = tmp_path / "silver_metrics_rejected"
    silver_metrics_rejected_dir.mkdir(parents=True, exist_ok=True)

    df_bronze = pd.DataFrame(
        {
            "ticker": ["BADTICKER", "AAPL"],
            "dividend_yield": [0.0, 0.0051],
            "trailing_pe": [None, 15.42],
            "market_cap": [4969374000, 2600000000000],
            "peg_ratio": [None, 1.5],
            "price_to_book": [None, 2.5],
            "enterprise_to_ebitda": [None, 12.3],
            "enterprise_to_ebit": [None, 14.1],
            "book_value": [None, 35.2],
            "trailing_eps": [None, 6.5],
            "price_to_sales": [-1.4997, 7.2],
            "operating_margins": [-274.0, 0.25],
            "asset_turnover": [None, 0.8],
            "shares_outstanding": [500440447, 15000000000],
            "ebitda": [None, 100000000000],
            "total_debt": [None, 120000000000],
            "total_cash": [None, 80000000000],
            "debt_to_equity": [None, 1.5],
            "roa": [None, 0.12],
            "roe": [None, 0.28],
            "current_ratio": [None, 1.8],
            "gross_margins": [None, 0.42],
            "ebitda_margins": [None, 0.32],
            "profit_margins": [None, 0.21],
            "net_income_to_common": [583464000, 80000000000],
            "extraction_date": ["2026-08-03", "2026-08-03"],
            "ingestion_timestamp": ["2026-08-03 00:00:00", "2026-08-03 00:00:00"],
        }
    )

    df_bronze_spark = spark_session.createDataFrame(df_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_metadata_dir))

    with (
        patch("src.streaming.silver_metadata.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.streaming.silver_metadata.SILVER_METRICS_DIR", silver_metrics_dir),
        patch("src.streaming.silver_metadata.SILVER_METRICS_REJECTED_DIR", silver_metrics_rejected_dir),
        patch("src.streaming.silver_metadata.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        run_silver_metrics("2026-08-03")

    df_silver_metrics = spark_session.read.format("delta").load(str(silver_metrics_dir))
    rows = {row.ticker: row for row in df_silver_metrics.collect()}

    # The breaching row is withheld from Silver entirely; the clean row is untouched
    assert "BADTICKER" not in rows
    assert float(rows["AAPL"].price_to_sales) == pytest.approx(7.2)
    assert rows["AAPL"].operating_margins == 0.25

    # One quarantined row per source row (not one per breached metric), carrying both reasons
    df_rejected = spark_session.read.format("delta").load(str(silver_metrics_rejected_dir))
    rejected_rows = df_rejected.collect()
    assert len(rejected_rows) == 1
    assert rejected_rows[0].ticker == "BADTICKER"

    reasons = rejected_rows[0].rejection_reasons
    assert any("price_to_sales outside expected range" in reason for reason in reasons)
    assert any("operating_margins outside expected range" in reason for reason in reasons)

    # Original values are preserved for investigation
    assert float(rejected_rows[0].price_to_sales) == pytest.approx(-1.4997)
    assert rejected_rows[0].operating_margins == -274.0


def test_silver_metrics_rerun_replaces_rejected_rows_for_same_date(spark_session, tmp_path):
    """Test that rerunning the pipeline for the same date replaces (not duplicates) quarantined rows."""
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metrics_dir = tmp_path / "silver_metrics"
    silver_metrics_dir.mkdir(parents=True, exist_ok=True)

    silver_metrics_rejected_dir = tmp_path / "silver_metrics_rejected"
    silver_metrics_rejected_dir.mkdir(parents=True, exist_ok=True)

    df_bronze = pd.DataFrame(
        {
            "ticker": ["BADTICKER", "AAPL"],
            "dividend_yield": [0.005, 0.0051],
            "trailing_pe": [12.0, 15.42],
            "market_cap": [4969374000, 2600000000000],
            "peg_ratio": [1.0, 1.5],
            "price_to_book": [2.0, 2.5],
            "enterprise_to_ebitda": [10.0, 12.3],
            "enterprise_to_ebit": [11.0, 14.1],
            "book_value": [30.0, 35.2],
            "trailing_eps": [5.0, 6.5],
            "price_to_sales": [-1.4997, 7.2],
            "operating_margins": [0.1, 0.25],
            "asset_turnover": [0.7, 0.8],
            "shares_outstanding": [500440447, 15000000000],
            "ebitda": [90000000000, 100000000000],
            "total_debt": [110000000000, 120000000000],
            "total_cash": [70000000000, 80000000000],
            "debt_to_equity": [1.2, 1.5],
            "roa": [0.1, 0.12],
            "roe": [0.25, 0.28],
            "current_ratio": [1.5, 1.8],
            "gross_margins": [0.4, 0.42],
            "ebitda_margins": [0.3, 0.32],
            "profit_margins": [0.2, 0.21],
            "net_income_to_common": [583464000, 80000000000],
            "extraction_date": ["2026-08-03", "2026-08-03"],
            "ingestion_timestamp": ["2026-08-03 00:00:00", "2026-08-03 00:00:00"],
        }
    )

    df_bronze_spark = spark_session.createDataFrame(df_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_metadata_dir))

    with (
        patch("src.streaming.silver_metadata.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.streaming.silver_metadata.SILVER_METRICS_DIR", silver_metrics_dir),
        patch("src.streaming.silver_metadata.SILVER_METRICS_REJECTED_DIR", silver_metrics_rejected_dir),
        patch("src.streaming.silver_metadata.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        run_silver_metrics("2026-08-03")
        run_silver_metrics("2026-08-03")

    df_rejected = spark_session.read.format("delta").load(str(silver_metrics_rejected_dir))
    assert df_rejected.count() == 1


def test_silver_metadata_quarantined_ticker_keeps_existing_active_record(spark_session, tmp_path):
    """Test that bad incoming data does not close out a ticker's existing active SCD Type 2 record.

    Regression test for ordering: if quarantining ran after the SCD Type 2 diff, a ticker whose
    incoming row is bad would still be merged, deactivating the previously good record and losing
    history over a single bad extraction.
    """
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_rejected_dir = tmp_path / "silver_metadata_rejected"
    silver_metadata_rejected_dir.mkdir(parents=True, exist_ok=True)

    def _run(df_bronze, date_str):
        spark_session.createDataFrame(df_bronze).write.format("delta").mode("overwrite").save(str(bronze_metadata_dir))
        with (
            patch("src.streaming.silver_metadata.BRONZE_METADATA_DIR", bronze_metadata_dir),
            patch("src.streaming.silver_metadata.SILVER_METADATA_DIR", silver_metadata_dir),
            patch("src.streaming.silver_metadata.SILVER_METADATA_REJECTED_DIR", silver_metadata_rejected_dir),
            patch("src.streaming.silver_metadata.create_spark_session", return_value=spark_session),
            patch.object(spark_session, "stop"),
        ):
            run_silver_metadata(date_str)
        return spark_session.read.format("delta").load(str(silver_metadata_dir))

    # Cold start: AAPL lands as the active record
    _run(_get_bronze_data(["AAPL"], ["Technology"], ["2026-05-28"]), "2026-05-28")

    # Next run: AAPL's incoming row is unusable (sector came back as the upstream placeholder)
    df_bronze_bad = _get_bronze_data(["AAPL"], ["N/A"], ["2026-05-29"])
    df_silver = _run(df_bronze_bad, "2026-05-29")

    # The good 2026-05-28 record is still the active one, untouched
    rows = df_silver.collect()
    assert len(rows) == 1
    _assert_row_values(
        rows[0],
        {
            "ticker": "AAPL",
            "sector": "Technology",
            "is_active": 1,
            "start_date": date(2026, 5, 28),
            "end_date": None,
        },
    )

    # The bad incoming row is visible in quarantine instead
    df_rejected = spark_session.read.format("delta").load(str(silver_metadata_rejected_dir))
    rejected = df_rejected.collect()
    assert len(rejected) == 1
    assert rejected[0].ticker == "AAPL"
    assert rejected[0].pipeline_exec_date == date(2026, 5, 29)
    assert any("sector is missing" in reason for reason in rejected[0].rejection_reasons)


@pytest.mark.parametrize(
    ("pipeline", "silver_constant"),
    [(run_silver_metadata, "SILVER_METADATA_DIR"), (run_silver_metrics, "SILVER_METRICS_DIR")],
)
def test_silver_metadata_replays_the_archive_only_after_a_bronze_rollback(
    spark_session, tmp_path, pipeline, silver_constant
):
    """Test that both metadata pipelines replay the archive after a Bronze rollback.

    They read the same Bronze table, so both have to repair it; a rollback of their own Silver
    table needs no replay because the next run recomputes it from Bronze.
    """
    bronze_dir = tmp_path / "bronze_metadata"
    silver_dir = tmp_path / "silver"

    for healed, expect_replay in [(bronze_dir, True), (silver_dir, False)]:
        with (
            patch("src.streaming.silver_metadata.BRONZE_METADATA_DIR", bronze_dir),
            patch(f"src.streaming.silver_metadata.{silver_constant}", silver_dir),
            patch("src.streaming.silver_metadata.create_spark_session", return_value=spark_session),
            patch(
                "src.streaming.silver_metadata.read_delta_table",
                side_effect=Exception("FAILED_READ_FILE.NO_HINT"),
            ),
            patch("src.streaming.silver_metadata.check_and_heal_corrupt_data_file", return_value=healed),
            patch("src.streaming.silver_metadata.recover_bronze_from_archive") as mock_recover,
            patch.object(spark_session, "stop"),
            pytest.raises(RuntimeError, match="self-healed"),
        ):
            pipeline("2026-05-28", raise_on_error=True)

        assert mock_recover.called is expect_replay
        if expect_replay:
            assert mock_recover.call_args.kwargs["watermark_column"] == "extraction_date"
