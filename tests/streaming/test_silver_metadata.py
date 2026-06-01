import importlib
import sys
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
from pyspark.sql.types import DateType, DecimalType, IntegerType, LongType, StringType, TimestampType


def test_silver_metadata_cleaning_and_casting(spark_session, tmp_path):
    """
    Test that the Silver metadata pipeline correctly cleans and casts columns.
    """
    # Set up isolated temporary directories
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

    # Convert to Spark DataFrame and write as a Delta table (source format)
    df_bronze_spark = spark_session.createDataFrame(df_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_metadata_dir))

    # Mock environment configuration directories and bypass Spark stop during tests
    with (
        patch("src.producer.config.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.producer.config.SILVER_METADATA_DIR", silver_metadata_dir),
        patch("src.streaming.spark_session.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        if "src.streaming.silver_metadata" in sys.modules:
            importlib.reload(sys.modules["src.streaming.silver_metadata"])
        else:
            import src.streaming.silver_metadata  # noqa: F401

    # Read pipeline output and assert formatting/casing rules
    df_silver_metadata = spark_session.read.format("delta").load(str(silver_metadata_dir))
    assert df_silver_metadata.count() == 1
    row = df_silver_metadata.collect()[0]

    # 1. Validate cleaned values (trim, upper, and exchange standardization)
    assert row.ticker == "AAPL"
    assert row.short_name == "Apple Inc."
    assert row.sector == "Technology"
    assert row.industry == "Consumer Electronics"
    assert row.country == "United States"
    assert row.isin == "US0378331005"
    assert row.currency == "usd"
    assert row.exchange == "B3"  # Validate conversion of 'sao' -> 'SAO' -> 'B3'

    # 2. Validate converted data types
    assert isinstance(df_silver_metadata.schema["ticker"].dataType, StringType)
    assert isinstance(df_silver_metadata.schema["short_name"].dataType, StringType)
    assert isinstance(df_silver_metadata.schema["sector"].dataType, StringType)
    assert isinstance(df_silver_metadata.schema["exchange"].dataType, StringType)
    assert isinstance(df_silver_metadata.schema["extraction_date"].dataType, DateType)
    assert isinstance(df_silver_metadata.schema["ingestion_timestamp"].dataType, TimestampType)

    # Numerical validations (Integer and BigInt/Long)
    assert isinstance(df_silver_metadata.schema["full_time_employees"].dataType, IntegerType)
    assert isinstance(df_silver_metadata.schema["market_cap"].dataType, LongType)

    # Validate decimal(10,2) data type for yields
    assert isinstance(df_silver_metadata.schema["dividend_yield"].dataType, DecimalType)
    assert df_silver_metadata.schema["dividend_yield"].dataType.precision == 10
    assert df_silver_metadata.schema["dividend_yield"].dataType.scale == 2


def test_silver_metadata_null_dropping(spark_session, tmp_path):
    """
    Test that rows containing null values in critical columns are filtered out.
    """
    # Set up isolated temporary directories
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    # 1 valid row (MSFT) and multiple invalid rows (with nulls in critical columns)
    # Using a list of dicts directly prevents Pandas from converting None to string "nan" or float NaN.
    data_bronze = [
        # 1. Valid row
        {
            "ticker": "MSFT",
            "short_name": "Microsoft Corp.",
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
        },
        # 2. Invalid row: null ticker
        {
            "ticker": None,
            "short_name": "No Ticker",
            "sector": "Technology",
            "industry": "Software",
            "country": "USA",
            "isin": "US123",
            "full_time_employees": 100,
            "exchange": "NASDAQ",
            "market_cap": 1000,
            "currency": "USD",
            "dividend_yield": 0.0,
            "extraction_date": "2026-05-28",
            "ingestion_timestamp": "2026-05-28 10:00:00",
        },
        # 3. Invalid row: null short_name
        {
            "ticker": "AAPL",
            "short_name": None,
            "sector": "Technology",
            "industry": "Electronics",
            "country": "USA",
            "isin": "US456",
            "full_time_employees": 160000,
            "exchange": "NASDAQ",
            "market_cap": 2600000000000,
            "currency": "USD",
            "dividend_yield": 0.005,
            "extraction_date": "2026-05-28",
            "ingestion_timestamp": "2026-05-28 10:00:00",
        },
        # 4. Invalid row: null sector
        {
            "ticker": "GOOGL",
            "short_name": "Google Inc.",
            "sector": None,
            "industry": "Internet",
            "country": "USA",
            "isin": "US789",
            "full_time_employees": 180000,
            "exchange": "NASDAQ",
            "market_cap": 1700000000000,
            "currency": "USD",
            "dividend_yield": 0.0,
            "extraction_date": "2026-05-28",
            "ingestion_timestamp": "2026-05-28 10:00:00",
        },
        # 5. Invalid row: null exchange
        {
            "ticker": "AMZN",
            "short_name": "Amazon Inc.",
            "sector": "Technology",
            "industry": "Retail",
            "country": "USA",
            "isin": "US101",
            "full_time_employees": 1500000,
            "exchange": None,
            "market_cap": 1800000000000,
            "currency": "USD",
            "dividend_yield": 0.0,
            "extraction_date": "2026-05-28",
            "ingestion_timestamp": "2026-05-28 10:00:00",
        },
    ]

    df_bronze_spark = spark_session.createDataFrame(data_bronze)
    df_bronze_spark.write.format("delta").mode("overwrite").save(str(bronze_metadata_dir))

    with (
        patch("src.producer.config.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.producer.config.SILVER_METADATA_DIR", silver_metadata_dir),
        patch("src.streaming.spark_session.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        if "src.streaming.silver_metadata" in sys.modules:
            importlib.reload(sys.modules["src.streaming.silver_metadata"])
        else:
            import src.streaming.silver_metadata  # noqa: F401

    # Read pipeline output and assert that only the valid row survived
    df_silver_metadata = spark_session.read.format("delta").load(str(silver_metadata_dir))
    assert df_silver_metadata.count() == 1
    assert df_silver_metadata.collect()[0].ticker == "MSFT"


def test_silver_metadata_exchange_standardization(spark_session, tmp_path):
    """
    Test that exchange names are normalized correctly:
    - 'SAO' to 'B3'
    - 'NYQ' to 'NYSE'
    - 'NMS', 'NGM', 'NCM', 'NASDAQ' to 'NASDAQ'
    - other exchange names should be kept as is.
    """
    # Set up isolated temporary directories
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    # Prepare input containing different cases for the 'exchange' field
    data_bronze = [
        # 1. 'sao' with spaces and lowercase -> should become 'B3'
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
        # 2. 'nyq' lowercase -> should become 'NYSE'
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
        # 3. 'nms' -> should become 'NASDAQ'
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
        # 4. 'ngm' -> should become 'NASDAQ'
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
        # 5. 'ncm' -> should become 'NASDAQ'
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
        # 6. 'nasdaq' with spaces and lowercase -> should become 'NASDAQ'
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
        # 7. Other exchange with spaces and lowercase (e.g. lse) -> should keep upper-trimmed 'LSE'
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
        patch("src.producer.config.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.producer.config.SILVER_METADATA_DIR", silver_metadata_dir),
        patch("src.streaming.spark_session.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        if "src.streaming.silver_metadata" in sys.modules:
            importlib.reload(sys.modules["src.streaming.silver_metadata"])
        else:
            import src.streaming.silver_metadata  # noqa: F401

    # Read output and verify the mapping of each exchange
    df_silver_metadata = spark_session.read.format("delta").load(str(silver_metadata_dir))

    # Check count is 7
    assert df_silver_metadata.count() == 7

    # Gather rows as dict by ticker to ease assertion checking
    rows = {r.ticker: r.exchange for r in df_silver_metadata.collect()}

    assert rows["PETR4"] == "B3"  # 'sao' -> 'B3'
    assert rows["IBM"] == "NYSE"  # 'nyq' -> 'NYSE'
    assert rows["AAPL"] == "NASDAQ"  # 'nms' -> 'NASDAQ'
    assert rows["MSFT"] == "NASDAQ"  # 'ngm' -> 'NASDAQ'
    assert rows["GOOGL"] == "NASDAQ"  # 'ncm' -> 'NASDAQ'
    assert rows["AMZN"] == "NASDAQ"  # 'nasdaq' -> 'NASDAQ'
    assert rows["BP"] == "LSE"  # 'lse' -> 'LSE' (other kept as is, but upper-trimmed)


def test_silver_metadata_deduplication_by_ticker(spark_session, tmp_path):
    """
    Test that metadata is deduplicated per ticker, keeping only the most recent record.
    """
    # Set up isolated temporary directories
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
        patch("src.producer.config.BRONZE_METADATA_DIR", bronze_metadata_dir),
        patch("src.producer.config.SILVER_METADATA_DIR", silver_metadata_dir),
        patch("src.streaming.spark_session.create_spark_session", return_value=spark_session),
        patch.object(spark_session, "stop"),
    ):
        if "src.streaming.silver_metadata" in sys.modules:
            importlib.reload(sys.modules["src.streaming.silver_metadata"])
        else:
            import src.streaming.silver_metadata  # noqa: F401

    # Read output and verify deduplication
    df_silver_metadata = spark_session.read.format("delta").load(str(silver_metadata_dir))
    assert df_silver_metadata.count() == 3

    # Check that the latest records were kept for each ticker
    rows = {r.ticker: r.ingestion_timestamp for r in df_silver_metadata.collect()}
    assert rows["MSFT"] == datetime(2026, 5, 28, 14, 0, 0)
    assert rows["AAPL"] == datetime(2026, 5, 28, 13, 0, 0)
    assert rows["GOOGL"] == datetime(2026, 5, 28, 16, 0, 0)


def test_silver_metadata_failure(spark_session, tmp_path):
    """
    Test Silver metadata pipeline exit code 1 when processing fails.
    """
    from loguru import logger

    # Set up isolated temporary directories
    bronze_metadata_dir = tmp_path / "bronze_metadata"
    bronze_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    # Input data
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

    # Add a dynamic sink to loguru to capture ERROR logs
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="ERROR")

    try:
        # Inject a write exception to simulate a failure and check exit behavior
        with (
            patch("src.producer.config.BRONZE_METADATA_DIR", bronze_metadata_dir),
            patch("src.producer.config.SILVER_METADATA_DIR", silver_metadata_dir),
            patch("src.streaming.spark_session.create_spark_session", return_value=spark_session),
            patch("src.streaming.utils.write_delta_table", side_effect=Exception("Simulated writing failure")),
            patch.object(spark_session, "stop"),
        ):
            with pytest.raises(SystemExit) as exc_info:
                if "src.streaming.silver_metadata" in sys.modules:
                    importlib.reload(sys.modules["src.streaming.silver_metadata"])
                else:
                    import src.streaming.silver_metadata  # noqa: F401
    finally:
        logger.remove(sink_id)

    # Check failure code and transaction isolation
    assert exc_info.value.code == 1
    assert len(list(silver_metadata_dir.glob("**/*.parquet"))) == 0

    # Verify that the exception is logged to our loguru sink
    log_content = "".join(captured_logs)
    assert "Failed to process Silver layer metadata" in log_content
    assert "Simulated writing failure" in log_content
