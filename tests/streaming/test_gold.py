from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from loguru import logger
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.streaming.gold import main


@pytest.fixture(autouse=True)
def mock_spark_foreach_partition():
    """Mock PySpark RDD.foreachPartition to run in-process on the driver.

    This is necessary because unittest mocks on the driver do not propagate
    to Spark executor subprocesses.
    """
    from pyspark.rdd import RDD

    original_foreach = RDD.foreachPartition

    def dummy_foreach(self, f):
        f(iter(self.collect()))

    RDD.foreachPartition = dummy_foreach
    yield
    RDD.foreachPartition = original_foreach


METADATA_SCHEMA = StructType(
    [
        StructField("ticker", StringType(), True),
        StructField("short_name", StringType(), True),
        StructField("sector", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("country", StringType(), True),
        StructField("isin", StringType(), True),
        StructField("full_time_employees", IntegerType(), True),
        StructField("exchange", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("extraction_date", DateType(), True),
        StructField("ingestion_timestamp", StringType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("is_active", IntegerType(), True),
    ]
)

METRICS_SCHEMA = StructType(
    [
        StructField("extraction_date", DateType(), True),
        StructField("ticker", StringType(), True),
        StructField("dividend_yield", DecimalType(10, 4), True),
        StructField("trailing_pe", DecimalType(10, 2), True),
        StructField("peg_ratio", DecimalType(10, 4), True),
        StructField("price_to_book", DecimalType(10, 4), True),
        StructField("enterprise_to_ebitda", DecimalType(10, 4), True),
        StructField("enterprise_to_ebit", DecimalType(10, 4), True),
        StructField("book_value", DecimalType(10, 4), True),
        StructField("trailing_eps", DecimalType(10, 4), True),
        StructField("price_to_sales", DecimalType(10, 4), True),
        StructField("operating_margins", DecimalType(10, 4), True),
        StructField("asset_turnover", DecimalType(10, 4), True),
        StructField("shares_outstanding", LongType(), True),
        StructField("market_cap", LongType(), True),
        StructField("ebitda", LongType(), True),
        StructField("total_debt", LongType(), True),
        StructField("total_cash", LongType(), True),
        StructField("debt_to_equity", DecimalType(10, 4), True),
        StructField("roa", DecimalType(10, 4), True),
        StructField("roe", DecimalType(10, 4), True),
        StructField("current_ratio", DecimalType(10, 4), True),
        StructField("gross_margins", DecimalType(10, 4), True),
        StructField("ebitda_margins", DecimalType(10, 4), True),
        StructField("profit_margins", DecimalType(10, 4), True),
        StructField("net_income_to_common", LongType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ]
)


def _setup_silver_test_data(spark_session, tmp_path):
    """Set up mock silver Delta tables (prices, metadata, metrics) for testing."""
    silver_prices_dir = tmp_path / "silver_prices"
    silver_prices_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    silver_metrics_dir = tmp_path / "silver_metrics"
    silver_metrics_dir.mkdir(parents=True, exist_ok=True)

    df_prices = pd.DataFrame(
        {
            "date": [date(2026, 5, 28)],
            "ticker": ["AAPL"],
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

    df_metadata = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "short_name": ["Apple Inc."],
            "sector": ["Technology"],
            "industry": ["Electronics"],
            "country": ["USA"],
            "isin": ["US0378331005"],
            "full_time_employees": [160000],
            "exchange": ["NASDAQ"],
            "currency": ["USD"],
            "extraction_date": [date(2026, 5, 28)],
            "ingestion_timestamp": ["2026-05-28 10:00:00"],
            "start_date": [date(2026, 5, 28)],
            "end_date": [None],
            "is_active": [1],
        }
    )

    df_metrics = pd.DataFrame(
        {
            "extraction_date": [date(2026, 5, 28)],
            "ticker": ["AAPL"],
            "dividend_yield": [0.0051],
            "trailing_pe": [15.42],
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
            "market_cap": [2600000000000],
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
            "ingestion_timestamp": [pd.Timestamp("2026-05-28 10:00:00")],
        }
    )

    spark_session.createDataFrame(df_prices).write.format("delta").mode("overwrite").save(str(silver_prices_dir))
    spark_session.createDataFrame(df_metadata, schema=METADATA_SCHEMA).write.format("delta").mode("overwrite").save(
        str(silver_metadata_dir)
    )
    from pyspark.sql.functions import col

    df_metrics_spark = spark_session.createDataFrame(df_metrics)
    for col_name, data_type in [
        ("dividend_yield", "decimal(10,4)"),
        ("trailing_pe", "decimal(10,4)"),
        ("peg_ratio", "decimal(10,4)"),
        ("price_to_book", "decimal(10,4)"),
        ("enterprise_to_ebitda", "decimal(10,4)"),
        ("enterprise_to_ebit", "decimal(10,4)"),
        ("book_value", "decimal(10,4)"),
        ("trailing_eps", "decimal(10,4)"),
        ("price_to_sales", "decimal(10,4)"),
        ("operating_margins", "decimal(10,4)"),
        ("asset_turnover", "decimal(10,4)"),
        ("shares_outstanding", "long"),
        ("market_cap", "long"),
        ("ebitda", "long"),
        ("total_debt", "long"),
        ("total_cash", "long"),
        ("debt_to_equity", "decimal(10,4)"),
        ("roa", "decimal(10,4)"),
        ("roe", "decimal(10,4)"),
        ("current_ratio", "decimal(10,4)"),
        ("gross_margins", "decimal(10,4)"),
        ("ebitda_margins", "decimal(10,4)"),
        ("profit_margins", "decimal(10,4)"),
        ("net_income_to_common", "long"),
        ("extraction_date", "date"),
        ("ingestion_timestamp", "timestamp"),
    ]:
        df_metrics_spark = df_metrics_spark.withColumn(col_name, col(col_name).cast(data_type))
    df_metrics_spark.write.format("delta").mode("overwrite").save(str(silver_metrics_dir))

    return silver_prices_dir, silver_metadata_dir, silver_metrics_dir


def test_gold_load_success(spark_session, tmp_path):
    """Test successful Gold layer pipeline execution and ClickHouse integrations."""
    silver_prices_dir, silver_metadata_dir, silver_metrics_dir = _setup_silver_test_data(spark_session, tmp_path)

    mock_client = MagicMock()

    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="INFO")

    try:
        with (
            patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
            patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
            patch("src.streaming.gold.SILVER_METRICS_DIR", silver_metrics_dir),
            patch("src.streaming.gold.get_clickhouse_client", return_value=mock_client),
            patch("src.streaming.gold.create_spark_session", return_value=spark_session),
            patch.object(spark_session, "stop"),
        ):
            main()
    finally:
        logger.remove(sink_id)

    # Assert partition drop and direct load for fact_prices
    mock_client.command.assert_any_call("ALTER TABLE stock_market.fact_prices DROP PARTITION '202605'")
    # Assert partition drop for fact_company_metrics
    mock_client.command.assert_any_call("ALTER TABLE stock_market.fact_company_metrics DROP PARTITION '202605'")

    # Assert staging table creation and exchange for dim_companies
    mock_client.command.assert_any_call(
        "CREATE TABLE IF NOT EXISTS stock_market.dim_companies_staging AS stock_market.dim_companies"
    )
    mock_client.command.assert_any_call("TRUNCATE TABLE stock_market.dim_companies_staging")
    mock_client.command.assert_any_call(
        "EXCHANGE TABLES stock_market.dim_companies AND stock_market.dim_companies_staging"
    )
    mock_client.command.assert_any_call("DROP TABLE IF EXISTS stock_market.dim_companies_staging")

    # Assert inserts
    assert mock_client.insert_df.call_count == 3

    # Map the target table name to the DataFrame that was sent in the mock
    inserted_dfs = {args[0]: args[1] for args, _ in mock_client.insert_df.call_args_list}

    # 1. Validate insertion into fact_prices
    assert "stock_market.fact_prices" in inserted_dfs
    df_prices_inserted = inserted_dfs["stock_market.fact_prices"]
    assert isinstance(df_prices_inserted, pd.DataFrame)
    assert df_prices_inserted.shape[0] == 1
    assert df_prices_inserted.iloc[0]["ticker"] == "AAPL"

    # 2. Validate insertion into dim_companies_staging
    assert "stock_market.dim_companies_staging" in inserted_dfs
    df_metadata_inserted = inserted_dfs["stock_market.dim_companies_staging"]
    assert isinstance(df_metadata_inserted, pd.DataFrame)
    assert df_metadata_inserted.shape[0] == 1
    assert df_metadata_inserted.iloc[0]["ticker"] == "AAPL"

    # 3. Validate insertion into fact_company_metrics
    assert "stock_market.fact_company_metrics" in inserted_dfs
    df_metrics_inserted = inserted_dfs["stock_market.fact_company_metrics"]
    assert isinstance(df_metrics_inserted, pd.DataFrame)
    assert df_metrics_inserted.shape[0] == 1
    assert df_metrics_inserted.iloc[0]["ticker"] == "AAPL"

    log_content = "".join(captured_logs)
    assert "Starting Gold layer processing" in log_content
    assert "Gold layer processing completed successfully" in log_content


def test_gold_clickhouse_interaction_failure(spark_session, tmp_path):
    """Test Gold pipeline exit code 1 when ClickHouse query or insertion fails."""
    silver_prices_dir = tmp_path / "silver_prices"
    silver_prices_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    df_prices = pd.DataFrame(
        {
            "date": [date(2026, 5, 28)],
            "ticker": ["AAPL"],
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

    df_metadata = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "short_name": ["Apple Inc."],
            "sector": ["Technology"],
            "industry": ["Electronics"],
            "country": ["USA"],
            "isin": ["US0378331005"],
            "full_time_employees": [160000],
            "exchange": ["NASDAQ"],
            "currency": ["USD"],
            "extraction_date": [date(2026, 5, 28)],
            "ingestion_timestamp": ["2026-05-28 10:00:00"],
            "start_date": [date(2026, 5, 28)],
            "end_date": [None],
            "is_active": [1],
        }
    )

    spark_session.createDataFrame(df_prices).write.format("delta").mode("overwrite").save(str(silver_prices_dir))
    spark_session.createDataFrame(df_metadata, schema=METADATA_SCHEMA).write.format("delta").mode("overwrite").save(
        str(silver_metadata_dir)
    )

    mock_client = MagicMock()
    mock_client.command.side_effect = Exception("Simulated ClickHouse connection failure")

    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="ERROR")

    try:
        with (
            patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
            patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
            patch("src.streaming.gold.SILVER_METRICS_DIR", tmp_path / "test_silver_metrics"),
            patch("src.streaming.gold.get_clickhouse_client", return_value=mock_client),
            patch("src.streaming.gold.create_spark_session", return_value=spark_session),
            patch.object(spark_session, "stop"),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()
    finally:
        logger.remove(sink_id)

    assert exc_info.value.code == 1

    log_content = "".join(captured_logs)
    assert "Failed to process Gold layer" in log_content
    assert "Simulated ClickHouse connection failure" in log_content


def test_gold_empty_silver_data(spark_session, tmp_path):
    """Test Gold pipeline execution when Silver input dataset is empty."""
    silver_prices_dir = tmp_path / "silver_prices"
    silver_prices_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    df_prices = pd.DataFrame(
        columns=[
            "date",
            "ticker",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
            "dividends",
            "stock_splits",
            "ingestion_timestamp",
        ]
    )
    df_metadata = pd.DataFrame(
        columns=[
            "ticker",
            "short_name",
            "sector",
            "industry",
            "country",
            "isin",
            "full_time_employees",
            "exchange",
            "currency",
            "extraction_date",
            "ingestion_timestamp",
            "start_date",
            "end_date",
            "is_active",
        ]
    )

    prices_schema = StructType(
        [
            StructField("date", DateType(), True),
            StructField("ticker", StringType(), True),
            StructField("open", DecimalType(10, 2), True),
            StructField("high", DecimalType(10, 2), True),
            StructField("low", DecimalType(10, 2), True),
            StructField("close", DecimalType(10, 2), True),
            StructField("adj_close", DecimalType(10, 2), True),
            StructField("volume", LongType(), True),
            StructField("dividends", DecimalType(10, 2), True),
            StructField("stock_splits", DecimalType(10, 4), True),
            StructField("ingestion_timestamp", TimestampType(), True),
        ]
    )

    spark_session.createDataFrame(df_prices, schema=prices_schema).write.format("delta").mode("overwrite").save(
        str(silver_prices_dir)
    )
    spark_session.createDataFrame(df_metadata, schema=METADATA_SCHEMA).write.format("delta").mode("overwrite").save(
        str(silver_metadata_dir)
    )

    mock_client = MagicMock()

    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="INFO")

    try:
        with (
            patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
            patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
            patch("src.streaming.gold.SILVER_METRICS_DIR", tmp_path / "test_silver_metrics"),
            patch("src.streaming.gold.get_clickhouse_client", return_value=mock_client),
            patch("src.streaming.gold.create_spark_session", return_value=spark_session),
            patch.object(spark_session, "stop"),
        ):
            main()
    finally:
        logger.remove(sink_id)

    assert mock_client.insert_df.call_count == 0

    log_content = "".join(captured_logs)
    assert "Starting Gold layer processing" in log_content
    assert "Gold layer processing completed successfully" in log_content


def test_gold_date_from_arguments(spark_session, tmp_path):
    """Test that Gold pipeline parses --date from CLI arguments correctly."""
    silver_prices_dir = tmp_path / "silver_prices"
    silver_prices_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    df_prices = pd.DataFrame(
        {
            "date": [date(2026, 5, 28)],
            "ticker": ["AAPL"],
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

    df_metadata = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "short_name": ["Apple Inc."],
            "sector": ["Technology"],
            "industry": ["Electronics"],
            "country": ["USA"],
            "isin": ["US0378331005"],
            "full_time_employees": [160000],
            "exchange": ["NASDAQ"],
            "currency": ["USD"],
            "extraction_date": [date(2026, 5, 28)],
            "ingestion_timestamp": ["2026-05-28 10:00:00"],
            "start_date": [date(2026, 5, 28)],
            "end_date": [None],
            "is_active": [1],
        }
    )

    spark_session.createDataFrame(df_prices).write.format("delta").mode("overwrite").save(str(silver_prices_dir))
    spark_session.createDataFrame(df_metadata, schema=METADATA_SCHEMA).write.format("delta").mode("overwrite").save(
        str(silver_metadata_dir)
    )

    mock_client = MagicMock()

    with (
        patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
        patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
        patch("src.streaming.gold.SILVER_METRICS_DIR", tmp_path / "test_silver_metrics"),
        patch("src.streaming.gold.get_clickhouse_client", return_value=mock_client),
        patch("src.streaming.gold.create_spark_session", return_value=spark_session),
        patch("sys.argv", ["gold.py", "--date", "2026-05-28"]),
        patch.object(spark_session, "stop"),
    ):
        main()

    assert mock_client.insert_df.call_count == 2


def test_gold_invalid_date_format(spark_session, tmp_path):
    """Test that an invalid date format passed to --date exits with code 1."""
    silver_prices_dir = tmp_path / "silver_prices"
    silver_prices_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    mock_client = MagicMock()

    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="ERROR")

    try:
        with (
            patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
            patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
            patch("src.streaming.gold.SILVER_METRICS_DIR", tmp_path / "test_silver_metrics"),
            patch("src.streaming.gold.get_clickhouse_client", return_value=mock_client),
            patch("src.streaming.gold.create_spark_session", return_value=spark_session),
            patch("sys.argv", ["gold.py", "--date", "invalid_date_format"]),
            patch.object(spark_session, "stop"),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()
    finally:
        logger.remove(sink_id)

    assert exc_info.value.code == 1
    log_content = "".join(captured_logs)
    assert "Invalid date format" in log_content


def test_gold_missing_metadata_delta_table(spark_session, tmp_path):
    """Test that if only Silver Prices exists and Silver Metadata is missing, only Prices is processed."""
    silver_prices_dir = tmp_path / "silver_prices"
    silver_prices_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata_missing"

    df_prices = pd.DataFrame(
        {
            "date": [date(2026, 5, 28)],
            "ticker": ["AAPL"],
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
    spark_session.createDataFrame(df_prices).write.format("delta").mode("overwrite").save(str(silver_prices_dir))

    mock_client = MagicMock()
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="INFO")

    try:
        with (
            patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
            patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
            patch("src.streaming.gold.SILVER_METRICS_DIR", tmp_path / "test_silver_metrics"),
            patch("src.streaming.gold.get_clickhouse_client", return_value=mock_client),
            patch("src.streaming.gold.create_spark_session", return_value=spark_session),
            patch.object(spark_session, "stop"),
        ):
            main()
    finally:
        logger.remove(sink_id)

    mock_client.command.assert_any_call("ALTER TABLE stock_market.fact_prices DROP PARTITION '202605'")
    with pytest.raises(AssertionError):
        mock_client.command.assert_any_call(
            "CREATE TABLE IF NOT EXISTS stock_market.dim_companies_staging AS stock_market.dim_companies"
        )

    assert mock_client.insert_df.call_count == 1
    assert mock_client.insert_df.call_args_list[0][0][0] == "stock_market.fact_prices"

    log_content = "".join(captured_logs)
    assert "Processing Silver Prices" in log_content
    assert "Silver Metadata Delta table not found. Skipping metadata load" in log_content


def test_gold_missing_prices_delta_table(spark_session, tmp_path):
    """Test that if only Silver Metadata exists and Silver Prices is missing, only Metadata is processed."""
    silver_prices_dir = tmp_path / "silver_prices_missing"
    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    df_metadata = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "short_name": ["Apple Inc."],
            "sector": ["Technology"],
            "industry": ["Electronics"],
            "country": ["USA"],
            "isin": ["US0378331005"],
            "full_time_employees": [160000],
            "exchange": ["NASDAQ"],
            "currency": ["USD"],
            "extraction_date": [date(2026, 5, 28)],
            "ingestion_timestamp": ["2026-05-28 10:00:00"],
            "start_date": [date(2026, 5, 28)],
            "end_date": [None],
            "is_active": [1],
        }
    )
    spark_session.createDataFrame(df_metadata, schema=METADATA_SCHEMA).write.format("delta").mode("overwrite").save(
        str(silver_metadata_dir)
    )

    mock_client = MagicMock()
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="INFO")

    try:
        with (
            patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
            patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
            patch("src.streaming.gold.SILVER_METRICS_DIR", tmp_path / "test_silver_metrics"),
            patch("src.streaming.gold.get_clickhouse_client", return_value=mock_client),
            patch("src.streaming.gold.create_spark_session", return_value=spark_session),
            patch.object(spark_session, "stop"),
        ):
            main()
    finally:
        logger.remove(sink_id)

    mock_client.command.assert_any_call(
        "CREATE TABLE IF NOT EXISTS stock_market.dim_companies_staging AS stock_market.dim_companies"
    )
    with pytest.raises(AssertionError):
        mock_client.command.assert_any_call("ALTER TABLE stock_market.fact_prices DROP PARTITION '202605'")

    assert mock_client.insert_df.call_count == 1
    assert mock_client.insert_df.call_args_list[0][0][0] == "stock_market.dim_companies_staging"

    log_content = "".join(captured_logs)
    assert "Processing Silver Metadata" in log_content
    assert "Silver Prices Delta table not found. Skipping prices load" in log_content


def test_gold_missing_both_delta_tables(spark_session, tmp_path):
    """Test that if both Silver Prices and Silver Metadata Delta tables are missing, the pipeline skips."""
    silver_prices_dir = tmp_path / "silver_prices_missing"
    silver_metadata_dir = tmp_path / "silver_metadata_missing"

    mock_client = MagicMock()
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="WARNING")

    try:
        with (
            patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
            patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
            patch("src.streaming.gold.SILVER_METRICS_DIR", silver_prices_dir),
            patch("src.streaming.gold.get_clickhouse_client", return_value=mock_client),
            patch("src.streaming.gold.create_spark_session", return_value=spark_session),
            patch.object(spark_session, "stop"),
        ):
            main()
    finally:
        logger.remove(sink_id)

    assert mock_client.command.call_count == 0
    assert mock_client.insert_df.call_count == 0

    log_content = "".join(captured_logs)
    assert (
        "Neither Silver Prices nor Silver Metadata Delta tables exist" in log_content
        or "No matching Silver Delta tables exist" in log_content
    )


def test_gold_selective_loading(spark_session, tmp_path):
    """Test that run_gold with target table selection only processes the requested table."""
    test_cases = [
        ("prices", "fact_prices", "Silver Metadata skipped (not requested by target table selection)"),
        ("metadata", "dim_companies", "Silver Prices skipped (not requested by target table selection)"),
    ]

    for table_param, expected_processed, expected_skipped_log in test_cases:
        silver_prices_dir = tmp_path / f"silver_prices_{table_param}"
        silver_prices_dir.mkdir(parents=True, exist_ok=True)

        silver_metadata_dir = tmp_path / f"silver_metadata_{table_param}"
        silver_metadata_dir.mkdir(parents=True, exist_ok=True)

        df_prices = pd.DataFrame(
            {
                "date": [date(2026, 5, 28)],
                "ticker": ["AAPL"],
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

        df_metadata = pd.DataFrame(
            {
                "ticker": ["AAPL"],
                "short_name": ["Apple Inc."],
                "sector": ["Technology"],
                "industry": ["Electronics"],
                "country": ["USA"],
                "isin": ["US0378331005"],
                "full_time_employees": [160000],
                "exchange": ["NASDAQ"],
                "currency": ["USD"],
                "extraction_date": [date(2026, 5, 28)],
                "ingestion_timestamp": ["2026-05-28 10:00:00"],
                "start_date": [date(2026, 5, 28)],
                "end_date": [None],
                "is_active": [1],
            }
        )

        spark_session.createDataFrame(df_prices).write.format("delta").mode("overwrite").save(str(silver_prices_dir))
        spark_session.createDataFrame(df_metadata, schema=METADATA_SCHEMA).write.format("delta").mode("overwrite").save(
            str(silver_metadata_dir)
        )

        mock_client = MagicMock()
        captured_logs = []
        sink_id = logger.add(lambda msg, logs=captured_logs: logs.append(str(msg)), level="INFO")

        try:
            from src.streaming.gold import run_gold

            with (
                patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
                patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
                patch("src.streaming.gold.SILVER_METRICS_DIR", tmp_path / f"silver_metrics_{table_param}"),
                patch("src.streaming.gold.get_clickhouse_client", return_value=mock_client),
                patch("src.streaming.gold.create_spark_session", return_value=spark_session),
                patch.object(spark_session, "stop"),
            ):
                run_gold(exec_date="2026-05-28", table=table_param)
        finally:
            logger.remove(sink_id)

        if table_param == "prices":
            mock_client.command.assert_any_call("ALTER TABLE stock_market.fact_prices DROP PARTITION '202605'")
            assert mock_client.insert_df.call_count == 1
            assert mock_client.insert_df.call_args_list[0][0][0] == "stock_market.fact_prices"
        else:
            mock_client.command.assert_any_call(
                f"CREATE TABLE IF NOT EXISTS stock_market.{expected_processed}_staging AS stock_market.{expected_processed}"
            )
            assert mock_client.insert_df.call_count == 1
            assert mock_client.insert_df.call_args_list[0][0][0] == f"stock_market.{expected_processed}_staging"

        log_content = "".join(captured_logs)
        assert expected_skipped_log in log_content
