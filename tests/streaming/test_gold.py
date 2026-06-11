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


def test_gold_load_success(spark_session, tmp_path):
    """
    Test successful Gold layer pipeline execution and ClickHouse integrations.
    """

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
            "market_cap": [2600000000000],
            "currency": ["USD"],
            "dividend_yield": [0.005],
            "extraction_date": [date(2026, 5, 28)],
            "ingestion_timestamp": ["2026-05-28 10:00:00"],
        }
    )

    spark_session.createDataFrame(df_prices).write.format("delta").mode("overwrite").save(str(silver_prices_dir))
    spark_session.createDataFrame(df_metadata).write.format("delta").mode("overwrite").save(str(silver_metadata_dir))

    mock_client = MagicMock()

    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="INFO")

    try:
        with (
            patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
            patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
            patch("src.streaming.gold.get_clickhouse_client", return_value=mock_client),
            patch("src.streaming.gold.create_spark_session", return_value=spark_session),
            patch.object(spark_session, "stop"),
        ):
            main()
    finally:
        logger.remove(sink_id)

    mock_client.command.assert_any_call(
        "CREATE TABLE IF NOT EXISTS stock_market.fact_prices_staging AS stock_market.fact_prices"
    )
    mock_client.command.assert_any_call(
        "CREATE TABLE IF NOT EXISTS stock_market.dim_companies_staging AS stock_market.dim_companies"
    )

    mock_client.command.assert_any_call("TRUNCATE TABLE stock_market.fact_prices_staging")
    mock_client.command.assert_any_call("TRUNCATE TABLE stock_market.dim_companies_staging")

    assert mock_client.insert_df.call_count == 2
    first_call_args = mock_client.insert_df.call_args_list[0][0]
    second_call_args = mock_client.insert_df.call_args_list[1][0]

    assert first_call_args[0] == "stock_market.fact_prices_staging"
    assert isinstance(first_call_args[1], pd.DataFrame)
    assert first_call_args[1].shape[0] == 1
    assert first_call_args[1].iloc[0]["ticker"] == "AAPL"

    assert second_call_args[0] == "stock_market.dim_companies_staging"
    assert isinstance(second_call_args[1], pd.DataFrame)
    assert second_call_args[1].shape[0] == 1
    assert second_call_args[1].iloc[0]["ticker"] == "AAPL"

    mock_client.command.assert_any_call("EXCHANGE TABLES stock_market.fact_prices AND stock_market.fact_prices_staging")
    mock_client.command.assert_any_call(
        "EXCHANGE TABLES stock_market.dim_companies AND stock_market.dim_companies_staging"
    )

    mock_client.command.assert_any_call("DROP TABLE IF EXISTS stock_market.fact_prices_staging")
    mock_client.command.assert_any_call("DROP TABLE IF EXISTS stock_market.dim_companies_staging")

    log_content = "".join(captured_logs)
    assert "Starting Gold layer processing" in log_content
    assert "Gold layer processing completed successfully" in log_content


def test_gold_clickhouse_interaction_failure(spark_session, tmp_path):
    """
    Test Gold pipeline exit code 1 when ClickHouse query or insertion fails.
    """
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
            "market_cap": [2600000000000],
            "currency": ["USD"],
            "dividend_yield": [0.005],
            "extraction_date": [date(2026, 5, 28)],
            "ingestion_timestamp": ["2026-05-28 10:00:00"],
        }
    )

    spark_session.createDataFrame(df_prices).write.format("delta").mode("overwrite").save(str(silver_prices_dir))
    spark_session.createDataFrame(df_metadata).write.format("delta").mode("overwrite").save(str(silver_metadata_dir))

    mock_client = MagicMock()
    mock_client.command.side_effect = Exception("Simulated ClickHouse connection failure")

    # Capture logs to assert expected error messages on pipeline failure
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="ERROR")

    try:
        with (
            patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
            patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
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
    """
    Test Gold pipeline execution when Silver input dataset is empty.
    """
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
            "market_cap",
            "currency",
            "dividend_yield",
            "extraction_date",
            "ingestion_timestamp",
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

    metadata_schema = StructType(
        [
            StructField("ticker", StringType(), True),
            StructField("short_name", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("industry", StringType(), True),
            StructField("country", StringType(), True),
            StructField("isin", StringType(), True),
            StructField("full_time_employees", IntegerType(), True),
            StructField("exchange", StringType(), True),
            StructField("market_cap", LongType(), True),
            StructField("currency", StringType(), True),
            StructField("dividend_yield", DecimalType(10, 2), True),
            StructField("extraction_date", DateType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
        ]
    )

    spark_session.createDataFrame(df_prices, schema=prices_schema).write.format("delta").mode("overwrite").save(
        str(silver_prices_dir)
    )
    spark_session.createDataFrame(df_metadata, schema=metadata_schema).write.format("delta").mode("overwrite").save(
        str(silver_metadata_dir)
    )

    mock_client = MagicMock()

    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="INFO")

    try:
        with (
            patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
            patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
            patch("src.streaming.gold.get_clickhouse_client", return_value=mock_client),
            patch("src.streaming.gold.create_spark_session", return_value=spark_session),
            patch.object(spark_session, "stop"),
        ):
            main()
    finally:
        logger.remove(sink_id)

    # Check that staging creation and exchange were still executed
    assert mock_client.insert_df.call_count == 2
    first_call_args = mock_client.insert_df.call_args_list[0][0]
    second_call_args = mock_client.insert_df.call_args_list[1][0]

    assert first_call_args[0] == "stock_market.fact_prices_staging"
    assert first_call_args[1].empty

    assert second_call_args[0] == "stock_market.dim_companies_staging"
    assert second_call_args[1].empty

    log_content = "".join(captured_logs)
    assert "Starting Gold layer processing" in log_content
    assert "Gold layer processing completed successfully" in log_content


def test_gold_date_from_arguments(spark_session, tmp_path):
    """
    Test that Gold pipeline parses --date from CLI arguments correctly.
    """
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
            "market_cap": [2600000000000],
            "currency": ["USD"],
            "dividend_yield": [0.005],
            "extraction_date": [date(2026, 5, 28)],
            "ingestion_timestamp": ["2026-05-28 10:00:00"],
        }
    )

    spark_session.createDataFrame(df_prices).write.format("delta").mode("overwrite").save(str(silver_prices_dir))
    spark_session.createDataFrame(df_metadata).write.format("delta").mode("overwrite").save(str(silver_metadata_dir))

    mock_client = MagicMock()

    with (
        patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
        patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
        patch("src.streaming.gold.get_clickhouse_client", return_value=mock_client),
        patch("src.streaming.gold.create_spark_session", return_value=spark_session),
        patch("sys.argv", ["gold.py", "--date", "2026-05-28"]),
        patch.object(spark_session, "stop"),
    ):
        main()

    assert mock_client.insert_df.call_count == 2


def test_gold_invalid_date_format(spark_session, tmp_path):
    """
    Test that an invalid date format passed to --date exits with code 1.
    """
    silver_prices_dir = tmp_path / "silver_prices"
    silver_prices_dir.mkdir(parents=True, exist_ok=True)

    silver_metadata_dir = tmp_path / "silver_metadata"
    silver_metadata_dir.mkdir(parents=True, exist_ok=True)

    mock_client = MagicMock()

    # Capture logs to assert expected error messages on pipeline failure
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="ERROR")

    try:
        with (
            patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
            patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
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
    """
    Test that if only Silver Prices exists and Silver Metadata is missing,
    only Prices is processed and Metadata is skipped gracefully.
    """
    silver_prices_dir = tmp_path / "silver_prices"
    silver_prices_dir.mkdir(parents=True, exist_ok=True)

    # Do not create silver_metadata_dir or write anything to it
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
            patch("src.streaming.gold.get_clickhouse_client", return_value=mock_client),
            patch("src.streaming.gold.create_spark_session", return_value=spark_session),
            patch.object(spark_session, "stop"),
        ):
            main()
    finally:
        logger.remove(sink_id)

    # Check that prices staging table was created and swapped
    mock_client.command.assert_any_call(
        "CREATE TABLE IF NOT EXISTS stock_market.fact_prices_staging AS stock_market.fact_prices"
    )
    # Check that metadata staging table was NOT created
    with pytest.raises(AssertionError):
        mock_client.command.assert_any_call(
            "CREATE TABLE IF NOT EXISTS stock_market.dim_companies_staging AS stock_market.dim_companies"
        )

    assert mock_client.insert_df.call_count == 1
    assert mock_client.insert_df.call_args_list[0][0][0] == "stock_market.fact_prices_staging"

    log_content = "".join(captured_logs)
    assert "Processing Silver Prices" in log_content
    assert "Silver Metadata Delta table not found. Skipping metadata load" in log_content


def test_gold_missing_prices_delta_table(spark_session, tmp_path):
    """
    Test that if only Silver Metadata exists and Silver Prices is missing,
    only Metadata is processed and Prices is skipped gracefully.
    """
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
            "market_cap": [2600000000000],
            "currency": ["USD"],
            "dividend_yield": [0.005],
            "extraction_date": [date(2026, 5, 28)],
            "ingestion_timestamp": ["2026-05-28 10:00:00"],
        }
    )
    spark_session.createDataFrame(df_metadata).write.format("delta").mode("overwrite").save(str(silver_metadata_dir))

    mock_client = MagicMock()
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="INFO")

    try:
        with (
            patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
            patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
            patch("src.streaming.gold.get_clickhouse_client", return_value=mock_client),
            patch("src.streaming.gold.create_spark_session", return_value=spark_session),
            patch.object(spark_session, "stop"),
        ):
            main()
    finally:
        logger.remove(sink_id)

    # Check that dim_companies staging table was created and swapped
    mock_client.command.assert_any_call(
        "CREATE TABLE IF NOT EXISTS stock_market.dim_companies_staging AS stock_market.dim_companies"
    )
    # Check that prices staging table was NOT created
    with pytest.raises(AssertionError):
        mock_client.command.assert_any_call(
            "CREATE TABLE IF NOT EXISTS stock_market.fact_prices_staging AS stock_market.fact_prices"
        )

    assert mock_client.insert_df.call_count == 1
    assert mock_client.insert_df.call_args_list[0][0][0] == "stock_market.dim_companies_staging"

    log_content = "".join(captured_logs)
    assert "Processing Silver Metadata" in log_content
    assert "Silver Prices Delta table not found. Skipping prices load" in log_content


def test_gold_missing_both_delta_tables(spark_session, tmp_path):
    """
    Test that if both Silver Prices and Silver Metadata Delta tables are missing,
    the pipeline logs a warning and exits/returns gracefully without calling ClickHouse.
    """
    silver_prices_dir = tmp_path / "silver_prices_missing"
    silver_metadata_dir = tmp_path / "silver_metadata_missing"

    mock_client = MagicMock()
    captured_logs = []
    sink_id = logger.add(lambda msg: captured_logs.append(str(msg)), level="WARNING")

    try:
        with (
            patch("src.streaming.gold.SILVER_PRICES_DIR", silver_prices_dir),
            patch("src.streaming.gold.SILVER_METADATA_DIR", silver_metadata_dir),
            patch("src.streaming.gold.get_clickhouse_client", return_value=mock_client),
            patch("src.streaming.gold.create_spark_session", return_value=spark_session),
            patch.object(spark_session, "stop"),
        ):
            main()
    finally:
        logger.remove(sink_id)

    # ClickHouse should not be contacted at all
    assert mock_client.command.call_count == 0
    assert mock_client.insert_df.call_count == 0

    log_content = "".join(captured_logs)
    assert "Neither Silver Prices nor Silver Metadata Delta tables exist. Skipping Gold layer processing" in log_content
