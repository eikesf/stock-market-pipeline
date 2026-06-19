import argparse
import sys
from datetime import date

from clickhouse_connect.driver.client import Client
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.producer.config import SILVER_METADATA_DIR, SILVER_PRICES_DIR
from src.streaming.spark_session import create_spark_session
from src.streaming.utils import get_clickhouse_client, read_delta_table
from src.utils.logger import logger


def _load_prices_to_gold(spark: SparkSession, client: Client) -> None:
    """Read silver prices Delta table, clean it, and load it into ClickHouse fact_prices."""
    logger.info("Processing Silver Prices...")
    df_prices = read_delta_table(spark, SILVER_PRICES_DIR)
    df_prices = df_prices.select(
        col("date"),
        col("ticker"),
        col("open"),
        col("high"),
        col("low"),
        col("close"),
        col("adj_close"),
        col("volume"),
        col("dividends"),
        col("stock_splits"),
        col("ingestion_timestamp"),
    )
    df_prices_pd = df_prices.toPandas()

    if df_prices_pd.empty:
        logger.warning("Silver prices DataFrame is empty. Nothing to load to Gold.")
        return

    # Extract unique partition IDs in 'YYYYMM' format based on dates
    dates = df_prices_pd["date"].astype(str)
    affected_partitions = dates.apply(lambda x: x.replace("-", "")[:6]).unique().tolist()

    for partition in affected_partitions:
        logger.info(f"Dropping partition '{partition}' in ClickHouse fact_prices table...")
        client.command(f"ALTER TABLE stock_market.fact_prices DROP PARTITION '{partition}'")

    logger.info("Inserting new/updated prices into ClickHouse fact_prices...")
    client.insert_df("stock_market.fact_prices", df_prices_pd)
    logger.success("Silver prices loaded to gold successfully.")


def _load_metadata_to_gold(spark: SparkSession, client: Client) -> None:
    """Read silver metadata Delta table, clean it, and load it into ClickHouse dim_companies."""
    logger.info("Processing Silver Metadata...")
    df_metadata = read_delta_table(spark, SILVER_METADATA_DIR)
    df_metadata = df_metadata.select(
        col("ticker"),
        col("short_name"),
        col("sector"),
        col("industry"),
        col("country"),
        col("isin"),
        col("full_time_employees"),
        col("exchange"),
        col("market_cap"),
        col("currency"),
        col("dividend_yield"),
        col("extraction_date"),
        col("ingestion_timestamp"),
    )
    df_metadata_pd = df_metadata.toPandas()

    client.command("CREATE TABLE IF NOT EXISTS stock_market.dim_companies_staging AS stock_market.dim_companies")
    client.command("TRUNCATE TABLE stock_market.dim_companies_staging")
    client.insert_df("stock_market.dim_companies_staging", df_metadata_pd)
    client.command("EXCHANGE TABLES stock_market.dim_companies AND stock_market.dim_companies_staging")
    client.command("DROP TABLE IF EXISTS stock_market.dim_companies_staging")
    logger.success("Silver Metadata loaded to Gold successfully.")


def run_gold(exec_date: str, table: str = "all") -> None:
    """Clean and load prices and metadata from Silver to Gold Layer (ClickHouse) using Spark."""
    try:
        date.fromisoformat(exec_date)
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD format.")
        sys.exit(1)

    logger.info(f"Starting Gold layer processing for table '{table}' (execution date: {exec_date})...")

    # Creating spark session
    spark = create_spark_session()
    try:
        # Check delta tables existence based on selected table filter
        prices_exist = table in ("prices", "all") and (SILVER_PRICES_DIR / "_delta_log").exists()
        metadata_exist = table in ("metadata", "all") and (SILVER_METADATA_DIR / "_delta_log").exists()

        if not prices_exist and not metadata_exist:
            logger.warning(
                f"No matching Silver Delta tables exist to process for table '{table}'. Skipping Gold layer processing."
            )
            return

        # Creating clickhouse connection
        client = get_clickhouse_client()

        if prices_exist:
            _load_prices_to_gold(spark, client)
        elif table not in ("prices", "all"):
            logger.info("Silver Prices skipped (not requested by target table selection).")
        else:
            logger.warning("Silver Prices Delta table not found. Skipping prices load.")

        if metadata_exist:
            _load_metadata_to_gold(spark, client)
        elif table not in ("metadata", "all"):
            logger.info("Silver Metadata skipped (not requested by target table selection).")
        else:
            logger.warning("Silver Metadata Delta table not found. Skipping metadata load.")

        logger.success("Gold layer processing completed successfully.")

    except Exception as e:
        logger.exception(f"Failed to process Gold layer: {e}")
        sys.exit(1)
    finally:
        # Stopping spark session
        spark.stop()


def main() -> None:
    """CLI entrypoint for Gold layer processing."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=str,
        default=date.today().isoformat(),
        help="Date to process (format YYYY-MM-DD)",
    )
    parser.add_argument(
        "--table",
        type=str,
        default="all",
        choices=["prices", "metadata", "all"],
        help="Select which table to load (prices, metadata, or all)",
    )
    args, _ = parser.parse_known_args()

    run_gold(args.date, args.table)


if __name__ == "__main__":
    main()
