import argparse
import sys
from datetime import date

from pyspark.sql.functions import col

from src.producer.config import SILVER_METADATA_DIR, SILVER_PRICES_DIR
from src.streaming.spark_session import create_spark_session
from src.streaming.utils import get_clickhouse_client, read_delta_table
from src.utils.logger import logger


def run_gold(exec_date: str) -> None:
    """Clean and load prices and metadata from Silver to Gold Layer (ClickHouse) using Spark."""
    try:
        date.fromisoformat(exec_date)
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD format.")
        sys.exit(1)

    logger.info(f"Starting Gold layer processing (execution date: {exec_date})...")

    # Creating spark session
    spark = create_spark_session()
    try:
        # Check delta tables existence
        prices_exist = SILVER_PRICES_DIR.exists() and (SILVER_PRICES_DIR / "_delta_log").exists()
        metadata_exist = SILVER_METADATA_DIR.exists() and (SILVER_METADATA_DIR / "_delta_log").exists()

        if not prices_exist and not metadata_exist:
            logger.warning(
                "Neither Silver Prices nor Silver Metadata Delta tables exist. Skipping Gold layer processing."
            )
            return

        # Creating clickhouse connection
        client = get_clickhouse_client()

        if prices_exist:
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

            client.command("CREATE TABLE IF NOT EXISTS stock_market.fact_prices_staging AS stock_market.fact_prices")
            client.command("TRUNCATE TABLE stock_market.fact_prices_staging")
            client.insert_df("stock_market.fact_prices_staging", df_prices_pd)
            client.command("EXCHANGE TABLES stock_market.fact_prices AND stock_market.fact_prices_staging")
            client.command("DROP TABLE IF EXISTS stock_market.fact_prices_staging")
            logger.success("Silver Prices loaded to Gold successfully.")
        else:
            logger.info("Silver Prices Delta table not found. Skipping prices load.")

        if metadata_exist:
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

            client.command(
                "CREATE TABLE IF NOT EXISTS stock_market.dim_companies_staging AS stock_market.dim_companies"
            )
            client.command("TRUNCATE TABLE stock_market.dim_companies_staging")
            client.insert_df("stock_market.dim_companies_staging", df_metadata_pd)
            client.command("EXCHANGE TABLES stock_market.dim_companies AND stock_market.dim_companies_staging")
            client.command("DROP TABLE IF EXISTS stock_market.dim_companies_staging")
            logger.success("Silver Metadata loaded to Gold successfully.")
        else:
            logger.info("Silver Metadata Delta table not found. Skipping metadata load.")

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
    args, _ = parser.parse_known_args()

    run_gold(args.date)


if __name__ == "__main__":
    main()
