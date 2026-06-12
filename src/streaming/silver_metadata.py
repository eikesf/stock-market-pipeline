import argparse
import sys
from datetime import date

from pyspark.sql.functions import col, row_number, trim, upper, when
from pyspark.sql.window import Window

from src.producer.config import BRONZE_METADATA_DIR, SILVER_METADATA_DIR
from src.streaming.spark_session import create_spark_session
from src.streaming.utils import read_delta_table, write_delta_table
from src.utils.logger import logger


def run_silver_metadata(exec_date: str) -> None:
    """Clean and deduplicate stock metadata from Bronze to Silver Layer using Spark."""
    try:
        date.fromisoformat(exec_date)
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD format.")
        sys.exit(1)

    logger.info(f"Starting Silver layer processing for stock metadata (execution date: {exec_date})...")

    spark = create_spark_session()
    try:
        # Reading bronze metadata
        metadata_df_bronze = read_delta_table(spark, BRONZE_METADATA_DIR)

        # Cleaning and organizing the metadata dataframe
        metadata_df_silver = (
            metadata_df_bronze.na.drop(subset=["ticker", "sector", "exchange", "short_name"])
            .withColumn("ticker", upper(trim(col("ticker").cast("string"))))
            .withColumn("short_name", trim(col("short_name").cast("string")))
            .withColumn("sector", trim(col("sector").cast("string")))
            .withColumn("industry", trim(col("industry").cast("string")))
            .withColumn("country", trim(col("country").cast("string")))
            .withColumn("isin", trim(col("isin").cast("string")))
            .withColumn("full_time_employees", col("full_time_employees").cast("integer"))
            .withColumn("exchange", upper(trim(col("exchange").cast("string"))))
            .withColumn("market_cap", col("market_cap").cast("bigint"))
            .withColumn("currency", trim(col("currency").cast("string")))
            .withColumn("dividend_yield", col("dividend_yield").cast("decimal(10,2)"))
            .withColumn("extraction_date", col("extraction_date").cast("date"))
            .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("timestamp"))
        )

        # Adjusting the exchange names to correspond to the pattern
        metadata_df_silver = metadata_df_silver.withColumn(
            "exchange",
            when(col("exchange") == "SAO", "B3")
            .when(col("exchange") == "NYQ", "NYSE")
            .when(col("exchange").isin("NMS", "NGM", "NCM", "NASDAQ"), "NASDAQ")
            .otherwise(col("exchange")),
        )

        # Deduplication: Keeping only the most recent row per ticker
        window_spec = Window.partitionBy("ticker").orderBy(col("ingestion_timestamp").desc())

        metadata_df_silver = (
            metadata_df_silver.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")
        )

        # Writing data to silver delta table
        write_delta_table(metadata_df_silver, SILVER_METADATA_DIR, mode="overwrite")

        logger.success("Bronze to Silver (Metadata) pipeline completed successfully.")

    except Exception as e:
        logger.exception(f"Failed to process Silver layer metadata: {e}")
        sys.exit(1)

    finally:
        # Stopping spark session
        spark.stop()


def main() -> None:
    """CLI entrypoint for Silver metadata processing."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=str,
        default=date.today().isoformat(),
        help="Date to process (format YYYY-MM-DD)",
    )
    args, _ = parser.parse_known_args()

    run_silver_metadata(args.date)


if __name__ == "__main__":
    main()
