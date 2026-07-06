import argparse
import sys
from datetime import date

from pyspark.sql.functions import col, row_number, trim, upper, when
from pyspark.sql.window import Window

from src.producer.config import BRONZE_PRICES_DIR, SILVER_PRICES_DIR
from src.streaming.spark_session import create_spark_session
from src.streaming.utils import read_delta_table, write_delta_table
from src.utils.logger import logger


def run_silver(exec_date: str) -> None:
    """Clean and deduplicate stock prices from Bronze to Silver Layer using Spark.

    Reads from the Bronze prices Delta table, drops records with missing critical
    fields, casts prices/volumes to their target database types (Decimals and Longs),
    deduplicates per (ticker, date) keeping the latest entry, and writes the
    cleaned dataset to the Silver prices Delta table.

    Args:
        exec_date: Execution date in YYYY-MM-DD format.

    Raises:
        SystemExit: If the date format is invalid or processing fails.
    """
    try:
        date.fromisoformat(exec_date)
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD format.")
        sys.exit(1)

    logger.info(f"Starting Silver layer processing for stock prices (execution date: {exec_date})...")

    spark = create_spark_session()
    try:
        # Reading bronze stock data
        stock_df_bronze = read_delta_table(spark, BRONZE_PRICES_DIR)

        # Clean data: drop nulls in critical columns and cast columns to appropriate data types
        stock_df_silver = (
            stock_df_bronze.na.drop(
                subset=["open", "close", "high", "low", "volume", "ticker", "date", "ingestion_timestamp", "adj_close"]
            )
            .withColumn("date", col("date").cast("date"))
            .withColumn("ticker", upper(trim(col("ticker").cast("string"))))
            .withColumn("open", col("open").cast("decimal(10,2)"))
            .withColumn("high", col("high").cast("decimal(10,2)"))
            .withColumn("low", col("low").cast("decimal(10,2)"))
            .withColumn("close", col("close").cast("decimal(10,2)"))
            .withColumn("adj_close", col("adj_close").cast("decimal(10,2)"))
            .withColumn("volume", col("volume").cast("bigint"))
            .withColumn("dividends", col("dividends").cast("decimal(10,2)"))
            .withColumn("stock_splits", col("stock_splits").cast("decimal(10,4)"))
            .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("timestamp"))
        )

        stock_df_silver = stock_df_silver.withColumn(
            "ticker", when(col("ticker") == "USDBRL=X", "USDBRL").otherwise(col("ticker"))
        )

        # Define a window to partition data by ticker and date, ordering by the most recent ingestion timestamp
        window_spec = Window.partitionBy("ticker", "date").orderBy(col("ingestion_timestamp").desc())

        # Deduplicate by keeping only the most recent record (row number 1) for each ticker/date partition
        stock_df_silver = (
            stock_df_silver.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")
        )

        # Writing data to silver delta table
        write_delta_table(stock_df_silver, SILVER_PRICES_DIR, mode="overwrite")

        logger.success("Bronze to Silver pipeline completed successfully.")

    except Exception as e:
        logger.exception(f"Failed to process Silver layer: {e}")
        sys.exit(1)

    finally:
        spark.stop()


def main() -> None:
    """CLI entrypoint for Silver price processing.

    Parses CLI arguments for the target execution date, and runs the Silver prices pipeline.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=str,
        default=date.today().isoformat(),
        help="Date to process (format YYYY-MM-DD)",
    )
    args, _ = parser.parse_known_args()

    run_silver(args.date)


if __name__ == "__main__":
    main()
