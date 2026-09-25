import argparse
import sys
from datetime import date

from pyspark.sql.functions import col, row_number, trim, upper, when
from pyspark.sql.window import Window

from src.producer.config import (
    ARCHIVE_PRICES_DIR,
    BRONZE_PRICES_DIR,
    LANDING_PRICES_DIR,
    SILVER_PRICES_DIR,
    SILVER_PRICES_REJECTED_DIR,
)
from src.streaming.quality_rules import (
    PRICES_REQUIRED_COLUMNS,
    classify,
    prices_rules,
    require_columns,
    split_valid_rejected,
)
from src.streaming.spark_session import create_spark_session
from src.streaming.utils import (
    check_and_heal_corrupt_data_file,
    read_delta_table,
    recover_bronze_from_archive,
    write_delta_table,
)
from src.utils.logger import logger


def run_silver(exec_date: str, raise_on_error: bool = False) -> None:
    """Clean, validate and deduplicate stock prices from Bronze to Silver Layer using Spark.

    Reads from the Bronze prices Delta table, casts prices/volumes to their target database
    types (Decimals and Longs), deduplicates per (ticker, date) keeping the latest entry, then
    splits the batch: rows breaching a quality rule are written whole to the Silver prices
    quarantine table and kept out of the Silver prices table entirely.

    Args:
        exec_date: Execution date in YYYY-MM-DD format.
        raise_on_error: If True, raise errors instead of exiting.

    Raises:
        SystemExit: If the date format is invalid or processing fails.
        RequiredColumnMissingError: If any row is missing ticker or date.
    """
    try:
        date.fromisoformat(exec_date)
    except ValueError as e:
        logger.error("Invalid date format. Please use YYYY-MM-DD format.")
        if raise_on_error:
            raise e
        sys.exit(1)

    logger.info(f"Starting Silver layer processing for stock prices (execution date: {exec_date})...")

    spark = create_spark_session(raise_on_error=raise_on_error)
    try:
        # Reading bronze stock data
        stock_df_bronze = read_delta_table(spark, BRONZE_PRICES_DIR)

        # Cast the identity columns first so the essential-column check can run before anything else
        stock_df_silver = stock_df_bronze.withColumn("date", col("date").cast("date")).withColumn(
            "ticker", upper(trim(col("ticker").cast("string")))
        )

        require_columns(stock_df_silver, PRICES_REQUIRED_COLUMNS, domain="prices")

        # Cast remaining columns to their target database types
        stock_df_silver = (
            stock_df_silver.withColumn("open", col("open").cast("decimal(10,2)"))
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

        # Drop non-trading days before validating. yfinance emits a row per calendar date per
        # ticker, with every OHLC value null when no session took place (market holiday, or a date
        # before the ticker listed). That is the expected shape of the source, not a quality
        # problem, so quarantining it would bury real defects under tens of thousands of rows.
        no_session = col("open").isNull() & col("high").isNull() & col("low").isNull() & col("close").isNull()
        before_count = stock_df_silver.count()
        stock_df_silver = stock_df_silver.filter(~no_session)
        skipped = before_count - stock_df_silver.count()
        if skipped:
            logger.info(f"Skipped {skipped} non-trading-day rows with no OHLC data.")

        # Split clean rows from rows breaching a quality rule; only clean rows reach Silver (and Gold)
        classified_df = classify(stock_df_silver, prices_rules())
        stock_df_silver, rejected_df = split_valid_rejected(classified_df, pipeline_exec_date=exec_date)

        # Writing data to silver delta tables (prices is a full refresh on every run)
        write_delta_table(stock_df_silver, SILVER_PRICES_DIR, mode="overwrite")
        write_delta_table(rejected_df, SILVER_PRICES_REJECTED_DIR, mode="overwrite")

        logger.success("Bronze to Silver pipeline completed successfully.")

    except Exception as e:
        logger.exception(f"Failed to process Silver layer: {e}")
        healed = check_and_heal_corrupt_data_file(
            [BRONZE_PRICES_DIR, SILVER_PRICES_DIR, SILVER_PRICES_REJECTED_DIR], str(e), spark
        )
        if healed:
            logger.warning("Corrupted data file detected and Delta table self-healed. Reverted to previous version.")
            if healed == BRONZE_PRICES_DIR:
                # Bronze is the only copy of the ingested rows, so a rollback there has to be
                # followed by replaying the archived landing files the rollback discarded.
                recover_bronze_from_archive(
                    paths={
                        "landing": LANDING_PRICES_DIR,
                        "archive": ARCHIVE_PRICES_DIR,
                        "bronze": BRONZE_PRICES_DIR,
                    },
                    domain_name="Prices",
                    watermark_column="date",
                    spark=spark,
                )
            if raise_on_error:
                raise RuntimeError(
                    "Corrupted data file detected and Delta table self-healed. Please retry the task."
                ) from e
        if raise_on_error:
            raise e
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
