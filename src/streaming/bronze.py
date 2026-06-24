import argparse
import re
import shutil
import sys
from datetime import date

from pyspark.sql.functions import current_timestamp

from src.producer.config import ARCHIVE_PRICES_DIR, BRONZE_PRICES_DIR, LANDING_PRICES_DIR
from src.streaming.spark_session import create_spark_session
from src.streaming.utils import write_delta_table
from src.utils.logger import logger


def run_bronze(exec_date: str) -> None:
    """Ingest stock prices from Landing Zone to Bronze Layer using Spark.

    Reads the raw stock prices parquet file for the specified execution date,
    enriches it with ingestion timestamps, appends it to the Bronze Delta table,
    and archives the raw landing file.

    Args:
        exec_date: Execution date in YYYY-MM-DD format.

    Raises:
        SystemExit: If the date format is invalid, reading/writing fails, or
            archiving fails.
    """
    try:
        date.fromisoformat(exec_date)
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD format.")
        sys.exit(1)

    spark = create_spark_session()
    try:
        landing_file = LANDING_PRICES_DIR / f"tickers_{exec_date}.parquet"
        try:
            stock_df_raw = (
                spark.read.format("parquet")
                .load(str(landing_file))
                .withColumn("ingestion_timestamp", current_timestamp())
            )
        except Exception as e:
            logger.warning(f"Failed to read landing stock price data: {e}. Exiting cleanly as folder might be empty.")
            sys.exit(0)

        try:
            write_delta_table(stock_df_raw, BRONZE_PRICES_DIR, mode="append")

            if landing_file.exists():
                dest_file = ARCHIVE_PRICES_DIR / landing_file.name
                if dest_file.exists():
                    dest_file.unlink()
                shutil.move(str(landing_file), str(ARCHIVE_PRICES_DIR))

            logger.info(f"Successfully archived landing file to: {ARCHIVE_PRICES_DIR}")
            logger.success("Bronze (Prices) pipeline completed successfully.")

        except Exception as e:
            logger.exception(f"Failed during Bronze prices pipeline execution: {e}")
            sys.exit(1)
    finally:
        spark.stop()


def main() -> None:
    """CLI entrypoint for Bronze price ingestion.

    Parses the target date (or infers it if a single landing parquet exists)
    and runs the Bronze prices pipeline.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to process (format: YYYY-MM-DD)",
    )
    args, _ = parser.parse_known_args()

    exec_date = args.date

    if not exec_date:
        landing_files = list(LANDING_PRICES_DIR.glob("*.parquet"))
        if len(landing_files) == 1:
            filename = landing_files[0].name
            match = re.search(r"\d{4}-\d{2}-\d{2}", filename)
            if match:
                exec_date = match.group(0)

    if not exec_date:
        exec_date = date.today().isoformat()

    run_bronze(exec_date)


if __name__ == "__main__":
    main()
