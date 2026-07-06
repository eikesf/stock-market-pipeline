import argparse
import re
import shutil
import sys
from datetime import date

from pyspark.sql.functions import current_timestamp

from src.producer.config import ARCHIVE_METADATA_DIR, BRONZE_METADATA_DIR, LANDING_METADATA_DIR
from src.streaming.spark_session import create_spark_session
from src.streaming.utils import write_delta_table
from src.utils.logger import logger


def run_bronze_metadata(exec_date: str) -> None:
    """Ingest stock metadata from Landing Zone to Bronze Layer using Spark.

    Reads the raw stock metadata parquet file for the specified execution date,
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

    # Find file matching date dynamically in landing
    matching_files = list(LANDING_METADATA_DIR.glob(f"*metadata_{exec_date}.parquet"))
    if matching_files:
        landing_file = matching_files[0]
        in_landing = True
    else:
        landing_file = LANDING_METADATA_DIR / f"ticker_metadata_{exec_date}.parquet"
        in_landing = False

    # Check if the file has already been archived (processed by another DAG)
    if not in_landing or not landing_file.exists():
        archive_file = ARCHIVE_METADATA_DIR / f"ticker_metadata_{exec_date}.parquet"
        matching_archive_files = list(ARCHIVE_METADATA_DIR.glob(f"*metadata_{exec_date}.parquet"))
        if matching_archive_files:
            archive_file = matching_archive_files[0]

        if archive_file.exists():
            logger.info(
                f"Metadata file for {exec_date} already processed and archived at {archive_file}. "
                "Skipping ingestion to prevent duplication."
            )
            return

        logger.warning(f"Metadata file for {exec_date} not found in landing or archive. Skipping.")
        return

    spark = create_spark_session()

    # Try reading the raw metadata Parquet file
    try:
        metadata_df_raw = (
            spark.read.format("parquet").load(str(landing_file)).withColumn("ingestion_timestamp", current_timestamp())
        )
    except Exception as e:
        logger.warning(f"Failed to read landing metadata: {e}. Skipping.")
        spark.stop()
        return

    # Try writing to Bronze Delta table and archiving the raw file
    try:
        write_delta_table(metadata_df_raw, BRONZE_METADATA_DIR, mode="append")

        if landing_file.exists():
            dest_file = ARCHIVE_METADATA_DIR / landing_file.name
            if dest_file.exists():
                dest_file.unlink()
            shutil.move(str(landing_file), str(ARCHIVE_METADATA_DIR))

        logger.info(f"Successfully archived landing file to: {ARCHIVE_METADATA_DIR}")
        logger.success("Bronze (Metadata) pipeline completed successfully.")
    except Exception as e:
        logger.exception(f"Failed during Bronze metadata pipeline execution: {e}")
        spark.stop()
        raise e

    spark.stop()


def main() -> None:
    """CLI entrypoint for Bronze metadata ingestion.

    Parses the target date (or infers it if a single landing parquet exists)
    and runs the Bronze metadata pipeline.
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
        landing_files = list(LANDING_METADATA_DIR.glob("*.parquet"))
        if len(landing_files) == 1:
            filename = landing_files[0].name
            match = re.search(r"\d{4}-\d{2}-\d{2}", filename)
            if match:
                exec_date = match.group(0)

    if not exec_date:
        exec_date = date.today().isoformat()

    try:
        run_bronze_metadata(exec_date)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
