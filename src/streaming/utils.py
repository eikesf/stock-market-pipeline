import argparse
import os
import re
import shutil
import sys
from datetime import date
from pathlib import Path

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp

from src.streaming.spark_session import create_spark_session
from src.utils.logger import logger


def read_delta_table(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read a Delta table from the given path.

    Args:
        spark: The active Spark session.
        path: Path to the Delta table directory.

    Returns:
        A Spark DataFrame loaded from the Delta path.
    """
    return spark.read.format("delta").load(str(path))


def write_delta_table(df: DataFrame, path: str | Path, mode: str = "append") -> None:
    """Write a DataFrame to a Delta table with the given mode.

    Automatically enables overwriteSchema for overwrite mode, and mergeSchema
    for append mode.

    Args:
        df: Spark DataFrame to write.
        path: Target path for the Delta table.
        mode: Spark write mode (e.g., 'append', 'overwrite').
    """
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    elif mode == "append":
        writer = writer.option("mergeSchema", "true")
    writer.save(str(path))
    logger.success(f"Data successfully written to {path} (Mode: {mode})")


def get_clickhouse_client() -> Client:
    """Responsible for connecting with the ClickHouse database.

    Uses environment variables for host, port, user, password, and database.

    Returns:
        A ClickHouse driver Client instance.
    """
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=os.environ.get("CLICKHOUSE_PORT", "8123"),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "stock_market"),
    )


def infer_execution_date(landing_dir: Path) -> str:
    """Parse CLI arguments for a target date or infer it from the landing zone files.

    If a date is not specified via --date, it attempts to find the date from
    a single parquet file in the landing directory. If none or multiple files exist,
    falls back to today's date.

    Args:
        landing_dir: Path to the Landing directory containing raw parquet files.

    Returns:
        Execution date string in YYYY-MM-DD format.
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
        landing_files = list(landing_dir.glob("*.parquet"))
        if len(landing_files) == 1:
            filename = landing_files[0].name
            match = re.search(r"\d{4}-\d{2}-\d{2}", filename)
            if match:
                exec_date = match.group(0)

    if not exec_date:
        exec_date = date.today().isoformat()

    return exec_date


def resolve_bronze_filename(exec_date: str, domain_name: str) -> str:
    """Resolve the exact file name based on domain."""
    if domain_name.lower() == "prices":
        return f"tickers_{exec_date}.parquet"
    return f"ticker_metadata_{exec_date}.parquet"


def ingest_landing_to_bronze(
    exec_date: str,
    landing_dir: Path,
    archive_dir: Path,
    bronze_dir: Path,
    domain_name: str,
) -> None:
    """Ingest files from Landing Zone to Bronze Layer using Spark.

    Reads the raw Parquet file matching the pattern, enriches it with
    ingestion timestamps, appends it to the Bronze Delta table, and archives
    the raw landing file.

    Args:
        exec_date: Target execution date in YYYY-MM-DD format.
        landing_dir: Path to the Landing zone directory.
        archive_dir: Path to the Archive directory.
        bronze_dir: Path to the Bronze layer Delta table.
        domain_name: Name of the domain being loaded (e.g., 'Prices', 'Metadata').
    """
    filename = resolve_bronze_filename(exec_date, domain_name)
    landing_file = landing_dir / filename

    try:
        date.fromisoformat(exec_date)
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD format.")
        sys.exit(1)

    # Check if the file has already been archived or processed
    if not landing_file.exists():
        archive_file = archive_dir / filename
        if archive_file.exists():
            logger.info(
                f"{domain_name} file for {exec_date} already processed and archived at {archive_file}. "
                "Skipping ingestion to prevent duplication."
            )
            return

        logger.warning(f"{domain_name} file for {exec_date} not found in landing or archive. Skipping.")
        return

    spark = create_spark_session()

    # Try reading the raw Parquet file
    try:
        df_raw = (
            spark.read.format("parquet").load(str(landing_file)).withColumn("ingestion_timestamp", current_timestamp())
        )
    except Exception as e:
        logger.warning(f"Failed to read landing {domain_name.lower()} data: {e}. Skipping.")
        spark.stop()
        return

    # Try writing to Bronze Delta table and archiving the raw file
    try:
        write_delta_table(df_raw, bronze_dir, mode="append")

        if landing_file.exists():
            dest_file = archive_dir / filename
            if dest_file.exists():
                dest_file.unlink()
            shutil.move(str(landing_file), str(archive_dir))

        logger.info(f"Successfully archived landing file to: {archive_dir}")
        logger.success(f"Bronze ({domain_name}) pipeline completed successfully.")
    except Exception as e:
        logger.exception(f"Failed during Bronze {domain_name.lower()} pipeline execution: {e}")
        spark.stop()
        raise e

    spark.stop()
