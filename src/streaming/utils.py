import argparse
import contextlib
import json
import os
import re
import shutil
import sys
from datetime import date
from pathlib import Path

import clickhouse_connect
import pyarrow.parquet as pq
from clickhouse_connect.driver.client import Client
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp

from src.streaming.spark_session import create_spark_session
from src.utils.logger import logger


def _is_checkpoint_corrupted(checkpoint_file: Path) -> bool:
    """Check if the checkpoint parquet file is corrupted."""
    try:
        pq_file = pq.ParquetFile(str(checkpoint_file))
        if pq_file.num_row_groups > 0:
            pq_file.read_row_group(0)
        return False
    except Exception as e:
        logger.warning(f"Detected corrupted Delta checkpoint file {checkpoint_file.name}: {e}")
        return True


def _rollback_to_previous_checkpoint(log_dir: Path, last_checkpoint_file: Path) -> bool:
    """Find and rollback to the previous valid checkpoint.

    Returns True if successfully rolled back, False otherwise.
    """
    checkpoint_files = sorted(log_dir.glob("*.checkpoint.parquet"))
    if not checkpoint_files:
        return False

    latest_remaining = checkpoint_files[-1]
    if _is_checkpoint_corrupted(latest_remaining):
        return False

    try:
        prev_version = int(latest_remaining.name.split(".")[0])
        new_checkpoint_data = {
            "version": prev_version,
            "size": 12,
            "sizeInBytes": latest_remaining.stat().st_size,
            "numOfAddFiles": 1,
        }
        with open(last_checkpoint_file, "w") as f:
            json.dump(new_checkpoint_data, f)
        logger.info(f"Updated _last_checkpoint to point to previous valid version {prev_version}")
        return True
    except Exception as e:
        logger.warning(f"Failed to rollback to previous checkpoint {latest_remaining.name}: {e}")
        return False


def heal_corrupt_delta_checkpoints(path: str | Path) -> None:
    """Checks the Delta table's latest checkpoint file and heals it if corrupted.

    If the parquet checkpoint file specified in _last_checkpoint is unreadable
    or corrupted, it deletes the file and updates _last_checkpoint to point to
    the previous checkpoint version (or deletes _last_checkpoint if no previous
    version exists). This forces Spark to fallback and self-heal automatically.

    Args:
        path: Path to the Delta table directory.
    """
    log_dir = Path(path) / "_delta_log"
    if not log_dir.exists():
        return

    last_checkpoint_file = log_dir / "_last_checkpoint"
    if not last_checkpoint_file.exists():
        return

    try:
        with open(last_checkpoint_file) as f:
            checkpoint_data = json.load(f)
        version = checkpoint_data.get("version")
    except Exception as e:
        logger.warning(f"Could not read _last_checkpoint for {path}: {e}. Deleting it to force fallback.")
        with contextlib.suppress(Exception):
            last_checkpoint_file.unlink(missing_ok=True)
        return

    if version is None:
        return

    checkpoint_file = log_dir / f"{version:020d}.checkpoint.parquet"
    if not checkpoint_file.exists():
        return

    if _is_checkpoint_corrupted(checkpoint_file):
        logger.info(f"Healing Delta table at {path}...")
        with contextlib.suppress(Exception):
            checkpoint_file.unlink(missing_ok=True)
            logger.info(f"Deleted corrupted checkpoint file: {checkpoint_file.name}")

        if _rollback_to_previous_checkpoint(log_dir, last_checkpoint_file):
            return

        # Fallback: Delete _last_checkpoint entirely to force replay from JSON log files
        try:
            last_checkpoint_file.unlink(missing_ok=True)
            logger.info("Deleted _last_checkpoint to force full JSON log replay fallback.")
        except Exception as e:
            logger.error(f"Failed to delete _last_checkpoint file: {e}")


def read_delta_table(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read a Delta table from the given path.

    Args:
        spark: The active Spark session.
        path: Path to the Delta table directory.

    Returns:
        A Spark DataFrame loaded from the Delta path.
    """
    heal_corrupt_delta_checkpoints(path)
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
    heal_corrupt_delta_checkpoints(path)
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


def ingest_landing_to_bronze(  # noqa: PLR0913
    exec_date: str,
    landing_dir: Path,
    archive_dir: Path,
    bronze_dir: Path,
    domain_name: str,
    raise_on_error: bool = False,
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
        raise_on_error: If True, raise validation and execution errors.
    """
    filename = resolve_bronze_filename(exec_date, domain_name)
    landing_file = landing_dir / filename

    try:
        date.fromisoformat(exec_date)
    except ValueError as e:
        logger.error("Invalid date format. Please use YYYY-MM-DD format.")
        if raise_on_error:
            raise e
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


def extract_corrupt_parquet_filename(error_message: str) -> str | None:
    """Extract standard Spark parquet filenames from an error message using regex."""
    match = re.search(r"(part-\d+-[a-fA-F0-9\-]+\S*\.parquet)", error_message)
    return match.group(1) if match else None


def find_version_introducing_file(table_path: Path, filename: str) -> int | None:
    """Scan Delta Table logs descending to find the version that introduced a file."""
    log_dir = table_path / "_delta_log"
    if not log_dir.exists():
        return None

    json_files = sorted(log_dir.glob("*.json"), reverse=True)
    for json_file in json_files:
        try:
            version = int(json_file.name.split(".")[0])
            with open(json_file, encoding="utf-8") as f:
                for line in f:
                    if filename in line:
                        data = json.loads(line)
                        if "add" in data and data["add"]["path"].endswith(filename):
                            return version
        except Exception as e:
            logger.debug(f"Failed to read commit JSON file {json_file.name}: {e}")
            continue
    return None


def check_and_heal_corrupt_data_file(table_paths: list[str | Path], error_message: str, spark: SparkSession) -> bool:
    """Detect corrupted parquet files in a list of tables and rollback Delta table versions.

    Args:
        table_paths: A list of Delta table paths to check.
        error_message: Exception message to parse for corrupt filename.
        spark: The active Spark session.

    Returns:
        True if a corrupt file was found and table successfully healed/rolled back.
    """
    corrupt_filename = extract_corrupt_parquet_filename(error_message)
    if not corrupt_filename:
        return False

    for path_str in table_paths:
        path = Path(path_str)
        corrupt_file_path = path / corrupt_filename
        if not corrupt_file_path.exists():
            matching_files = list(path.glob(f"**/{corrupt_filename}"))
            if matching_files:
                corrupt_file_path = matching_files[0]
            else:
                continue

        logger.warning(f"Detected corrupted data file {corrupt_filename} in Delta table {path}")

        version = find_version_introducing_file(path, corrupt_filename)
        if version is None:
            logger.warning(f"Could not locate Delta version introducing corrupted file {corrupt_filename}")
            continue

        prev_version = version - 1
        if prev_version < 0:
            logger.error(f"Cannot rollback to version before 0 for table {path}")
            continue

        logger.info(f"Automatically rolling back table {path} to healthy version {prev_version}...")
        try:
            dt = DeltaTable.forPath(spark, str(path))
            dt.restoreToVersion(prev_version)
            logger.success(f"Delta table {path} successfully restored to healthy version {prev_version}")

            try:
                corrupt_file_path.unlink(missing_ok=True)
                crc_file = corrupt_file_path.parent / f".{corrupt_file_path.name}.crc"
                crc_file.unlink(missing_ok=True)
                logger.info(f"Deleted physical corrupted file: {corrupt_file_path.name}")
            except Exception as fe:
                logger.warning(f"Failed to delete physical file {corrupt_file_path}: {fe}")

            return True
        except Exception as re:
            logger.error(f"Failed to execute Delta table restore on {path} to version {prev_version}: {re}")

    return False
