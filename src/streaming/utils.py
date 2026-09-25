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


def ingest_landing_to_bronze(
    exec_date: str,
    paths: dict[str, Path],
    domain_name: str,
    raise_on_error: bool = False,
) -> None:
    """Ingest files from Landing Zone to Bronze Layer using Spark.

    Reads the raw Parquet file matching the pattern, enriches it with
    ingestion timestamps, appends it to the Bronze Delta table, and archives
    the raw landing file.

    Args:
        exec_date: Target execution date in YYYY-MM-DD format.
        paths: A dictionary containing 'landing', 'archive', and 'bronze' Paths.
        domain_name: Name of the domain being loaded (e.g., 'Prices', 'Metadata').
        raise_on_error: If True, raise validation and execution errors.
    """
    landing_dir = paths["landing"]
    archive_dir = paths["archive"]
    bronze_dir = paths["bronze"]
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


def is_corruption_error(error_message: str) -> bool:
    """Check whether an exception message carries a parquet corruption signature.

    Used to decide whether a failure is worth attempting recovery on, rather than running the
    recovery path for every unrelated error.

    Args:
        error_message: Exception message to inspect.

    Returns:
        True if the message looks like a corrupt or unreadable parquet file.
    """
    signatures = (
        "FAILED_READ_FILE",
        "uncompressed_page_size",
        "can not read class org.apache.parquet",
        "is not a Parquet file",
        "ChecksumException",
        "TASK_WRITE_FAILED",
    )
    return any(signature in error_message for signature in signatures) or bool(
        extract_corrupt_parquet_filename(error_message)
    )


def verify_delta_table(path: str | Path) -> list[str]:
    """Read every data file in a Delta table to find physically corrupt parquet files.

    Proactive counterpart to the reactive healing below: nothing else in the pipeline notices a
    damaged file until a query happens to touch it, which previously let corruption sit unreported
    for weeks. Reads each row group because a file's footer can be intact while its data pages are
    not, which is exactly how the observed corruption presented.

    Args:
        path: Path to the Delta table directory.

    Returns:
        Names of the corrupt data files, empty when the table is healthy.
    """
    table_path = Path(path)
    if not (table_path / "_delta_log").exists():
        return []

    corrupt: list[str] = []
    for data_file in sorted(table_path.rglob("*.parquet")):
        if "_delta_log" in data_file.parts:
            continue
        try:
            parquet_file = pq.ParquetFile(str(data_file))
            for row_group in range(parquet_file.num_row_groups):
                parquet_file.read_row_group(row_group)
        except Exception as e:
            logger.warning(f"Corrupt data file detected in {table_path}: {data_file.name} ({e})")
            corrupt.append(data_file.name)
    return corrupt


def replay_archive_to_bronze(
    exec_date: str, paths: dict[str, Path], domain_name: str, spark: SparkSession | None = None
) -> bool:
    """Re-ingest an already-archived landing file into Bronze.

    `ingest_landing_to_bronze` deliberately refuses to re-process an archived file, which is
    correct for scheduled runs but blocks recovery. This is the explicit escape hatch for it: it
    reads straight from the archive and does not move anything, leaving the archive intact as the
    replay log. Duplicate Bronze rows are harmless because Silver deduplicates on
    (ticker, date) keeping the latest ingestion_timestamp.

    Args:
        exec_date: Execution date of the archived file, in YYYY-MM-DD format.
        paths: Dictionary containing at least 'archive' and 'bronze' Paths.
        domain_name: Domain being replayed (e.g. 'Prices', 'Metadata').
        spark: An already-active Spark session to reuse. Only a session created here is stopped
            here — a caller recovering mid-failure still needs its own session afterwards.

    Returns:
        True if the file was replayed, False if it was not found.
    """
    archive_file = paths["archive"] / resolve_bronze_filename(exec_date, domain_name)
    if not archive_file.exists():
        logger.warning(f"Cannot replay {domain_name} for {exec_date}: {archive_file} not found.")
        return False

    session = spark or create_spark_session()
    try:
        df_raw = (
            session.read.format("parquet")
            .load(str(archive_file))
            .withColumn("ingestion_timestamp", current_timestamp())
        )
        write_delta_table(df_raw, paths["bronze"], mode="append")
        logger.success(f"Replayed archived {domain_name} file for {exec_date} into Bronze.")
        return True
    finally:
        if spark is None:
            session.stop()


def find_archive_dates(archive_dir: Path, domain_name: str) -> list[str]:
    """List the execution dates available in an archive directory, oldest first.

    Args:
        archive_dir: Archive directory holding the processed landing files.
        domain_name: Domain being inspected (e.g. 'Prices', 'Metadata').

    Returns:
        Sorted execution dates parsed from the archived filenames.
    """
    prefix = resolve_bronze_filename("", domain_name).replace(".parquet", "")
    dates = []
    for archived in archive_dir.glob(f"{prefix}*.parquet"):
        candidate = archived.stem.removeprefix(prefix)
        try:
            date.fromisoformat(candidate)
        except ValueError:
            continue
        dates.append(candidate)
    return sorted(dates)


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


def check_and_heal_corrupt_data_file(
    table_paths: list[str | Path], error_message: str, spark: SparkSession
) -> Path | None:
    """Detect corrupted parquet files in a list of tables and rollback Delta table versions.

    Args:
        table_paths: A list of Delta table paths to check.
        error_message: Exception message to parse for corrupt filename.
        spark: The active Spark session.

    Returns:
        The path of the table that was healed, or None if nothing was healed. Callers need the
        identity of the table and not just a boolean, because rolling back Bronze loses rows that
        no upstream table can recompute and has to be followed by an archive replay.
    """
    corrupt_filename = extract_corrupt_parquet_filename(error_message)
    if not corrupt_filename:
        return None

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

            return path
        except Exception as re:
            logger.error(f"Failed to execute Delta table restore on {path} to version {prev_version}: {re}")

    return None


def recover_bronze_from_archive(
    paths: dict[str, Path],
    domain_name: str,
    watermark_column: str,
    spark: SparkSession,
) -> int:
    """Replay archived landing files that a Bronze rollback discarded.

    Rolling a Bronze table back to the version before a corrupt file also discards every good
    commit made after it, and unlike Silver those rows cannot be recomputed from an upstream
    table. They can, however, be replayed from the archive, which keeps one file per execution
    date. Rather than replaying the whole archive, this reads the highest value still present in
    the restored table and replays only the files past it.

    The watermark does not need to be exact. Bronze is an append log and Silver deduplicates on
    (ticker, date) keeping the latest ingestion_timestamp, so replaying an overlapping file is
    harmless, whereas skipping one would leave a permanent hole.

    Args:
        paths: Dictionary containing 'archive' and 'bronze' Paths.
        domain_name: Domain being recovered (e.g. 'Prices', 'Metadata').
        watermark_column: Date column used to decide what is missing ('date' for prices,
            'extraction_date' for metadata).
        spark: The active Spark session.

    Returns:
        The number of archived files replayed.
    """
    archive_dates = find_archive_dates(paths["archive"], domain_name)
    if not archive_dates:
        logger.warning(f"No archived {domain_name} files available to replay.")
        return 0

    try:
        restored = read_delta_table(spark, paths["bronze"])
        watermark_row = restored.agg({watermark_column: "max"}).collect()[0][0]
    except Exception as e:
        logger.warning(f"Could not read the {domain_name} watermark after rollback: {e}. Replaying the full archive.")
        watermark_row = None

    if watermark_row is None:
        missing = archive_dates
    else:
        watermark = str(watermark_row)
        # Inclusive of the watermark itself: the commit holding it may have been partially
        # discarded, and a duplicate replay is cheaper than a missing day.
        missing = [d for d in archive_dates if d >= watermark]
        logger.info(
            f"{domain_name} watermark after rollback is {watermark}; replaying {len(missing)} archived file(s)."
        )

    replayed = 0
    for exec_date in missing:
        # A failed replay must not mask the corruption that triggered the recovery, nor stop the
        # remaining dates from being replayed: it is logged and the caller still fails loudly.
        try:
            if replay_archive_to_bronze(exec_date, paths, domain_name, spark):
                replayed += 1
        except Exception as e:
            logger.error(f"Failed to replay archived {domain_name} file for {exec_date}: {e}")

    logger.success(f"Replayed {replayed} archived {domain_name} file(s) into Bronze after rollback.")
    return replayed
