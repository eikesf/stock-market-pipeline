import argparse
import sys
from pathlib import Path

from delta.tables import DeltaTable

from src.producer.config import (
    BRONZE_METADATA_DIR,
    BRONZE_PRICES_DIR,
    SILVER_METADATA_DIR,
    SILVER_METADATA_REJECTED_DIR,
    SILVER_METRICS_DIR,
    SILVER_METRICS_REJECTED_DIR,
    SILVER_PRICES_DIR,
    SILVER_PRICES_REJECTED_DIR,
)
from src.streaming.spark_session import create_spark_session
from src.streaming.utils import check_and_heal_corrupt_data_file, is_corruption_error
from src.utils.logger import logger


def run_maintenance(retention_hours: float, raise_on_error: bool = False) -> None:
    """Run Delta Lake table maintenance (Compaction + vacuum) on all medallion tables.

    This function initializes a Spark session, disables the retention duration safety check
    to allow custom windows, and executes maintenance on Bronze and Silver Delta tables for
    prices, metadata, and metrics. For each active table, it performs:
      1. Compaction (OPTIMIZE): Merges small Parquet files to improve read query performance.
      2. Vacuuming (VACUUM): Removes unreferenced data files older than the retention threshold.

    Args:
        retention_hours: The age threshold in hours beyond which historical files
            will be permanently removed.
        raise_on_error: If True, raise errors instead of exiting.
    """
    spark = None

    try:
        spark = create_spark_session(raise_on_error=raise_on_error)
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

        paths_to_manage = [
            BRONZE_PRICES_DIR,
            BRONZE_METADATA_DIR,
            SILVER_PRICES_DIR,
            SILVER_PRICES_REJECTED_DIR,
            SILVER_METADATA_DIR,
            SILVER_METADATA_REJECTED_DIR,
            SILVER_METRICS_DIR,
            SILVER_METRICS_REJECTED_DIR,
        ]

        failures: list[str] = []

        for table_path in paths_to_manage:
            if not (Path(table_path) / "_delta_log").exists():
                logger.warning(f"Skipping maintenance: Path is not an active Delta table: {table_path}")
                continue

            logger.info(f"Running maintenance on {table_path}...")

            # Each table is isolated: OPTIMIZE reads every file it compacts, so one corrupt table
            # used to abort maintenance for every table after it in this list.
            try:
                dt = DeltaTable.forPath(spark, str(table_path))
                dt.optimize().executeCompaction()
                dt.vacuum(retention_hours)
            except Exception as table_error:
                logger.error(f"Maintenance failed for {table_path}: {table_error}")
                failures.append(str(table_path))

                if is_corruption_error(str(table_error)) and check_and_heal_corrupt_data_file(
                    [table_path], str(table_error), spark
                ):
                    logger.warning(
                        f"Corrupt data file in {table_path} healed by rollback. "
                        "Re-run maintenance to compact the restored table."
                    )

        if failures:
            raise RuntimeError(f"Delta maintenance failed for {len(failures)} table(s): {', '.join(failures)}")

    except Exception as e:
        logger.error(f"Error running Delta table maintenance: {e}")
        if raise_on_error:
            raise e
        sys.exit(1)

    finally:
        if spark is not None:
            spark.stop()


def main() -> None:
    """CLI entrypoint for running Delta table maintenance.

    Parses CLI arguments for retention hours, and runs Delta vacuum and optimize.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--retention",
        type=float,
        default=168.0,
        help="Retention hours for Delta Lake vacuum operation.",
    )
    args, _ = parser.parse_known_args()

    run_maintenance(args.retention)


if __name__ == "__main__":
    main()
