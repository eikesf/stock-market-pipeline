import argparse
import sys
from pathlib import Path

from delta.tables import DeltaTable

from src.producer.config import (
    BRONZE_METADATA_DIR,
    BRONZE_PRICES_DIR,
    SILVER_METADATA_DIR,
    SILVER_METRICS_DIR,
    SILVER_PRICES_DIR,
)
from src.streaming.spark_session import create_spark_session
from src.utils.logger import logger


def run_maintenance(retention_hours: float) -> None:
    """Run Delta Lake table maintenance (Compaction + vacuum) on all medallion tables.

    This function initializes a Spark session, disables the retention duration safety check
    to allow custom windows, and executes maintenance on Bronze and Silver Delta tables for
    prices, metadata, and metrics. For each active table, it performs:
      1. Compaction (OPTIMIZE): Merges small Parquet files to improve read query performance.
      2. Vacuuming (VACUUM): Removes unreferenced data files older than the retention threshold.

    Args:
        retention_hours: The age threshold in hours beyond which historical files
            will be permanently removed.
    """
    spark = None

    try:
        spark = create_spark_session()
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

        paths_to_manage = [
            BRONZE_PRICES_DIR,
            BRONZE_METADATA_DIR,
            SILVER_PRICES_DIR,
            SILVER_METADATA_DIR,
            SILVER_METRICS_DIR,
        ]

        for table_path in paths_to_manage:
            if not (Path(table_path) / "_delta_log").exists():
                logger.warning(f"Skipping maintenance: Path is not an active Delta table: {table_path}")
                continue

            logger.info(f"Running maintenance on {table_path}...")

            dt = DeltaTable.forPath(spark, str(table_path))
            dt.optimize().executeCompaction()
            dt.vacuum(retention_hours)

    except Exception as e:
        logger.error(f"Error running Delta table maintenance: {e}")
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
