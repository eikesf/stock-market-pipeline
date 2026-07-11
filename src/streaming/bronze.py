import sys

from src.producer.config import ARCHIVE_PRICES_DIR, BRONZE_PRICES_DIR, LANDING_PRICES_DIR
from src.streaming.utils import infer_execution_date, ingest_landing_to_bronze


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
    ingest_landing_to_bronze(
        exec_date=exec_date,
        landing_dir=LANDING_PRICES_DIR,
        archive_dir=ARCHIVE_PRICES_DIR,
        bronze_dir=BRONZE_PRICES_DIR,
        domain_name="Prices",
    )


def main() -> None:
    """CLI entrypoint for Bronze price ingestion.

    Parses the target date (or infers it if a single landing parquet exists)
    and runs the Bronze prices pipeline.
    """
    exec_date = infer_execution_date(LANDING_PRICES_DIR)

    try:
        run_bronze(exec_date)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
