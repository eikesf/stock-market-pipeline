import sys

from src.producer.config import ARCHIVE_METADATA_DIR, BRONZE_METADATA_DIR, LANDING_METADATA_DIR
from src.streaming.utils import infer_execution_date, ingest_landing_to_bronze


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
    ingest_landing_to_bronze(
        exec_date=exec_date,
        landing_dir=LANDING_METADATA_DIR,
        archive_dir=ARCHIVE_METADATA_DIR,
        bronze_dir=BRONZE_METADATA_DIR,
        domain_name="Metadata",
    )


def main() -> None:
    """CLI entrypoint for Bronze metadata ingestion.

    Parses the target date (or infers it if a single landing parquet exists)
    and runs the Bronze metadata pipeline.
    """
    exec_date = infer_execution_date(LANDING_METADATA_DIR)

    try:
        run_bronze_metadata(exec_date)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
