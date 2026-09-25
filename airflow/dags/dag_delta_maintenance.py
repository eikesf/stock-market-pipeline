import os
from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.timetables.trigger import CronTriggerTimetable

from src.utils.alerts import send_airflow_failure_discord, send_airflow_failure_email

email_recipient = os.getenv("ALERT_EMAIL")

default_args = {
    "owner": "eikesf",
    "depends_on_past": False,
    "email": [email_recipient] if email_recipient else [],
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": [send_airflow_failure_email, send_airflow_failure_discord],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


@task(task_id="task_verify_delta_integrity", pool="spark_write_pool")
def verify_delta_integrity() -> None:
    """Read every data file in every medallion table to surface corruption early.

    Nothing else notices a damaged file until a query happens to touch it, which previously let a
    corrupt Silver file break three DAGs for three weeks before anyone knew. Running this before
    compaction also means OPTIMIZE is not the thing that discovers the problem.
    """
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
    from src.streaming.utils import verify_delta_table
    from src.utils.logger import logger

    tables = [
        BRONZE_PRICES_DIR,
        BRONZE_METADATA_DIR,
        SILVER_PRICES_DIR,
        SILVER_PRICES_REJECTED_DIR,
        SILVER_METADATA_DIR,
        SILVER_METADATA_REJECTED_DIR,
        SILVER_METRICS_DIR,
        SILVER_METRICS_REJECTED_DIR,
    ]

    damaged: dict[str, list[str]] = {}
    for table in tables:
        corrupt = verify_delta_table(table)
        if corrupt:
            damaged[str(table)] = corrupt
        else:
            logger.info(f"Integrity check passed for {table}.")

    if damaged:
        details = "; ".join(f"{table}: {', '.join(files)}" for table, files in damaged.items())
        raise RuntimeError(f"Corrupt Delta data files detected: {details}")

    logger.success("Delta integrity check passed for all medallion tables.")


@task(task_id="task_optimize_and_vacuum", pool="spark_write_pool")
def optimize_and_vacuum() -> None:
    """Run compaction and vacuum maintenance on all medallion tables."""
    from src.streaming.maintenance import run_maintenance

    run_maintenance(retention_hours=168.0, raise_on_error=True)


with DAG(
    dag_id="dag_delta_maintenance",
    default_args=default_args,
    description="Delta Lake integrity verification, optimization and vacuum maintenance",
    schedule=CronTriggerTimetable("@weekly", timezone="UTC"),
    start_date=datetime(2026, 6, 18),
    catchup=False,
    tags=["stock_market", "maintenance"],
) as dag:
    verify_delta_integrity() >> optimize_and_vacuum()
