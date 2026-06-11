from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import DAG, task
from airflow.timetables.trigger import CronTriggerTimetable

default_args = {
    "owner": "eikesf",
    "depends_on_past": False,
    "retries": 1,
    "execution_timeout": timedelta(minutes=20),
}


@task(task_id="task_extract_metadata")
def extract_metadata(**context: Any) -> None:
    """Extracts stock metadata from Yahoo Finance for a list of tickers and saves them to Landing Zone."""
    from src.producer.metadata_generator import run_metadata_generator

    exec_date = context["ds"]
    run_metadata_generator(exec_date=exec_date)


@task(task_id="task_ingest_bronze_metadata")
def ingest_bronze_metadata(**context: Any) -> None:
    """Ingest stock metadata from Landing zone to Bronze using Spark."""
    from src.streaming.bronze_metadata import run_bronze_metadata

    exec_date = context["ds"]
    run_bronze_metadata(exec_date=exec_date)


@task(task_id="task_deduplicate_silver_metadata")
def deduplicate_silver_metadata(**context: Any) -> None:
    """Deduplicate stock metadata in Silver Layer using Spark."""
    from src.streaming.silver_metadata import run_silver_metadata

    exec_date = context["ds"]
    run_silver_metadata(exec_date=exec_date)


@task(task_id="task_load_gold_metadata")
def load_gold_metadata(**context: Any) -> None:
    """Load deduplicated stock metadata from Silver Layer to Gold Layer using Spark."""
    from src.streaming.gold import run_gold

    exec_date = context["ds"]
    run_gold(exec_date=exec_date)


with DAG(
    dag_id="dag_stock_metadata",
    default_args=default_args,
    description="Stock Metadata Pipeline",
    schedule=CronTriggerTimetable("@monthly", timezone="UTC"),
    start_date=datetime(2026, 6, 9),
    catchup=False,
    tags=["stock_market", "stock_metadata"],
) as dag:
    extract_metadata_task = extract_metadata()
    ingest_bronze_metadata_task = ingest_bronze_metadata()
    deduplicate_silver_metadata_task = deduplicate_silver_metadata()
    load_gold_metadata_task = load_gold_metadata()

    extract_metadata_task >> ingest_bronze_metadata_task >> deduplicate_silver_metadata_task >> load_gold_metadata_task
