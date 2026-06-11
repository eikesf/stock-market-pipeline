from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import DAG, task
from airflow.timetables.trigger import CronTriggerTimetable

default_args = {
    "owner": "eikesf",
    "depends_on_past": False,
    "retries": 1,
    "execution_timeout": timedelta(minutes=10),
}


@task(task_id="task_extract_prices")
def extract_prices(**context: Any) -> None:
    """Extracts stock prices from Yahoo Finance for a list of tickers and saves them to Landing layer."""
    from src.producer.generator import run_generator

    exec_date = context["ds"]
    run_generator(exec_date=exec_date)


@task(task_id="task_ingest_bronze_prices")
def ingest_bronze_prices(**context: Any) -> None:
    """Ingests stock prices from Landing Zone to Bronze Layer using Spark."""
    from src.streaming.bronze import run_bronze

    exec_date = context["ds"]
    run_bronze(exec_date=exec_date)


@task(task_id="task_deduplicate_silver_prices")
def deduplicate_silver_prices(**context: Any) -> None:
    """Deduplicates stock prices in Silver Layer."""
    from src.streaming.silver import run_silver

    exec_date = context["ds"]
    run_silver(exec_date=exec_date)


@task(task_id="task_load_gold_prices")
def load_gold_prices(**context: Any) -> None:
    """Loads deduplicated stock prices from Silver Layer to Gold Layer using staging tables."""
    from src.streaming.gold import run_gold

    exec_date = context["ds"]
    run_gold(exec_date=exec_date)


with DAG(
    dag_id="dag_stock_prices",
    default_args=default_args,
    description="Stock Price Pipeline",
    schedule=CronTriggerTimetable("0 22 * * 1-5", timezone="UTC"),  # Execute every weekday at 22:00 (UTC)
    start_date=datetime(2026, 6, 9),
    catchup=False,
    tags=["stock_market", "stock_prices"],
) as dag:
    extract_prices_task = extract_prices()
    ingest_bronze_prices_task = ingest_bronze_prices()
    deduplicate_silver_prices_task = deduplicate_silver_prices()
    load_gold_prices_task = load_gold_prices()

    extract_prices_task >> ingest_bronze_prices_task >> deduplicate_silver_prices_task >> load_gold_prices_task
