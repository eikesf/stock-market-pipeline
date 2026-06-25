import os
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import DAG, task
from airflow.timetables.trigger import CronTriggerTimetable

from src.utils.alerts import send_airflow_failure_discord, send_airflow_failure_email

email_recipient = os.getenv("AIRFLOW__SMTP__SMTP_USER")

default_args = {
    "owner": "eikesf",
    "depends_on_past": False,
    "email": [email_recipient] if email_recipient else [],
    "email_on_failure": False,
    "on_failure_callback": [send_airflow_failure_discord, send_airflow_failure_email],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}


@task(task_id="task_extract_prices")
def extract_prices(**context: Any) -> None:
    """Extracts stock prices from Yahoo Finance for a list of tickers and saves them to Landing layer."""
    from airflow.sdk import Variable

    from src.producer.generator import run_generator

    try:
        tickers = Variable.get("yfinance_tickers", deserialize_json=True)
    except Exception:
        tickers = None

    exec_date = context["ds"]
    run_generator(exec_date=exec_date, tickers=tickers)


@task(task_id="task_ingest_bronze_prices", pool="spark_write_pool")
def ingest_bronze_prices(**context: Any) -> None:
    """Ingests stock prices from Landing Zone to Bronze Layer using Spark."""
    from src.streaming.bronze import run_bronze

    exec_date = context["ds"]
    run_bronze(exec_date=exec_date)


@task(task_id="task_deduplicate_silver_prices", pool="spark_write_pool")
def deduplicate_silver_prices(**context: Any) -> None:
    """Deduplicates stock prices in Silver Layer."""
    from src.streaming.silver import run_silver

    exec_date = context["ds"]
    run_silver(exec_date=exec_date)


@task(task_id="task_load_gold_prices", pool="spark_write_pool")
def load_gold_prices(**context: Any) -> None:
    """Loads deduplicated stock prices from Silver Layer to Gold Layer using staging tables."""
    from airflow.sdk import BaseHook

    from src.streaming.gold import run_gold

    try:
        conn = BaseHook.get_connection("clickhouse_default")
        os.environ["CLICKHOUSE_HOST"] = conn.host or "clickhouse"
        os.environ["CLICKHOUSE_PORT"] = str(conn.port or 8123)
        os.environ["CLICKHOUSE_USER"] = conn.login or "default"
        os.environ["CLICKHOUSE_PASSWORD"] = conn.password or ""
        os.environ["CLICKHOUSE_DB"] = conn.schema or "stock_market"
    except Exception as e:
        import logging

        logging.getLogger("airflow.dag").warning("Failed to get clickhouse connection from Airflow: %s", e)

    exec_date = context["ds"]
    run_gold(exec_date=exec_date, table="prices")


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
