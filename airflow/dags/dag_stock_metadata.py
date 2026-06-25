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
    "on_failure_callback": [send_airflow_failure_email, send_airflow_failure_discord],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}


@task(task_id="task_extract_metadata")
def extract_metadata(**context: Any) -> None:
    """Extracts stock metadata from Yahoo Finance for a list of tickers and saves them to Landing Zone."""
    from airflow.sdk import Variable

    from src.producer.metadata_generator import run_metadata_generator

    try:
        tickers = Variable.get("yfinance_tickers", deserialize_json=True)
    except Exception:
        tickers = None

    exec_date = context["ds"]
    run_metadata_generator(exec_date=exec_date, tickers=tickers)


@task(task_id="task_ingest_bronze_metadata", pool="spark_write_pool")
def ingest_bronze_metadata(**context: Any) -> None:
    """Ingest stock metadata from Landing zone to Bronze using Spark."""
    from src.streaming.bronze_metadata import run_bronze_metadata

    exec_date = context["ds"]
    run_bronze_metadata(exec_date=exec_date)


@task(task_id="task_deduplicate_silver_metadata", pool="spark_write_pool")
def deduplicate_silver_metadata(**context: Any) -> None:
    """Deduplicate stock metadata in Silver Layer using Spark."""
    from src.streaming.silver_metadata import run_silver_metadata

    exec_date = context["ds"]
    run_silver_metadata(exec_date=exec_date)


@task(task_id="task_deduplicate_silver_metrics", pool="spark_write_pool")
def deduplicate_silver_metrics(**context: Any) -> None:
    """Clean and deduplicate daily financial metrics in Silver Layer using Spark."""
    from src.streaming.silver_metadata import run_silver_metrics

    exec_date = context["ds"]
    run_silver_metrics(exec_date=exec_date)


@task(task_id="task_load_gold_metadata", pool="spark_write_pool")
def load_gold_metadata(**context: Any) -> None:
    """Load deduplicated stock metadata from Silver Layer to Gold Layer using Spark."""
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
    run_gold(exec_date=exec_date, table="metadata")


@task(task_id="task_load_gold_metrics", pool="spark_write_pool")
def load_gold_metrics(**context: Any) -> None:
    """Load stock metrics from Silver Layer to Gold Layer (ClickHouse) using Spark."""
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
    run_gold(exec_date=exec_date, table="metrics")


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
    deduplicate_silver_metrics_task = deduplicate_silver_metrics()
    load_gold_metadata_task = load_gold_metadata()
    load_gold_metrics_task = load_gold_metrics()

    extract_metadata_task >> ingest_bronze_metadata_task
    ingest_bronze_metadata_task >> deduplicate_silver_metadata_task >> load_gold_metadata_task
    ingest_bronze_metadata_task >> deduplicate_silver_metrics_task >> load_gold_metrics_task
