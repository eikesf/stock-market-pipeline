import os
from datetime import datetime, timedelta
from typing import Any

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
    run_metadata_generator(exec_date=exec_date, tickers=tickers, raise_on_error=True)


@task(task_id="task_ingest_bronze_metadata", pool="spark_write_pool")
def ingest_bronze_metadata(**context: Any) -> None:
    """Ingest stock metadata from Landing zone to Bronze using Spark."""
    from src.streaming.bronze_metadata import run_bronze_metadata

    exec_date = context["ds"]
    run_bronze_metadata(exec_date=exec_date, raise_on_error=True)


@task(task_id="task_deduplicate_silver_metadata", pool="spark_write_pool")
def deduplicate_silver_metadata(**context: Any) -> None:
    """Deduplicate stock metadata in Silver Layer using Spark."""
    from src.streaming.silver_metadata import run_silver_metadata

    exec_date = context["ds"]
    run_silver_metadata(exec_date=exec_date, raise_on_error=True)


@task(task_id="task_validate_silver_metadata", pool="spark_write_pool")
def validate_silver_metadata() -> None:
    """Validate Silver metadata Delta table using Soda Core."""
    from src.quality.soda_validator import run_silver_scan

    run_silver_scan(table_name="silver_metadata", contract_path="soda/contracts/silver_metadata_contract.yml")


@task(task_id="task_load_gold_metadata", pool="spark_write_pool")
def load_gold_metadata(**context: Any) -> None:
    """Load deduplicated stock metadata from Silver Layer to Gold Layer using Spark."""
    from src.streaming.gold import run_gold
    from src.utils.dag_helpers import setup_clickhouse_env

    setup_clickhouse_env()
    exec_date = context["ds"]
    run_gold(exec_date=exec_date, table="metadata", raise_on_error=True)


@task(task_id="task_validate_gold_companies")
def validate_gold_companies() -> None:
    """Validate Gold dim_companies and active view in ClickHouse using Soda Core."""
    from src.quality.soda_validator import run_gold_scan

    run_gold_scan(contract_path="soda/contracts/gold_companies_contract.yml")


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
    validate_silver_metadata_task = validate_silver_metadata()
    load_gold_metadata_task = load_gold_metadata()
    validate_gold_companies_task = validate_gold_companies()

    (
        extract_metadata_task
        >> ingest_bronze_metadata_task
        >> deduplicate_silver_metadata_task
        >> validate_silver_metadata_task
        >> load_gold_metadata_task
        >> validate_gold_companies_task
    )
