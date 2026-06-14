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


def _create_spark_pool() -> None:
    """Programmatically checks and creates the 'spark_write_pool' in the Airflow metadata database.

    This pool is configured with a single slot to serialize Spark write operations,
    preventing concurrent write conflicts and file corruption on local file systems.
    """
    try:
        from airflow.models.pool import Pool
        from airflow.utils.session import create_session

        with create_session() as session:
            pool = session.query(Pool).filter(Pool.pool == "spark_write_pool").first()
            if not pool:
                new_pool = Pool(
                    pool="spark_write_pool",
                    slots=1,
                    description="Serializes Spark writes to prevent Delta Lake filesystem conflicts",
                )
                session.add(new_pool)
                session.commit()
    except Exception as e:
        import logging

        logging.getLogger("airflow.dag").warning("Failed to programmatically create spark_write_pool: %s", e)


_create_spark_pool()


@task(task_id="task_extract_prices")
def extract_prices(**context: Any) -> None:
    """Extracts stock prices from Yahoo Finance for a list of tickers and saves them to Landing layer."""
    from src.producer.generator import run_generator

    exec_date = context["ds"]
    run_generator(exec_date=exec_date)


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
    from src.streaming.gold import run_gold

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
