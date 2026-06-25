import os
from datetime import datetime, timedelta

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
    "execution_timeout": timedelta(minutes=30),
}


@task(task_id="task_optimize_and_vacuum", pool="spark_write_pool")
def optimize_and_vacuum() -> None:
    """Run compaction and vacuum maintenance on all medallion tables."""
    from src.streaming.maintenance import run_maintenance

    run_maintenance(retention_hours=168.0)


with DAG(
    dag_id="dag_delta_maintenance",
    default_args=default_args,
    description="Delta Lake optimization and vacuum maintenance",
    schedule=CronTriggerTimetable("@weekly", timezone="UTC"),
    start_date=datetime(2026, 6, 18),
    catchup=False,
    tags=["stock_market", "maintenance"],
) as dag:
    optimize_and_vacuum()
