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
