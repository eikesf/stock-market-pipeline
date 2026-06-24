import os
from datetime import datetime
from typing import Any

import requests
from airflow.utils.email import send_email

from src.utils.logger import logger

MAX_EXCEPTION_LENGTH = 800


def _extract_operator_name(ti: object) -> str:
    """Extract the operator name from a task instance.

    Args:
        ti: The task instance object.

    Returns:
        The string name of the operator class, falling back to 'PythonOperator'
        if not found or is a mock object.
    """
    ti_op = getattr(ti, "operator", None)
    if isinstance(ti_op, str):
        return ti_op

    task_obj = getattr(ti, "task", None)
    if not task_obj:
        return "PythonOperator"

    op_name = getattr(task_obj, "operator_name", None)
    if isinstance(op_name, str):
        return op_name

    if type(task_obj).__name__ not in ("MagicMock", "Mock", "NonCallableMagicMock"):
        return type(task_obj).__name__

    return "PythonOperator"


def _extract_duration(ti: object) -> str:
    """Extract task execution duration from a task instance.

    Calculates the duration using start_date and end_date if the duration
    attribute is not directly set.

    Args:
        ti: The task instance object.

    Returns:
        A string formatted duration (e.g. '1.25s'), or 'Not registered' if unavailable.
    """
    ti_duration = getattr(ti, "duration", None)
    if isinstance(ti_duration, (int, float)):
        return f"{ti_duration:.2f}s"

    start_date = getattr(ti, "start_date", None)
    end_date = getattr(ti, "end_date", None)
    if isinstance(start_date, datetime) and isinstance(end_date, datetime):
        return f"{(end_date - start_date).total_seconds():.2f}s"

    return "Not registered"


def _extract_execution_date(context: dict[str, Any]) -> str:
    """Extract the execution date with fallbacks from Airflow context.

    Checks logical_date, ds, and dag_run attributes to determine the target date.

    Args:
        context: The Airflow callback context dictionary.

    Returns:
        A string representation of the execution date, or 'Not set'.
    """
    logical_date = context.get("logical_date")
    if logical_date is not None:
        return str(logical_date)

    ds = context.get("ds")
    if ds is not None:
        return str(ds)

    dag_run = context.get("dag_run")
    if dag_run is None:
        return "Not set"

    # Try run_after, start_date, execution_date on dag_run
    run_after = getattr(dag_run, "run_after", None)
    if run_after is not None:
        return str(run_after)

    start_date = getattr(dag_run, "start_date", None)
    if start_date is not None:
        return str(start_date)

    exec_date = getattr(dag_run, "execution_date", None)
    return str(exec_date) if exec_date is not None else "Not set"


def _extract_alert_context(context: dict[str, Any]) -> dict[str, Any]:
    """Extract common alerting fields from the Airflow callback context.

    Gathers details about the task run, operator, trial counts, dates, and
    logs to construct a unified metadata dictionary.

    Args:
        context: The Airflow callback context dictionary.

    Returns:
        A dictionary containing keys for alerts construction (e.g., 'dag_id',
        'task_id', 'operator_name', 'try_number', 'log_url', etc.).
    """
    ti = context["task_instance"]
    dag = context["dag"]

    task_id = ti.task_id
    dag_id = ti.dag_id

    desc_val = getattr(dag, "description", None)
    dag_desc = desc_val if isinstance(desc_val, str) else "No description"

    tags_val = getattr(dag, "tags", None)
    if isinstance(tags_val, (list, tuple)):
        clean_tags = [str(t) for t in tags_val if type(t).__name__ not in ("MagicMock", "Mock", "NonCallableMagicMock")]
        dag_tags = ", ".join(clean_tags) if clean_tags else "None"
    else:
        dag_tags = "None"

    operator_name = _extract_operator_name(ti)
    duration = _extract_duration(ti)
    execution_date = _extract_execution_date(context)

    return {
        "dag_id": dag_id,
        "dag_desc": dag_desc,
        "dag_tags": dag_tags,
        "task_id": task_id,
        "operator_name": operator_name,
        "try_number": ti.try_number,
        "max_tries": ti.max_tries + 1,
        "duration": duration,
        "execution_date": execution_date,
        "log_url": ti.log_url,
        "exception": context.get("exception"),
    }


def send_airflow_failure_email(context: dict[str, Any]) -> None:
    """Callback function to send an email notification when an Airflow task fails.

    Extracts detailed failure context and delivers a formatted HTML message.

    Args:
        context: The Airflow callback context dictionary.
    """
    logger.info(f"Failure callback context keys: {list(context.keys())}")

    info = _extract_alert_context(context)
    recipients = context["dag"].default_args.get("email")

    if not recipients:
        logger.warning("No emails were provided for sending alerts. Skipping email notification.")
        return

    subject = f"Airflow Alert: Task Failed - {info['task_id']} (DAG: {info['dag_id']})"

    html_content = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: auto; border: 1px solid #ddd; padding: 25px; border-radius: 6px; color: #333;">
        <h2 style="color: #d9534f; margin-top: 0; border-bottom: 2px solid #d9534f; padding-bottom: 10px;">
            Task Failure Notification
        </h2>

        <p style="font-size: 15px; margin-bottom: 20px;">
            The task execution failed in the Airflow environment. See the details below:
        </p>

        <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
            <tr>
                <td style="padding: 6px 0; font-weight: bold; width: 35%;">DAG ID:</td>
                <td style="padding: 6px 0;"><code>{info["dag_id"]}</code></td>
            </tr>
            <tr>
                <td style="padding: 6px 0; font-weight: bold;">DAG Description:</td>
                <td style="padding: 6px 0; color: #555;">{info["dag_desc"]}</td>
            </tr>
            <tr>
                <td style="padding: 6px 0; font-weight: bold;">Tags:</td>
                <td style="padding: 6px 0; color: #666; font-size: 12px;">{info["dag_tags"]}</td>
            </tr>
            <tr>
                <td style="padding: 6px 0; font-weight: bold;">Task ID:</td>
                <td style="padding: 6px 0;"><code>{info["task_id"]}</code></td>
            </tr>
            <tr>
                <td style="padding: 6px 0; font-weight: bold;">Operator:</td>
                <td style="padding: 6px 0; font-size: 13px; color: #666;"><code>{info["operator_name"]}</code></td>
            </tr>
            <tr>
                <td style="padding: 6px 0; font-weight: bold;">Attempt:</td>
                <td style="padding: 6px 0;">{info["try_number"]} / {info["max_tries"]}</td>
            </tr>
            <tr>
                <td style="padding: 6px 0; font-weight: bold;">Duration:</td>
                <td style="padding: 6px 0;">{info["duration"]}</td>
            </tr>
            <tr>
                <td style="padding: 6px 0; font-weight: bold;">Execution Date:</td>
                <td style="padding: 6px 0;">{info["execution_date"]}</td>
            </tr>
        </table>

        <div style="text-align: center; margin: 25px 0;">
            <a href="{info["log_url"]}" style="display: inline-block; background-color: #0275d8; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px; font-weight: bold;">
                View Logs in Airflow
            </a>
        </div>

        <div style="margin-top: 20px;">
            <p style="font-weight: bold; margin-bottom: 5px;">Exception Encountered:</p>
            <pre style="background-color: #f7f7f9; border: 1px solid #e1e1e8; padding: 15px; border-radius: 4px; overflow-x: auto; font-family: Courier, monospace; color: #c7254e; font-size: 13px; margin: 0;">{info["exception"]}</pre>
        </div>
    </div>
    """

    try:
        send_email(to=recipients, subject=subject, html_content=html_content)
        logger.success(f"Alert email for task {info['task_id']} sent successfully.")
    except Exception as e:
        logger.error(f"Error sending alert email: {e}")


def send_airflow_failure_discord(context: dict[str, Any]) -> None:
    """Callback function to send an alert notification to Discord on task failure.

    Args:
        context: The Airflow callback context dictionary.
    """
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

    if not webhook_url:
        logger.warning("DISCORD_WEBHOOK_URL not configured. Skipping Discord alert.")
        return

    info = _extract_alert_context(context)

    exception_str = str(info["exception"])
    if len(exception_str) > MAX_EXCEPTION_LENGTH:
        exception_str = exception_str[:MAX_EXCEPTION_LENGTH] + "..."

    payload: dict[str, Any] = {
        "username": "Airflow Alerts",
        "embeds": [
            {
                "title": "Task Failure Notification",
                "description": "A task execution failed in the Airflow environment. See details below:",
                "color": 14358895,
                "fields": [
                    {"name": "DAG ID", "value": f"`{info['dag_id']}`", "inline": True},
                    {"name": "DAG Description", "value": info["dag_desc"], "inline": True},
                    {"name": "Tags", "value": info["dag_tags"], "inline": True},
                    {"name": "Task ID", "value": f"`{info['task_id']}`", "inline": True},
                    {"name": "Operator", "value": f"`{info['operator_name']}`", "inline": True},
                    {"name": "Attempt", "value": f"{info['try_number']} / {info['max_tries']}", "inline": True},
                    {"name": "Duration", "value": info["duration"], "inline": True},
                    {"name": "Execution Date", "value": info["execution_date"], "inline": True},
                    {"name": "Logs URL", "value": f"[View Logs in Airflow]({info['log_url']})", "inline": False},
                    {"name": "Exception Encountered", "value": f"```python\n{exception_str}\n```", "inline": False},
                ],
                "footer": {"text": "Airflow Monitoring System"},
            }
        ],
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        if response.status_code in (200, 204):
            logger.success(f"Discord alert for task {info['task_id']} sent successfully.")
        else:
            logger.error(f"Failed to send Discord alert, status: {response.status_code}")
    except Exception as e:
        logger.error(f"Error sending Discord alert: {e}")
