import os
from unittest.mock import MagicMock, patch

from src.utils.alerts import send_airflow_failure_discord, send_airflow_failure_email


def test_send_airflow_failure_email() -> None:
    """Verify that send_airflow_failure_email parses context and triggers send_email correctly."""
    mock_ti = MagicMock()
    mock_ti.task_id = "test_task"
    mock_ti.dag_id = "test_dag"
    mock_ti.operator = "PythonOperator"
    mock_ti.try_number = 1
    mock_ti.max_tries = 2
    mock_ti.duration = 5.25
    mock_ti.log_url = "http://localhost:8080/log"

    mock_dag = MagicMock()
    mock_dag.description = "Test DAG Description"
    mock_dag.tags = ["test", "tag"]
    mock_dag.default_args = {"email": ["alert_receiver@example.com"]}

    context = {
        "task_instance": mock_ti,
        "dag": mock_dag,
        "logical_date": "2026-06-12",
        "exception": ValueError("Test error exception"),
    }

    with patch("src.utils.alerts.send_email") as mock_send_email:
        send_airflow_failure_email(context)
        mock_send_email.assert_called_once()
        _, kwargs = mock_send_email.call_args
        assert kwargs["to"] == ["alert_receiver@example.com"]
        assert "Airflow Alert: Task Failed - test_task" in kwargs["subject"]
        assert "test_dag" in kwargs["html_content"]
        assert "Test DAG Description" in kwargs["html_content"]
        assert "test, tag" in kwargs["html_content"]
        assert "PythonOperator" in kwargs["html_content"]
        assert "Test error exception" in kwargs["html_content"]
        assert "5.25s" in kwargs["html_content"]


def test_send_airflow_failure_email_no_recipients() -> None:
    """Verify that send_airflow_failure_email returns early if no recipients are configured."""
    mock_ti = MagicMock()
    mock_dag = MagicMock()
    mock_dag.default_args = {"email": []}

    context = {
        "task_instance": mock_ti,
        "dag": mock_dag,
    }

    with patch("src.utils.alerts.send_email") as mock_send_email:
        send_airflow_failure_email(context)
        mock_send_email.assert_not_called()


def test_send_airflow_failure_discord() -> None:
    """Verify that send_airflow_failure_discord parses context and triggers urlopen correctly."""
    mock_ti = MagicMock()
    mock_ti.task_id = "test_task"
    mock_ti.dag_id = "test_dag"
    mock_ti.try_number = 1
    mock_ti.max_tries = 2
    mock_ti.log_url = "http://localhost:8080/log"

    mock_dag = MagicMock()

    context = {
        "task_instance": mock_ti,
        "dag": mock_dag,
        "logical_date": "2026-06-12",
        "exception": ValueError("Test error exception"),
    }

    with (
        patch.dict(os.environ, {"DISCORD_WEBHOOK_URL": "https://discord.com/api/webhooks/dummy"}),
        patch("requests.post") as mock_post,
    ):
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_post.return_value = mock_response

        send_airflow_failure_discord(context)

        mock_post.assert_called_once()
        _, kwargs = mock_post.call_args
        payload = kwargs["json"]

        assert payload["username"] == "Airflow Alerts"
        embed = payload["embeds"][0]
        assert embed["title"] == "Task Failure Notification"

        fields = {f["name"]: f["value"] for f in embed["fields"]}
        assert fields["DAG ID"] == "`test_dag`"
        assert fields["Task ID"] == "`test_task`"
        assert "Test error exception" in fields["Exception Encountered"]


def test_send_airflow_failure_discord_with_metadata() -> None:
    """Verify that send_airflow_failure_discord includes DAG description, tags, duration, operator, etc."""
    mock_ti = MagicMock()
    mock_ti.task_id = "test_task"
    mock_ti.dag_id = "test_dag"
    mock_ti.operator = "PythonOperator"
    mock_ti.try_number = 2
    mock_ti.max_tries = 4
    mock_ti.duration = 15.75
    mock_ti.log_url = "http://localhost:8080/log"

    mock_dag = MagicMock()
    mock_dag.description = "Test DAG Description"
    mock_dag.tags = ["test", "tag"]

    context = {
        "task_instance": mock_ti,
        "dag": mock_dag,
        "logical_date": "2026-06-12",
        "exception": ValueError("Test error exception"),
    }

    with (
        patch.dict(os.environ, {"DISCORD_WEBHOOK_URL": "https://discord.com/api/webhooks/dummy"}),
        patch("requests.post") as mock_post,
    ):
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_post.return_value = mock_response

        send_airflow_failure_discord(context)

        mock_post.assert_called_once()
        _, kwargs = mock_post.call_args
        payload = kwargs["json"]
        embed = payload["embeds"][0]

        fields = {f["name"]: f["value"] for f in embed["fields"]}
        assert fields["DAG ID"] == "`test_dag`"
        assert fields["DAG Description"] == "Test DAG Description"
        assert fields["Tags"] == "test, tag"
        assert fields["Task ID"] == "`test_task`"
        assert fields["Operator"] == "`PythonOperator`"
        assert fields["Attempt"] == "2 / 5"
        assert fields["Duration"] == "15.75s"
        assert fields["Execution Date"] == "2026-06-12"
        assert fields["Logs URL"] == "[View Logs in Airflow](http://localhost:8080/log)"
        assert "Test error exception" in fields["Exception Encountered"]


def test_execution_date_fallbacks() -> None:
    """Verify that execution_date fallback logic successfully resolves date when logical_date is None."""
    mock_ti = MagicMock()
    mock_ti.task_id = "test_task"
    mock_ti.dag_id = "test_dag"
    mock_ti.operator = "PythonOperator"
    mock_ti.try_number = 1
    mock_ti.max_tries = 1
    mock_ti.log_url = "http://localhost:8080/log"

    mock_dag = MagicMock()
    mock_dag.description = "Test Description"
    mock_dag.tags = []
    mock_dag.default_args = {"email": ["test@example.com"]}

    context_ds = {
        "task_instance": mock_ti,
        "dag": mock_dag,
        "logical_date": None,
        "ds": "2026-06-12-ds",
        "exception": ValueError("Error"),
    }
    with patch("src.utils.alerts.send_email") as mock_send_email:
        send_airflow_failure_email(context_ds)
        _, kwargs = mock_send_email.call_args
        assert "2026-06-12-ds" in kwargs["html_content"]

    mock_dag_run = MagicMock()
    mock_dag_run.run_after = "2026-06-12-run_after"
    mock_dag_run.start_date = "2026-06-12-start_date"
    mock_dag_run.execution_date = "2026-06-12-exec_date"

    context_run_after = {
        "task_instance": mock_ti,
        "dag": mock_dag,
        "logical_date": None,
        "ds": None,
        "dag_run": mock_dag_run,
        "exception": ValueError("Error"),
    }
    with patch("src.utils.alerts.send_email") as mock_send_email:
        send_airflow_failure_email(context_run_after)
        _, kwargs = mock_send_email.call_args
        assert "2026-06-12-run_after" in kwargs["html_content"]

    mock_dag_run_start = MagicMock()
    mock_dag_run_start.run_after = None
    mock_dag_run_start.start_date = "2026-06-12-start_date"
    mock_dag_run_start.execution_date = "2026-06-12-exec_date"

    context_start_date = {
        "task_instance": mock_ti,
        "dag": mock_dag,
        "logical_date": None,
        "ds": None,
        "dag_run": mock_dag_run_start,
        "exception": ValueError("Error"),
    }
    with patch("src.utils.alerts.send_email") as mock_send_email:
        send_airflow_failure_email(context_start_date)
        _, kwargs = mock_send_email.call_args
        assert "2026-06-12-start_date" in kwargs["html_content"]

    mock_dag_run_exec = MagicMock()
    mock_dag_run_exec.run_after = None
    mock_dag_run_exec.start_date = None
    mock_dag_run_exec.execution_date = "2026-06-12-exec_date"

    context_exec_date = {
        "task_instance": mock_ti,
        "dag": mock_dag,
        "logical_date": None,
        "ds": None,
        "dag_run": mock_dag_run_exec,
        "exception": ValueError("Error"),
    }
    with patch("src.utils.alerts.send_email") as mock_send_email:
        send_airflow_failure_email(context_exec_date)
        _, kwargs = mock_send_email.call_args
        assert "2026-06-12-exec_date" in kwargs["html_content"]

    context_none = {
        "task_instance": mock_ti,
        "dag": mock_dag,
        "logical_date": None,
        "ds": None,
        "dag_run": None,
        "exception": ValueError("Error"),
    }
    with patch("src.utils.alerts.send_email") as mock_send_email:
        send_airflow_failure_email(context_none)
        _, kwargs = mock_send_email.call_args
        assert "Not set" in kwargs["html_content"]


def test_send_airflow_failure_discord_no_webhook() -> None:
    """Verify that send_airflow_failure_discord returns early if no webhook is configured."""
    mock_ti = MagicMock()
    mock_dag = MagicMock()

    context = {
        "task_instance": mock_ti,
        "dag": mock_dag,
    }

    with patch.dict(os.environ, {}, clear=True), patch("requests.post") as mock_post:
        send_airflow_failure_discord(context)
        mock_post.assert_not_called()
