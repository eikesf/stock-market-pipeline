import logging
import os


def setup_clickhouse_env() -> None:
    """Set ClickHouse environment variables from Airflow connection if available.

    This helper centralizes retrieving the 'clickhouse_default' connection from
    Airflow and populating the environment variables, avoiding boilerplate in DAGs.
    """
    try:
        from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook  # noqa: PLC0415

        conn = ClickHouseHook.get_connection("clickhouse_default")
        os.environ["CLICKHOUSE_HOST"] = conn.host or "clickhouse"
        os.environ["CLICKHOUSE_PORT"] = str(conn.port or 8123)
        os.environ["CLICKHOUSE_USER"] = conn.login or "default"
        os.environ["CLICKHOUSE_PASSWORD"] = conn.password or ""
        os.environ["CLICKHOUSE_DB"] = conn.schema or "stock_market"
    except Exception as e:
        logging.getLogger("airflow.dag").warning("Failed to get clickhouse connection from Airflow: %s", e)
