import os
from pathlib import Path

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from pyspark.sql import DataFrame, SparkSession

from src.utils.logger import logger


def read_delta_table(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read a Delta table from the given path.

    Args:
        spark: The active Spark session.
        path: Path to the Delta table directory.

    Returns:
        A Spark DataFrame loaded from the Delta path.
    """
    return spark.read.format("delta").load(str(path))


def write_delta_table(df: DataFrame, path: str | Path, mode: str = "append") -> None:
    """Write a DataFrame to a Delta table with the given mode.

    Automatically enables overwriteSchema for overwrite mode, and mergeSchema
    for append mode.

    Args:
        df: Spark DataFrame to write.
        path: Target path for the Delta table.
        mode: Spark write mode (e.g., 'append', 'overwrite').
    """
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    elif mode == "append":
        writer = writer.option("mergeSchema", "true")
    writer.save(str(path))
    logger.success(f"Data successfully written to {path} (Mode: {mode})")


def get_clickhouse_client() -> Client:
    """Responsible for connecting with the ClickHouse database.

    Uses environment variables for host, port, user, password, and database.

    Returns:
        A ClickHouse driver Client instance.
    """
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=os.environ.get("CLICKHOUSE_PORT", "8123"),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "stock_market"),
    )
