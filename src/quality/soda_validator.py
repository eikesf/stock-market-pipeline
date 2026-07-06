"""Module for running Soda Core quality contracts programmatically."""

import os
import sys
from pathlib import Path

from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from soda.scan import Scan

from src.streaming.spark_session import create_spark_session
from src.utils.logger import logger

SODA_FAIL_CODE = 2


def _resolve_soda_paths(contract_path: str) -> tuple[str, str]:
    """Dynamically resolve the paths for configuration.yml and the contract file.

    This ensures that paths are resolved correctly regardless of whether the code
    is running in a local virtual environment, python_finance container, or Airflow.

    Args:
        contract_path: The contract file path to resolve.

    Returns:
        A tuple of (resolved_config_path, resolved_contract_path).
    """
    this_file_dir = Path(__file__).resolve().parent
    soda_dir = None

    # 1. Search upwards for a sibling 'soda' directory
    for parent in this_file_dir.parents:
        potential_soda = parent / "soda"
        if potential_soda.exists() and potential_soda.is_dir():
            soda_dir = potential_soda
            break

    # 2. Fallbacks for Airflow/Docker environments where soda is mounted outside dags/src
    if not soda_dir:
        for path_candidate in [Path("/opt/airflow/soda"), Path("/app/soda")]:
            if path_candidate.exists() and path_candidate.is_dir():
                soda_dir = path_candidate
                break

    if not soda_dir:
        raise FileNotFoundError("Could not locate the 'soda' directory in any of the standard locations.")

    # Resolve configuration.yml
    config_path = soda_dir / "configuration.yml"
    if not config_path.exists():
        raise FileNotFoundError(f"Soda configuration file not found at: {config_path}")

    # Resolve contract file path
    # If the contract path has a prefix like 'soda/contracts/...', remove the 'soda/' part
    contract_rel_path = contract_path
    if contract_path.startswith("soda/"):
        contract_rel_path = contract_path[len("soda/") :]

    contract_file_path = soda_dir / contract_rel_path
    if not contract_file_path.exists():
        raise FileNotFoundError(f"Soda contract file not found at: {contract_file_path}")

    return str(config_path), str(contract_file_path)


def run_silver_scan(table_name: str, contract_path: str) -> None:
    """Run a Soda Core quality scan against a local Spark/Delta table.

    Args:
        table_name: The name of the temporary view to register.
        contract_path: The filesystem path to the SodaCL YAML contract.

    Raises:
        ValueError: If scan fails or table name is unknown.
        FileNotFoundError: If the delta path does not exist.
    """
    logger.info(
        f"Initializing Spark session for Soda Silver scan on '{table_name}' using contract '{contract_path}'..."
    )

    # Dynamically resolve data directory (handles local CLI vs Airflow worker paths)
    data_dir_env = os.environ.get("DATA_DIR")
    base_data_path = Path(data_dir_env) if data_dir_env else Path("data")

    path_mappings = {
        "silver_metadata": base_data_path / "silver/metadata",
        "silver_metrics": base_data_path / "silver/metrics",
        "silver_prices": base_data_path / "silver/prices",
    }

    path = path_mappings.get(table_name)
    if not path:
        raise ValueError(f"Unknown Silver table: {table_name}")

    if not path.exists():
        raise FileNotFoundError(f"Delta table path not found: {path.resolve()}")

    # Initialize local PySpark session
    spark = create_spark_session()

    # Load Delta table as a temp view in Spark
    logger.info(f"Loading Delta table from {path} as view '{table_name}'...")
    df = spark.read.format("delta").load(str(path))
    df.createOrReplaceTempView(table_name)

    config_file, contract_file = _resolve_soda_paths(contract_path)

    # Instantiate Soda Scan
    scan = Scan()
    scan.set_verbose(False)
    scan.add_configuration_yaml_file(config_file)
    scan.set_data_source_name("spark")
    scan.add_spark_session(spark, "spark")
    scan.add_sodacl_yaml_file(contract_file)

    logger.info(f"--- Executing Soda Scan for {table_name} ---")
    result = scan.execute()

    sys.stdout.write(scan.get_logs_text() + "\n")

    if result >= SODA_FAIL_CODE:
        raise ValueError(
            f"Soda quality scan failed for {table_name} with code {result}. Please inspect the logs above."
        )

    logger.success(f"Soda quality scan for {table_name} completed successfully.")


def run_gold_scan(contract_path: str) -> None:
    """Run a Soda Core quality scan against the ClickHouse database.

    Args:
        contract_path: The filesystem path to the SodaCL YAML contract.

    Raises:
        ValueError: If scan fails.
    """
    logger.info(f"Executing Soda Gold scan using contract '{contract_path}'...")

    # Expose ClickHouse connection properties from Airflow ClickHouseHook
    try:
        conn = ClickHouseHook.get_connection("clickhouse_default")
        os.environ["CLICKHOUSE_HOST"] = conn.host or "clickhouse"
        os.environ["CLICKHOUSE_USER"] = conn.login or "default"
        os.environ["CLICKHOUSE_PASSWORD"] = conn.password or ""
        os.environ["CLICKHOUSE_DB"] = conn.schema or "stock_market"
    except Exception as e:
        logger.debug(f"Could not fetch ClickHouse connection from Airflow context: {e}")

    # Default MySQL wire protocol port in ClickHouse container is 9004
    if not os.environ.get("CLICKHOUSE_MYSQL_PORT"):
        os.environ["CLICKHOUSE_MYSQL_PORT"] = "9004"

    config_file, contract_file = _resolve_soda_paths(contract_path)

    # Instantiate Soda Scan
    scan = Scan()
    scan.set_verbose(False)
    scan.add_configuration_yaml_file(config_file)
    scan.set_data_source_name("clickhouse")
    scan.add_sodacl_yaml_file(contract_file)

    result = scan.execute()

    # Log scan results
    sys.stdout.write(scan.get_logs_text() + "\n")

    if result >= SODA_FAIL_CODE:
        raise ValueError(
            f"Soda quality scan failed for {contract_path} with code {result}. Please inspect the logs above."
        )

    logger.success(f"Soda quality scan for {contract_path} completed successfully.")
