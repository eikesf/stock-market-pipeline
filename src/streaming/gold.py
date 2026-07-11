import argparse
import sys
from collections.abc import Callable, Iterable
from datetime import date

from clickhouse_connect.driver.client import Client
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, date_format

from src.producer.config import SILVER_METADATA_DIR, SILVER_METRICS_DIR, SILVER_PRICES_DIR
from src.streaming.spark_session import create_spark_session
from src.streaming.utils import get_clickhouse_client, read_delta_table
from src.utils.logger import logger


def make_write_partition(table_name: str, columns: list[str]) -> Callable[[Iterable[Row]], None]:
    """Create a partition writer function for Spark executors.

    Args:
        table_name: The destination table name in ClickHouse.
        columns: The column names in the Spark DataFrame.

    Returns:
        A callable that can be passed to RDD.foreachPartition().
    """

    def write_partition(partition_iterator: Iterable[Row]) -> None:
        import pandas as pd

        from src.streaming.gold import get_clickhouse_client

        rows = list(partition_iterator)
        if not rows:
            return

        df_partition = pd.DataFrame([row.asDict() for row in rows], columns=columns)
        clickhouse_client = get_clickhouse_client()
        try:
            clickhouse_client.insert_df(table_name, df_partition)
        finally:
            clickhouse_client.close()

    return write_partition


def _load_prices_to_gold(spark: SparkSession, client: Client) -> None:
    """Read silver prices Delta table, clean it, and load it into ClickHouse fact_prices.

    Args:
        spark: The active Spark session.
        client: The ClickHouse Connect client.
    """
    logger.info("Processing Silver Prices...")
    df_prices = read_delta_table(spark, SILVER_PRICES_DIR)
    df_prices = df_prices.select(
        col("date"),
        col("ticker"),
        col("open"),
        col("high"),
        col("low"),
        col("close"),
        col("adj_close"),
        col("volume"),
        col("dividends"),
        col("stock_splits"),
        col("ingestion_timestamp"),
    )
    # Clean prices before loading to Gold to satisfy quality contracts:
    # 1. Price fields and volume must be strictly positive (greater than zero)
    # 2. High must be >= low, open, and close
    # 3. Low must be <= open and close
    df_prices = df_prices.filter(
        (col("open") > 0)
        & (col("high") > 0)
        & (col("low") > 0)
        & (col("close") > 0)
        & (col("adj_close") > 0)
        & (col("volume") >= 0)
        & (col("high") >= col("low"))
        & (col("high") >= col("open"))
        & (col("high") >= col("close"))
        & (col("low") <= col("open"))
        & (col("low") <= col("close"))
    )

    # Extract unique partition IDs in 'YYYYMM' format based on dates
    partitions_df = df_prices.select(date_format(col("date"), "yyyyMM").alias("partition")).distinct()
    affected_partitions = [row["partition"] for row in partitions_df.collect() if row["partition"] is not None]

    if not affected_partitions:
        logger.warning("Silver prices DataFrame is empty. Nothing to load to Gold.")
        return

    for partition in affected_partitions:
        logger.info(f"Dropping partition '{partition}' in ClickHouse fact_prices table...")
        client.command(f"ALTER TABLE stock_market.fact_prices DROP PARTITION '{partition}'")

    logger.info("Inserting new/updated prices into ClickHouse fact_prices...")

    df_prices.rdd.foreachPartition(make_write_partition("stock_market.fact_prices", df_prices.columns))
    logger.success("Silver prices loaded to Gold successfully.")


def _load_metadata_to_gold(spark: SparkSession, client: Client) -> None:
    """Read silver metadata Delta table, clean it, and load it into ClickHouse dim_companies.

    Args:
        spark: The active Spark session.
        client: The ClickHouse Connect client.
    """
    logger.info("Processing Silver Metadata...")
    df_metadata = read_delta_table(spark, SILVER_METADATA_DIR)
    df_metadata = df_metadata.select(
        col("ticker"),
        col("short_name"),
        col("sector"),
        col("industry"),
        col("country"),
        col("isin"),
        col("full_time_employees"),
        col("exchange"),
        col("currency"),
        col("extraction_date"),
        col("ingestion_timestamp"),
        col("start_date"),
        col("end_date"),
        col("is_active"),
    )

    client.command("CREATE TABLE IF NOT EXISTS stock_market.dim_companies_staging AS stock_market.dim_companies")
    client.command("TRUNCATE TABLE stock_market.dim_companies_staging")

    df_metadata.rdd.foreachPartition(make_write_partition("stock_market.dim_companies_staging", df_metadata.columns))

    client.command("EXCHANGE TABLES stock_market.dim_companies AND stock_market.dim_companies_staging")
    client.command("DROP TABLE IF EXISTS stock_market.dim_companies_staging")
    logger.success("Silver Metadata loaded to Gold successfully.")


def _load_metrics_to_gold(spark: SparkSession, client: Client) -> None:
    """Read silver metrics Delta table and load it into ClickHouse fact_company_metrics.

    Loads the historical snapshot metrics from the Silver layer Delta table,
    identifies the affected partitions by extraction date, drops the existing
    partitions in ClickHouse to prevent duplication on reruns, and inserts the
    latest data.

    Args:
        spark: The active Spark session.
        client: The ClickHouse Connect client.
    """
    logger.info("Processing Silver Metrics...")
    df_metrics = read_delta_table(spark, SILVER_METRICS_DIR)
    df_metrics = df_metrics.select(
        col("extraction_date"),
        col("ticker"),
        col("dividend_yield"),
        col("trailing_pe"),
        col("peg_ratio"),
        col("price_to_book"),
        col("enterprise_to_ebitda"),
        col("enterprise_to_ebit"),
        col("book_value"),
        col("trailing_eps"),
        col("price_to_sales"),
        col("operating_margins"),
        col("asset_turnover"),
        col("shares_outstanding"),
        col("market_cap"),
        col("ebitda"),
        col("total_debt"),
        col("total_cash"),
        col("debt_to_equity"),
        col("roa"),
        col("roe"),
        col("current_ratio"),
        col("gross_margins"),
        col("ebitda_margins"),
        col("profit_margins"),
        col("net_income_to_common"),
        col("ingestion_timestamp"),
    )

    partitions_df = df_metrics.select(date_format(col("extraction_date"), "yyyyMM").alias("partition")).distinct()
    affected_partitions = [row["partition"] for row in partitions_df.collect() if row["partition"] is not None]

    if not affected_partitions:
        logger.warning("No partitions found in Silver metrics DataFrame. Nothing to load to Gold.")
        return

    for partition in affected_partitions:
        logger.info(f"Dropping partition '{partition}' in ClickHouse fact_company_metrics table...")
        client.command(f"ALTER TABLE stock_market.fact_company_metrics DROP PARTITION '{partition}'")

    logger.info("Inserting new/updated metrics into ClickHouse fact_company_metrics...")

    df_metrics.rdd.foreachPartition(make_write_partition("stock_market.fact_company_metrics", df_metrics.columns))
    logger.success("Silver metrics loaded to gold successfully.")


def _process_gold_table(
    table_name: str,
    target_table: str,
    exists: bool,
    load_fn: Callable[[], None],
) -> None:
    """Decide and load a specific silver table to ClickHouse gold layer.

    Args:
        table_name: Name of the table ("prices", "metadata", or "metrics").
        target_table: The target table selected in CLI filter.
        exists: Boolean indicating if the silver Delta table exists.
        load_fn: Function to load the specific table.
    """
    display_name = table_name.capitalize()
    if exists:
        load_fn()
    elif target_table not in (table_name, "all"):
        logger.info(f"Silver {display_name} skipped (not requested by target table selection).")
    else:
        logger.warning(f"Silver {display_name} Delta table not found. Skipping {table_name} load.")


def run_gold(exec_date: str, table: str = "all") -> None:
    """Clean and load prices, metadata, and metrics from Silver to Gold Layer (ClickHouse).

    Args:
        exec_date: Execution date in YYYY-MM-DD format.
        table: Target table to load ("prices", "metadata", "metrics", or "all").

    Raises:
        SystemExit: If the date format is invalid or processing fails.
    """
    try:
        date.fromisoformat(exec_date)
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD format.")
        sys.exit(1)

    logger.info(f"Starting Gold layer processing for table '{table}' (execution date: {exec_date})...")

    # Creating spark session
    spark = create_spark_session()
    try:
        # Check delta tables existence based on selected table filter
        prices_exist = table in ("prices", "all") and (SILVER_PRICES_DIR / "_delta_log").exists()
        metadata_exist = table in ("metadata", "all") and (SILVER_METADATA_DIR / "_delta_log").exists()
        metrics_exist = table in ("metrics", "all") and (SILVER_METRICS_DIR / "_delta_log").exists()

        if not prices_exist and not metadata_exist and not metrics_exist:
            logger.warning(
                f"No matching Silver Delta tables exist to process for table '{table}'. Skipping Gold layer processing."
            )
            return

        # Creating clickhouse connection
        client = get_clickhouse_client()

        _process_gold_table("prices", table, prices_exist, lambda: _load_prices_to_gold(spark, client))
        _process_gold_table("metadata", table, metadata_exist, lambda: _load_metadata_to_gold(spark, client))
        _process_gold_table("metrics", table, metrics_exist, lambda: _load_metrics_to_gold(spark, client))

        logger.success("Gold layer processing completed successfully.")

    except Exception as e:
        logger.exception(f"Failed to process Gold layer: {e}")
        sys.exit(1)
    finally:
        # Stopping spark session
        spark.stop()


def main() -> None:
    """CLI entrypoint for Gold layer loading to ClickHouse.

    Parses CLI arguments for target date and target table, and runs the Gold pipeline.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=str,
        default=date.today().isoformat(),
        help="Date to process (format YYYY-MM-DD)",
    )
    parser.add_argument(
        "--table",
        type=str,
        default="all",
        choices=["prices", "metadata", "metrics", "all"],
        help="Select which table to load (prices, metadata, metrics, or all)",
    )
    args, _ = parser.parse_known_args()

    run_gold(args.date, args.table)


if __name__ == "__main__":
    main()
