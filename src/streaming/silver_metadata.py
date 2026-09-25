import argparse
import sys
from datetime import date
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, row_number, trim, upper, when
from pyspark.sql.window import Window

from src.producer.config import (
    BRONZE_METADATA_DIR,
    SILVER_METADATA_DIR,
    SILVER_METADATA_REJECTED_DIR,
    SILVER_METRICS_DIR,
    SILVER_METRICS_REJECTED_DIR,
)
from src.streaming.quality_rules import (
    METADATA_REQUIRED_COLUMNS,
    METRICS_REQUIRED_COLUMNS,
    QualityRule,
    classify,
    metadata_rules,
    metrics_rules,
    require_columns,
    split_valid_rejected,
)
from src.streaming.spark_session import create_spark_session
from src.streaming.utils import check_and_heal_corrupt_data_file, read_delta_table, write_delta_table
from src.utils.logger import logger


def _write_rejected(spark_df: DataFrame, target_dir: Path, exec_date: str, spark: SparkSession) -> None:
    """Write quarantined rows, replacing whatever the same run wrote previously.

    Args:
        spark_df: Quarantined rows produced by `split_valid_rejected`.
        target_dir: Quarantine Delta table path.
        exec_date: Execution date, used to scope the idempotent replace.
        spark: The active Spark session.
    """
    if not (target_dir / "_delta_log").exists():
        write_delta_table(spark_df, target_dir, mode="overwrite")
        return

    rejected_delta = DeltaTable.forPath(spark, str(target_dir))
    rejected_delta.delete(col("pipeline_exec_date") == lit(exec_date).cast("date"))
    write_delta_table(spark_df, target_dir, mode="append")


def _split_by_quality(df: DataFrame, rules: list[QualityRule], exec_date: str) -> tuple[DataFrame, DataFrame]:
    """Classify a Silver batch and split it into its valid and quarantined halves.

    Args:
        df: Cleaned, deduplicated Silver batch.
        rules: Quality rules for the domain.
        exec_date: Execution date recorded on quarantined rows.

    Returns:
        A `(valid_df, rejected_df)` tuple.
    """
    return split_valid_rejected(classify(df, rules), pipeline_exec_date=exec_date)


def run_silver_metadata(exec_date: str, raise_on_error: bool = False) -> None:
    """Clean, standardize, and deduplicate stock metadata from Bronze to Silver.

    This function reads raw stock metadata from the Bronze layer, standardizes
    string columns, adjusts exchange codes (e.g., normalizes 'SAO' to 'B3'),
    quarantines rows breaching a quality rule, then implements SCD Type 2 logic over the
    remaining rows to track changing attributes without duplicating records unnecessarily.

    Quarantining happens before the SCD Type 2 diff so a ticker with bad incoming data keeps its
    existing active record instead of having it closed out.

    Args:
        exec_date: Execution date in YYYY-MM-DD format.
        raise_on_error: If True, raise errors instead of exiting.

    Raises:
        SystemExit: If the date format is invalid or processing fails.
        RequiredColumnMissingError: If any row is missing ticker or extraction_date.
    """
    try:
        date.fromisoformat(exec_date)
    except ValueError as e:
        logger.error("Invalid date format. Please use YYYY-MM-DD format.")
        if raise_on_error:
            raise e
        sys.exit(1)

    logger.info(f"Starting Silver layer processing for stock metadata (execution date: {exec_date})...")

    spark = create_spark_session(raise_on_error=raise_on_error)
    try:
        # Reading bronze metadata
        metadata_df_bronze = read_delta_table(spark, BRONZE_METADATA_DIR)

        # Cleaning and organizing the metadata dataframe
        # Cast the identity columns first so the essential-column check can run before anything else
        metadata_df_bronze = metadata_df_bronze.withColumn(
            "ticker", upper(trim(col("ticker").cast("string")))
        ).withColumn("extraction_date", col("extraction_date").cast("date"))

        require_columns(metadata_df_bronze, METADATA_REQUIRED_COLUMNS, domain="metadata")

        metadata_df_silver = (
            metadata_df_bronze.withColumn("short_name", trim(col("short_name").cast("string")))
            .withColumn("sector", trim(col("sector").cast("string")))
            .withColumn("industry", trim(col("industry").cast("string")))
            .withColumn("country", trim(col("country").cast("string")))
            .withColumn("isin", trim(col("isin").cast("string")))
            .withColumn("full_time_employees", col("full_time_employees").cast("integer"))
            .withColumn("exchange", upper(trim(col("exchange").cast("string"))))
            .withColumn("currency", trim(col("currency").cast("string")))
            .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("timestamp"))
            .select(
                "ticker",
                "short_name",
                "sector",
                "industry",
                "country",
                "isin",
                "full_time_employees",
                "exchange",
                "currency",
                "extraction_date",
                "ingestion_timestamp",
            )
        )

        # Adjusting exchange and currency names to correspond to standard patterns
        metadata_df_silver = metadata_df_silver.withColumn(
            "exchange",
            when(col("exchange") == "SAO", "B3")
            .when(col("exchange") == "NYQ", "NYSE")
            .when(col("exchange").isin("NMS", "NGM", "NCM", "NASDAQ"), "NASDAQ")
            .when((col("exchange") == "N/A") & col("ticker").endswith(".SA"), "B3")
            .otherwise(col("exchange")),
        ).withColumn(
            "currency",
            when((col("currency") == "N/A") & col("ticker").endswith(".SA"), "BRL")
            .when(col("currency") == "N/A", "USD")
            .otherwise(col("currency")),
        )

        # Deduplication: Keeping only the most recent row per ticker
        window_spec = Window.partitionBy("ticker").orderBy(col("ingestion_timestamp").desc())

        metadata_df_silver = (
            metadata_df_silver.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")
        )

        # Split before the SCD Type 2 diff below: a quarantined ticker must not participate in the
        # merge, or bad incoming data would close out its previously good active row.
        metadata_df_silver, rejected_df = _split_by_quality(metadata_df_silver, metadata_rules(), exec_date)

        metadata_df_silver = (
            metadata_df_silver.withColumn("start_date", col("extraction_date"))
            .withColumn("end_date", lit(None).cast("date"))
            .withColumn("is_active", lit(1).cast("integer"))
            .select(
                "ticker",
                "short_name",
                "sector",
                "industry",
                "country",
                "isin",
                "full_time_employees",
                "exchange",
                "currency",
                "extraction_date",
                "ingestion_timestamp",
                "start_date",
                "end_date",
                "is_active",
            )
        )

        is_cold_start = not (SILVER_METADATA_DIR / "_delta_log").exists()

        if is_cold_start:
            # First load
            write_delta_table(metadata_df_silver, SILVER_METADATA_DIR, mode="overwrite")
            _write_rejected(rejected_df, SILVER_METADATA_REJECTED_DIR, exec_date, spark)
            logger.success("Bronze to Silver (Metadata) cold-start pipeline completed successfully.")
            return
        # Incremental load (SCD Type 2)
        target_delta = DeltaTable.forPath(spark, str(SILVER_METADATA_DIR))

        # Legacy cleanup for rows committed before placeholder values were quarantined; a no-op now
        # that such rows never reach changed_or_new.
        target_delta.delete(
            (col("exchange") == "N/A")
            | (col("currency") == "N/A")
            | (col("short_name") == "N/A")
            | (col("sector") == "N/A")
        )

        target_df = target_delta.toDF().filter(col("is_active") == 1)

        incoming_df = metadata_df_silver.alias("incoming")
        existing_active = target_df.alias("existing")

        changed_or_new = (
            incoming_df.join(existing_active, "ticker", "left")
            .filter(
                existing_active.ticker.isNull()
                | (incoming_df.short_name != existing_active.short_name)
                | (incoming_df.sector != existing_active.sector)
                | (incoming_df.industry != existing_active.industry)
                | (incoming_df.country != existing_active.country)
                | (incoming_df.isin != existing_active.isin)
                | (incoming_df.full_time_employees != existing_active.full_time_employees)
                | (incoming_df.exchange != existing_active.exchange)
                | (incoming_df.currency != existing_active.currency)
            )
            .select("incoming.*")
        )

        target_delta.alias("target").merge(
            changed_or_new.alias("source"), "target.ticker = source.ticker AND target.is_active = 1"
        ).whenMatchedUpdate(set={"is_active": lit(0), "end_date": col("source.extraction_date")}).execute()

        write_delta_table(changed_or_new, SILVER_METADATA_DIR, mode="append")
        _write_rejected(rejected_df, SILVER_METADATA_REJECTED_DIR, exec_date, spark)
        logger.success("Bronze to Silver (Metadata) incremental SCD Type 2 pipeline completed successfully")
        return

    except Exception as e:
        logger.exception(f"Failed to process Silver layer metadata: {e}")
        healed = check_and_heal_corrupt_data_file([BRONZE_METADATA_DIR], str(e), spark)
        if healed:
            logger.warning("Corrupted data file detected and Delta table self-healed. Reverted to previous version.")
            if raise_on_error:
                raise RuntimeError(
                    "Corrupted data file detected and Delta table self-healed. Please retry the task."
                ) from e
        if raise_on_error:
            raise e
        sys.exit(1)

    finally:
        spark.stop()


def run_silver_metrics(exec_date: str, raise_on_error: bool = False) -> None:
    """Clean, cast, and deduplicate monthly financial metrics from Bronze to Silver.

    Reads raw stock metadata (which contains financial indicators) from the
    Bronze layer, casts all metrics to their appropriate data types (Decimal for ratios,
    Long for large currency values/counts), filters for the specified execution date,
    keeps only the most recent extraction per ticker for that date, then splits the batch:
    rows breaching a quality rule are written whole to the Silver metrics quarantine table and
    kept out of the Silver metrics table entirely.

    Args:
        exec_date: Execution date in YYYY-MM-DD format.
        raise_on_error: If True, raise errors instead of exiting.

    Raises:
        SystemExit: If the date format is invalid or processing fails.
        RequiredColumnMissingError: If any row is missing ticker or extraction_date.
    """
    try:
        date.fromisoformat(exec_date)
    except ValueError as e:
        logger.error("Invalid date format. Please use YYYY-MM-DD format.")
        if raise_on_error:
            raise e
        sys.exit(1)

    logger.info(f"Starting silver layer processing for stock metrics (execution date: {exec_date})...")

    spark = create_spark_session(raise_on_error=raise_on_error)
    try:
        # Reading bronze metadata
        metadata_df_bronze = read_delta_table(spark, BRONZE_METADATA_DIR)

        # Cast the identity columns first so the essential-column check can run before anything else
        metadata_df_bronze = metadata_df_bronze.withColumn(
            "ticker", upper(trim(col("ticker").cast("string")))
        ).withColumn("extraction_date", col("extraction_date").cast("date"))

        require_columns(metadata_df_bronze, METRICS_REQUIRED_COLUMNS, domain="metrics")

        # Cleaning and organizing the metrics dataframe
        metrics_df_silver = (
            metadata_df_bronze.withColumn("dividend_yield", col("dividend_yield").cast("decimal(10,4)"))
            .withColumn("trailing_pe", col("trailing_pe").cast("decimal(10,4)"))
            .withColumn("market_cap", col("market_cap").try_cast("long"))
            .withColumn("peg_ratio", col("peg_ratio").cast("decimal(10,4)"))
            .withColumn("price_to_book", col("price_to_book").cast("decimal(10,4)"))
            .withColumn("enterprise_to_ebitda", col("enterprise_to_ebitda").cast("decimal(10,4)"))
            .withColumn("enterprise_to_ebit", col("enterprise_to_ebit").cast("decimal(10,4)"))
            .withColumn("book_value", col("book_value").cast("decimal(10,4)"))
            .withColumn("trailing_eps", col("trailing_eps").cast("decimal(10,4)"))
            .withColumn("price_to_sales", col("price_to_sales").cast("decimal(10,4)"))
            .withColumn("operating_margins", col("operating_margins").cast("decimal(10,4)"))
            .withColumn("asset_turnover", col("asset_turnover").cast("decimal(10,4)"))
            .withColumn("shares_outstanding", col("shares_outstanding").try_cast("long"))
            .withColumn("ebitda", col("ebitda").try_cast("long"))
            .withColumn("total_debt", col("total_debt").try_cast("long"))
            .withColumn("total_cash", col("total_cash").try_cast("long"))
            .withColumn("debt_to_equity", col("debt_to_equity").cast("decimal(10,4)"))
            .withColumn("roa", col("roa").cast("decimal(10,4)"))
            .withColumn("roe", col("roe").cast("decimal(10,4)"))
            .withColumn("current_ratio", col("current_ratio").cast("decimal(10,4)"))
            .withColumn("gross_margins", col("gross_margins").cast("decimal(10,4)"))
            .withColumn("ebitda_margins", col("ebitda_margins").cast("decimal(10,4)"))
            .withColumn("profit_margins", col("profit_margins").cast("decimal(10,4)"))
            .withColumn("net_income_to_common", col("net_income_to_common").try_cast("long"))
            .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("timestamp"))
        ).select(
            "ticker",
            "market_cap",
            "dividend_yield",
            "trailing_pe",
            "peg_ratio",
            "price_to_book",
            "enterprise_to_ebitda",
            "enterprise_to_ebit",
            "book_value",
            "trailing_eps",
            "price_to_sales",
            "operating_margins",
            "asset_turnover",
            "shares_outstanding",
            "ebitda",
            "total_debt",
            "total_cash",
            "debt_to_equity",
            "roa",
            "roe",
            "current_ratio",
            "gross_margins",
            "ebitda_margins",
            "profit_margins",
            "net_income_to_common",
            "extraction_date",
            "ingestion_timestamp",
        )

        metrics_df_silver = metrics_df_silver.filter(col("extraction_date") == lit(exec_date).cast("date"))

        # Deduplication: keeping on the most recent row per ticker
        window_spec = Window.partitionBy("ticker", "extraction_date").orderBy(col("ingestion_timestamp").desc())

        metrics_df_silver = (
            metrics_df_silver.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")
        )

        # Split clean rows from rows breaching a quality rule; only clean rows reach Silver (and Gold)
        metrics_df_silver, rejected_df = _split_by_quality(metrics_df_silver, metrics_rules(), exec_date)

        is_cold_start = not (SILVER_METRICS_DIR / "_delta_log").exists()

        if is_cold_start:
            write_delta_table(metrics_df_silver, SILVER_METRICS_DIR, mode="overwrite")
        else:
            target_delta = DeltaTable.forPath(spark, str(SILVER_METRICS_DIR))
            target_delta.delete(col("extraction_date") == lit(exec_date).cast("date"))
            write_delta_table(metrics_df_silver, SILVER_METRICS_DIR, mode="append")

        _write_rejected(rejected_df, SILVER_METRICS_REJECTED_DIR, exec_date, spark)

        logger.success("Bronze to Silver (Metrics) pipeline completed successfully")
        return

    except Exception as e:
        logger.exception(f"Failed to process Silver metrics: {e}")
        healed = check_and_heal_corrupt_data_file([BRONZE_METADATA_DIR], str(e), spark)
        if healed:
            logger.warning("Corrupted data file detected and Delta table self-healed. Reverted to previous version.")
            if raise_on_error:
                raise RuntimeError(
                    "Corrupted data file detected and Delta table self-healed. Please retry the task."
                ) from e
        if raise_on_error:
            raise e
        sys.exit(1)

    finally:
        spark.stop()


def main() -> None:
    """CLI entrypoint to run Silver metadata and metrics pipelines.

    Parses command line arguments and triggers processing functions for the
    metadata dimension and metrics fact tables.
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
        choices=["metadata", "metrics", "all"],
        help="Select which table to run (metadata, metrics, or all)",
    )
    args, _ = parser.parse_known_args()

    if args.table in ("metadata", "all"):
        run_silver_metadata(args.date)
    if args.table in ("metrics", "all"):
        run_silver_metrics(args.date)


if __name__ == "__main__":
    main()
