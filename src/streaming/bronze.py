import shutil
import sys

from pyspark.sql.functions import current_timestamp

from src.producer.config import ARCHIVE_PRICES_DIR, BRONZE_PRICES_DIR, LANDING_PRICES_DIR
from src.streaming.spark_session import create_spark_session
from src.streaming.utils import write_delta_table
from src.utils.logger import logger

# Creating spark session
spark = create_spark_session()
try:
    try:
        # Reading landing stock data
        stock_df_raw = (
            spark.read.format("parquet")
            .load(str(LANDING_PRICES_DIR))
            .withColumn("ingestion_timestamp", current_timestamp())
        )
    except Exception as e:
        logger.warning(f"Failed to read landing stock price data: {e}. Exiting cleanly as folder might be empty.")
        sys.exit(0)

    try:
        # Write data to bronze delta table
        write_delta_table(stock_df_raw, BRONZE_PRICES_DIR, mode="append")

        # Archiving raw files to archive folder
        landing_files = list(LANDING_PRICES_DIR.glob("*.parquet"))

        for f in landing_files:
            dest_file = ARCHIVE_PRICES_DIR / f.name
            if dest_file.exists():
                dest_file.unlink()
            shutil.move(str(f), str(ARCHIVE_PRICES_DIR))

        logger.info(f"Successfully archived {len(landing_files)} landing files to: {ARCHIVE_PRICES_DIR}")
        logger.success("Bronze (Prices) pipeline completed successfully.")

    except Exception as e:
        logger.exception(f"Failed during Bronze prices pipeline execution: {e}")
        sys.exit(1)

finally:
    spark.stop()
