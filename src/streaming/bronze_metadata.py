import glob
import shutil
import os
from src.streaming.spark_session import create_spark_session
from src.streaming.utils import write_delta_table
from pyspark.sql.functions import current_timestamp
from src.producer.config import BRONZE_METADATA_DIR, LANDING_METADATA_DIR, ARCHIVE_METADATA_DIR
from src.utils.logger import logger

# Creating spark session
spark = create_spark_session()

try:
    try:
        # Reading landing metadata
        metadata_df_raw = spark.read \
            .format("parquet") \
            .load(str(LANDING_METADATA_DIR)) \
            .withColumn("ingestion_timestamp", current_timestamp())
    except Exception as e:
        logger.warning(f"Failed to read landing metadata: {e}. Exiting cleanly as folder might be empty.")
        exit(0)

    try:
        # Write metadata to bronze delta table
        write_delta_table(metadata_df_raw, BRONZE_METADATA_DIR, mode="append")

        # Archiving raw files to archive folder
        landing_files = glob.glob(str(LANDING_METADATA_DIR / "*.parquet"))

        for f in landing_files:
            dest_file = ARCHIVE_METADATA_DIR / os.path.basename(f)
            if dest_file.exists():
                dest_file.unlink()  # Prevent collision and shutil.move errors on daily replays
            shutil.move(f, str(ARCHIVE_METADATA_DIR))
            
        logger.info(f"Successfully archived {len(landing_files)} landing files to: {ARCHIVE_METADATA_DIR}")
        logger.success("Bronze (Metadata) pipeline completed successfully.")
        
    except Exception as e:
        logger.exception(f"Failed during Bronze metadata pipeline execution: {e}")
        exit(1)

finally:
    spark.stop()

