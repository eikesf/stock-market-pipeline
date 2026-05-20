import os
import glob
import shutil
from src.streaming.spark_session import create_spark_session
from src.streaming.utils import write_delta_table
from pyspark.sql.functions import current_timestamp
from src.producer.config import BRONZE_METADATA_DIR, LANDING_METADATA_DIR, ARCHIVE_METADATA_DIR

# Creating spark session
spark = create_spark_session()

try:
    # Reading landing metadata
    metadata_df_raw = spark.read \
        .format("parquet") \
        .load(str(LANDING_METADATA_DIR)) \
        .withColumn("ingestion_timestamp", current_timestamp())
except Exception as e:
    print(f"Error reading landing metadata (folder might be empty): {e}")
    exit(0)

# Write metadata to bronze delta table
write_delta_table(metadata_df_raw, BRONZE_METADATA_DIR, mode="append")

# Deleting all files in landing folder
landing_files = glob.glob(str(LANDING_METADATA_DIR / "*.parquet"))

for f in landing_files:
    shutil.move(f, str(ARCHIVE_METADATA_DIR))

print(f"Moved {len(landing_files)} files from landing folder to archive folder.")

spark.stop()
