from src.streaming.utils import read_delta_table, write_delta_table
from src.streaming.spark_session import create_spark_session
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, upper, round, when
from src.producer.config import BRONZE_METADATA_DIR, SILVER_METADATA_DIR

# Creating spark session
spark = create_spark_session()

# Reading bronze metadata
try:
    metadata_df_bronze = read_delta_table(spark, BRONZE_METADATA_DIR)
except Exception as e:
    print(f"Error reading bronze metadata: {e}")
    exit(1)

# Cleaning and organizing the metadata dataframe
metadata_df_silver = metadata_df_bronze.na.drop(subset=["ticker", "sector", "exchange", "short_name"])\
    .withColumn("ticker", upper(col("ticker").cast("string")))\
    .withColumn("short_name", col("short_name").cast("string"))\
    .withColumn("sector", col("sector").cast("string"))\
    .withColumn("industry", col("industry").cast("string"))\
    .withColumn("country", col("country").cast("string"))\
    .withColumn("isin", col("isin").cast("string"))\
    .withColumn("full_time_employees", col("full_time_employees").cast("integer"))\
    .withColumn("exchange", col("exchange").cast("string"))\
    .withColumn("market_cap", col("market_cap").cast("bigint"))\
    .withColumn("currency", col("currency").cast("string"))\
    .withColumn("dividend_yield", col("dividend_yield").cast("decimal(10,2)"))\
    .withColumn("extraction_date", col("extraction_date").cast("date"))\
    .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("timestamp"))

# Adjusting the exchange names to correspond to the pattern
metadata_df_silver = metadata_df_silver.withColumn("exchange",
    when(col("exchange") == "SAO", "B3")
    .when(col("exchange") == "NYQ", "NYSE")
    .when(col("exchange").isin("NMS", "NGM"), "NASDAQ")
    .otherwise(col("exchange"))
)

# Deduplication: Keeping only the most recent row per ticker
window_spec = Window.partitionBy("ticker").orderBy(col("ingestion_timestamp").desc())

metadata_df_silver = metadata_df_silver\
    .withColumn("rn", row_number().over(window_spec))\
    .filter(col("rn") == 1)\
    .drop("rn")

# Writing data to silver delta table
write_delta_table(metadata_df_silver, SILVER_METADATA_DIR, mode="overwrite")

# Stopping spark session
spark.stop()

print("✅ Bronze to Silver (Metadata) pipeline completed.")