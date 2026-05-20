from src.streaming.utils import read_delta_table, write_delta_table
from src.streaming.spark_session import create_spark_session
from src.producer.config import BRONZE_PRICES_DIR, SILVER_PRICES_DIR
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, upper, round

# Creating spark session
spark = create_spark_session()

# Reading bronze stock data
try:
    stock_df_bronze = read_delta_table(spark, BRONZE_PRICES_DIR)
except Exception as e:
    print(f"Error reading bronze stock data: {e}")
    exit(1)

# Clean data: drop nulls in critical columns and cast columns to appropriate data types
stock_df_silver = stock_df_bronze.na.drop(subset=["open", "close", "high", "low", "volume", "ticker", "date", "ingestion_timestamp", "adj_close"]) \
    .withColumn("date", col("date").cast("date"))\
    .withColumn("ticker", upper(col("ticker").cast("string")))\
    .withColumn("open", col("open").cast("decimal(10,2)"))\
    .withColumn("high", col("high").cast("decimal(10,2)"))\
    .withColumn("low", col("low").cast("decimal(10,2)"))\
    .withColumn("close", col("close").cast("decimal(10,2)"))\
    .withColumn("adj_close", col("adj_close").cast("decimal(10,2)"))\
    .withColumn("volume", col("volume").cast("bigint"))\
    .withColumn("dividends", col("dividends").cast("decimal(10,2)"))\
    .withColumn("stock_splits", col("stock_splits").cast("decimal(10,4)"))\
    .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("timestamp"))

# Define a window to partition data by ticker and date, ordering by the most recent ingestion timestamp
window_spec = Window\
    .partitionBy("ticker","date")\
    .orderBy(col("ingestion_timestamp").desc())

# Deduplicate by keeping only the most recent record (row number 1) for each ticker/date partition
stock_df_silver = stock_df_silver\
    .withColumn("rn", row_number().over(window_spec))\
    .filter(col("rn") == 1)\
    .drop("rn")

# Writing data to silver delta table
write_delta_table(stock_df_silver, SILVER_PRICES_DIR, mode="overwrite")

# Stopping spark session
spark.stop()

print("✅ Bronze to Silver pipeline completed.")