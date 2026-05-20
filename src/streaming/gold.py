from src.streaming.utils import read_delta_table, get_clickhouse_client
from src.producer.config import SILVER_PRICES_DIR, SILVER_METADATA_DIR
from src.streaming.spark_session import create_spark_session
from pyspark.sql.functions import col

# Creating spark session
spark = create_spark_session()

# Reading silver data
try:
    df_prices = read_delta_table(spark, SILVER_PRICES_DIR)
    df_metadata = read_delta_table(spark, SILVER_METADATA_DIR)
except Exception as e:
    print(f"Error reading silver data: {e}")
    exit(1)

# Renaming columns to match clickhouse schema
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
    col("ingestion_timestamp")
)

df_metadata = df_metadata.select(
    col("ticker"),
    col("short_name"),
    col("sector"),
    col("industry"),
    col("country"),
    col("isin"),
    col("full_time_employees"),
    col("exchange"),
    col("market_cap"),
    col("currency"),
    col("dividend_yield"),
    col("extraction_date"),
    col("ingestion_timestamp")
)

# Creating clickhouse connection
client = get_clickhouse_client()

# Convert silver tables to pandas
df_prices_pd = df_prices.toPandas()
df_metadata_pd = df_metadata.toPandas()

# Clearing old data to avoid duplication (Idempotency)
client.command("TRUNCATE TABLE stock_market.fact_prices")
client.command("TRUNCATE TABLE stock_market.dim_companies")

# Writing data to clickhouse
client.insert_df("stock_market.fact_prices", df_prices_pd)
client.insert_df("stock_market.dim_companies", df_metadata_pd)

# Stopping spark session
spark.stop()
print("✅ Silver to Gold pipeline completed.")