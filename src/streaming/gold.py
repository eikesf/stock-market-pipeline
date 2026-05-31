from src.streaming.utils import read_delta_table, get_clickhouse_client
from src.producer.config import SILVER_PRICES_DIR, SILVER_METADATA_DIR
from src.streaming.spark_session import create_spark_session
from pyspark.sql.functions import col
from src.utils.logger import logger

logger.info("Starting Gold layer processing...")

# Creating spark session
spark = create_spark_session()
try:
    # Reading silver data
    df_prices = read_delta_table(spark, SILVER_PRICES_DIR)
    df_metadata = read_delta_table(spark, SILVER_METADATA_DIR)

    # Selecting stock price columns to match clickhouse schema
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

    # Selecting stock metadata columns to match clickhouse schema
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
    
    # Create staging tables with the exact same schema as production tables
    client.command("CREATE TABLE IF NOT EXISTS stock_market.fact_prices_staging AS stock_market.fact_prices")
    client.command("CREATE TABLE IF NOT EXISTS stock_market.dim_companies_staging AS stock_market.dim_companies")
    
    # Ensure staging tables are empty (in case of a previously crashed run)
    client.command("TRUNCATE TABLE stock_market.fact_prices_staging")
    client.command("TRUNCATE TABLE stock_market.dim_companies_staging")
    
    # Write new data to staging tables
    client.insert_df("stock_market.fact_prices_staging", df_prices_pd)
    client.insert_df("stock_market.dim_companies_staging", df_metadata_pd)
    
    # Atomically swap staging tables with production tables
    client.command("EXCHANGE TABLES stock_market.fact_prices AND stock_market.fact_prices_staging")
    client.command("EXCHANGE TABLES stock_market.dim_companies AND stock_market.dim_companies_staging")
    
    # Clean up staging tables
    client.command("DROP TABLE IF EXISTS stock_market.fact_prices_staging")
    client.command("DROP TABLE IF EXISTS stock_market.dim_companies_staging")
    
    logger.success("Gold layer processing completed successfully.")

except Exception as e:
    logger.exception(f"Failed to process Gold layer: {e}")
    exit(1)
finally:
    # Stopping spark session
    spark.stop()