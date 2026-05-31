import yfinance as yf
import pandas as pd
import time
from src.producer.tickers import get_all_tickers
from src.producer.config import LANDING_METADATA_DIR
from src.utils.logger import logger
from datetime import datetime

def run_metadata_generator():
    # Grab tickers from the dictionary
    tickers = [ticker for exchange_tickers in get_all_tickers().values() for ticker in exchange_tickers]

    # Empty to store all data
    metadata_records = []

    # Loop through all tickers to grab their information calling the API
    logger.info(f"Searching metadata for {len(tickers)} tickers.")
    for i, t in enumerate(tickers, 1):
        try:
            if i % 10 == 0:
                logger.info(f"Processing {i}/{len(tickers)}...")
                
            # Access Yahoo API for a specific ticker
            stock = yf.Ticker(t)
            info = stock.info

            record = {
                "ticker": t,
                "short_name": info.get("shortName", "N/A"),
                "sector": info.get("sector", "N/A"),
                "industry": info.get("industry", "N/A"),
                "country": info.get("country", "N/A"),
                "isin": info.get("isin", "N/A"),
                "full_time_employees": info.get("fullTimeEmployees", 0),
                "exchange": info.get("exchange", "N/A"),
                "market_cap": info.get("marketCap", 0),
                "currency": info.get("currency", "N/A"),
                "dividend_yield": info.get("dividendYield", 0.0),
                "extraction_date": datetime.now().strftime('%Y-%m-%d')
            }

            metadata_records.append(record)
            time.sleep(0.1)
        except Exception as e:
            logger.warning(f"Error grabbing data from ticker {t}: {e}")

    logger.info(f"Successfully retrieved metadata for {len(metadata_records)}/{len(tickers)} tickers.")

    if not metadata_records:
        logger.warning("No metadata records were successfully retrieved. Exiting.")
        exit(0)

    df_metadata = pd.DataFrame(metadata_records)
    metadata_path = LANDING_METADATA_DIR / f"ticker_metadata_{datetime.now().strftime('%Y-%m-%d')}.parquet"

    try:
        # Save the dataframe in parquet format
        df_metadata.to_parquet(metadata_path, index=False, compression="snappy")
        logger.success(f"✅ Metadata successfully saved to {metadata_path}")
    except Exception:
        logger.opt(exception=True).critical(f"Failed to write parquet file: {metadata_path}")
        exit(1)

if __name__ == "__main__":
    run_metadata_generator()