import yfinance as yf
import pandas as pd
import time
from src.producer.tickers import ALL_TICKERS
from src.producer.config import LANDING_METADATA_DIR
from datetime import datetime

# Grab tickers from the dictionary
tickers = [ticker for exchange_tickers in ALL_TICKERS.values() for ticker in exchange_tickers]

# Empty to store all data
metadata_records = []

# Loop through all tickers to grab their information calling the API
print(f"Searching metadata for {len(tickers)} tickers.")
for i, t in enumerate(tickers, 1):
    try:
        if i % 10 == 0:
            print(f"Processing {i}/{len(tickers)}...")
            
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
        print(f"Error grabbing data from ticker {t}: {e}")

df_metadata = pd.DataFrame(metadata_records)

# Save the dataframe in parquet format
metadata_path = LANDING_METADATA_DIR / f"ticker_metadata_{datetime.now().strftime('%Y-%m-%d')}.parquet"
df_metadata.to_parquet(metadata_path, index=False, compression="snappy")

print(f"✅ Metadata successfully saved to {metadata_path}")