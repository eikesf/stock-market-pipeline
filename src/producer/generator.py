import yfinance as yf
import pandas as pd
from src.producer.tickers import ALL_TICKERS
from src.producer.config import LANDING_PRICES_DIR
from datetime import datetime

# Grab all tickers by flattening the dictionary
tickers = [ticker for exchange_tickers in ALL_TICKERS.values() for ticker in exchange_tickers]

print(f"Downloading data for {len(tickers)} tickers...")
data = yf.download(tickers=tickers, period='5d', interval='1d', actions=True, auto_adjust=False)

if data.empty:
    print("No data returned today (possible holiday or weekend). Exiting cleanly.")
    exit(0)

if isinstance(data.columns, pd.MultiIndex):
    tickers_long = data.stack(level=1, future_stack=True).reset_index()
    tickers_long.columns = [c.lower().replace(' ', '_') for c in tickers_long.columns]
    tickers_long = tickers_long.rename(columns={'level_1': 'ticker'})
else:
    tickers_long = data.reset_index()
    tickers_long.columns = [c.lower().replace(' ', '_') for c in tickers_long.columns]
    tickers_long['ticker'] = tickers[0]

for col in ['adj_close', 'dividends', 'stock_splits']:
    if col not in tickers_long.columns:
        tickers_long[col] = 0.0

if 'adj_close' not in tickers_long.columns:
    tickers_long['adj_close'] = tickers_long['close']

tickers_long = tickers_long[['date', 'ticker', 'open', 'high', 'low', 'close', 'adj_close', 'volume', 'dividends', 'stock_splits']]

# Keep only the latest available trading day for each ticker to prevent Bronze storage bloat
tickers_long = tickers_long.sort_values('date').groupby('ticker').tail(1)

# Get the latest stock date received
tickers_long['date'] = pd.to_datetime(tickers_long['date'])
max_trading_date = tickers_long['date'].max()
trading_date_str = max_trading_date.strftime('%Y-%m-%d')

# Define file name
target_path = f"tickers_{trading_date_str}.parquet"
target_file = LANDING_PRICES_DIR / target_path

# Saving in parquet without pandas index. Coercing timestamps to microseconds for Spark compatibility
tickers_long.to_parquet(target_file, index=False, compression='snappy', engine='pyarrow', coerce_timestamps='us')

print(f"✅ Data successfully saved to: {target_file}")
print(f"Final shape: {tickers_long.shape[0]} rows and {tickers_long.shape[1]} columns.")