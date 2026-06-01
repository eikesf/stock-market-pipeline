import pandas as pd
import yfinance as yf

from src.producer.config import LANDING_PRICES_DIR
from src.producer.tickers import get_all_tickers
from src.utils.logger import logger


def run_generator():
    # Grab all tickers by flattening the dictionary
    tickers = [ticker for exchange_tickers in get_all_tickers().values() for ticker in exchange_tickers]

    if not tickers:
        logger.critical("No tickers found to download. Aborting pipeline.")
        exit(1)

    try:
        logger.info(f"Downloading data for {len(tickers)} tickers...")
        data = yf.download(tickers=tickers, period="5d", interval="1d", actions=True, auto_adjust=False)
    except Exception:
        logger.opt(exception=True).critical("Failed to download from Yahoo Finance. Aborting pipeline.")
        exit(1)

    logger.debug(f"Raw data shape received from yfinance: {data.shape}")

    if data.empty:
        logger.warning("No data returned today (possible holiday or weekend). Exiting cleanly.")
        exit(0)

    if isinstance(data.columns, pd.MultiIndex):
        logger.debug("MultiIndex columns detected - reshaping with stack()")
        tickers_long = data.stack(level=1, future_stack=True).reset_index()
        tickers_long.columns = [c.lower().replace(" ", "_") for c in tickers_long.columns]
        tickers_long = tickers_long.rename(columns={"level_1": "ticker"})
    else:
        logger.debug("Single ticker format detected - using reset_index() path")
        tickers_long = data.reset_index()
        tickers_long.columns = [c.lower().replace(" ", "_") for c in tickers_long.columns]
        tickers_long["ticker"] = tickers[0]

    for col in ["dividends", "stock_splits"]:
        if col not in tickers_long.columns:
            tickers_long[col] = 0.0

    if "adj_close" not in tickers_long.columns:
        logger.warning("Column 'adj_close' not found - falling back to 'close' values")
        tickers_long["adj_close"] = tickers_long["close"]

    # Ensure volume is an integer type (bigint/int64) to match Spark's expected schema
    if "volume" in tickers_long.columns:
        tickers_long["volume"] = tickers_long["volume"].fillna(0).astype("int64")

    tickers_long = tickers_long[
        ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume", "dividends", "stock_splits"]
    ]

    # Keep only the latest available trading day for each ticker to prevent Bronze storage bloat
    tickers_long = tickers_long.sort_values("date").groupby("ticker").tail(1)

    # Get the latest stock date received
    tickers_long["date"] = pd.to_datetime(tickers_long["date"])
    max_trading_date = tickers_long["date"].max()
    trading_date_str = max_trading_date.strftime("%Y-%m-%d")

    logger.info(f"Latest trading date detected: {trading_date_str}")

    # Define file name
    target_path = f"tickers_{trading_date_str}.parquet"
    target_file = LANDING_PRICES_DIR / target_path

    try:
        # Saving in parquet without pandas index. Coercing timestamps to microseconds for Spark compatibility
        logger.info(f"Saving {tickers_long.shape[0]} rows to: {target_file}")
        tickers_long.to_parquet(
            target_file, index=False, compression="snappy", engine="pyarrow", coerce_timestamps="us"
        )
    except Exception:
        logger.opt(exception=True).critical(f"Failed to write parquet file: {target_file}")
        exit(1)

    logger.success(f"Data successfully saved to: {target_file}")
    logger.info(f"Final shape: {tickers_long.shape[0]} rows and {tickers_long.shape[1]} columns.")


if __name__ == "__main__":
    run_generator()
