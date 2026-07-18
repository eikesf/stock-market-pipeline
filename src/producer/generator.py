import argparse
import sys
from datetime import date, timedelta

import pandas as pd
import yfinance as yf

from src.producer.config import ARCHIVE_PRICES_DIR, LANDING_PRICES_DIR
from src.producer.tickers import get_all_tickers
from src.utils.logger import logger


def _reshape_and_clean_prices(data: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """Reshape, clean, and cast raw yfinance DataFrame to target schema.

    Args:
        data: The raw DataFrame downloaded from yfinance.
        tickers: List of stock tickers.

    Returns:
        A cleaned, reshaped DataFrame containing only the latest trading day per
        ticker, with columns matching the target database schema.
    """
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

    tickers_long["date"] = pd.to_datetime(tickers_long["date"])
    return tickers_long


def run_generator(exec_date: str, tickers: list[str] | None = None, raise_on_error: bool = False) -> None:
    """Extract daily stock prices from yFinance and persist to the Landing zone.

    Downloads price data for the specified execution date, cleans it, and
    saves it as a compressed Parquet file in the Landing zone.

    Args:
        exec_date: Execution date in YYYY-MM-DD format.
        tickers: Optional list of tickers to download. If not provided,
            downloads all configured tickers.

    Raises:
        SystemExit: If the download fails, no tickers are found, or saving the
            parquet file fails.
    """
    # Check if a file for the execution date already exists in landing or archive directory
    target_path = f"tickers_{exec_date}.parquet"
    in_landing = (LANDING_PRICES_DIR / target_path).exists()
    in_archive = (ARCHIVE_PRICES_DIR / target_path).exists()

    if in_landing or in_archive:
        location = "landing" if in_landing else "archive"
        logger.info(f"Prices file for {exec_date} already exists in {location}. Skipping download.")
        return

    # Convert exec_date to a date object to calculate the next day
    exec_date_obj = date.fromisoformat(exec_date)

    # Calculating the end_date
    end_date = exec_date_obj + timedelta(days=1)

    # Grab all tickers by flattening the dictionary if not provided
    if not tickers:
        tickers = [ticker for exchange_tickers in get_all_tickers().values() for ticker in exchange_tickers]

    if not tickers:
        logger.critical("No tickers found to download. Aborting pipeline.")
        if raise_on_error:
            raise ValueError("No tickers found to download.")
        sys.exit(1)

    if "USDBRL=X" not in tickers:
        tickers.append("USDBRL=X")

    try:
        logger.info(f"Downloading data for {len(tickers)} tickers...")
        data = yf.download(
            tickers=tickers,
            start=exec_date,
            end=end_date.isoformat(),
            actions=True,
            auto_adjust=False,
            progress=False,
        )
    except Exception as e:
        logger.opt(exception=True).critical("Failed to download from Yahoo Finance. Aborting pipeline.")
        if raise_on_error:
            raise e
        sys.exit(1)

    logger.debug(f"Raw data shape received from yfinance: {data.shape}")

    if data.empty:
        logger.warning("No data returned today (possible holiday or weekend). Exiting cleanly.")
        if raise_on_error:
            return
        sys.exit(0)

    # Reshape and clean data
    tickers_long = _reshape_and_clean_prices(data, tickers)

    logger.info(f"Running price generator for execution date: {exec_date}")

    # Define file name
    target_path = f"tickers_{exec_date}.parquet"
    target_file = LANDING_PRICES_DIR / target_path

    try:
        # Saving in parquet without pandas index. Coercing timestamps to microseconds for Spark compatibility
        logger.info(f"Saving {tickers_long.shape[0]} rows to: {target_file}")
        tickers_long.to_parquet(
            target_file, index=False, compression="snappy", engine="pyarrow", coerce_timestamps="us"
        )
    except Exception as e:
        logger.opt(exception=True).critical(f"Failed to write parquet file: {target_file}")
        if raise_on_error:
            raise e
        sys.exit(1)

    logger.success(f"Data successfully saved to: {target_file}")
    logger.info(f"Final shape: {tickers_long.shape[0]} rows and {tickers_long.shape[1]} columns.")


def main() -> None:
    """Main entry point to execute the generator CLI.

    Parses CLI arguments for the target date, validates the date format,
    and runs the generator.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date", type=str, default=date.today().isoformat(), help="Date to download data (format: YYYY-MM-DD)"
    )
    args = parser.parse_args()

    try:
        date.fromisoformat(args.date)
        run_generator(args.date)
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD format.")
        sys.exit(1)


if __name__ == "__main__":
    main()
