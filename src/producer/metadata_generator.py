import argparse
import sys
import time
from datetime import date

import pandas as pd
import yfinance as yf

from src.producer.config import LANDING_METADATA_DIR
from src.producer.tickers import get_all_tickers
from src.utils.logger import logger


def run_metadata_generator(exec_date: str | None = None, tickers: list[str] | None = None) -> None:
    """Extract company metadata from yFinance and persist to the Landing zone.

    Downloads and parses detailed stock metadata (such as profile, sector,
    industry, and financial metrics) using yfinance, and saves it as a compressed
    Parquet file.

    Args:
        exec_date: Optional execution date in YYYY-MM-DD format. Defaults to
            today's date.
        tickers: Optional list of tickers to download metadata for. If not
            provided, downloads metadata for all configured tickers.

    Raises:
        SystemExit: If no metadata records are retrieved or if saving the
            parquet file fails.
    """
    if exec_date is None:
        exec_date = date.today().isoformat()
    # Grab tickers from the dictionary if not provided
    if not tickers:
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
                "trailing_pe": info.get("trailingPE", 0.0),
                "peg_ratio": info.get("pegRatio", 0.0),
                "price_to_book": info.get("priceToBook", 0.0),
                "enterprise_to_ebitda": info.get("enterpriseToEbitda", 0.0),
                "enterprise_to_ebit": info.get("enterpriseToEbit", 0.0),
                "book_value": info.get("bookValue", 0.0),
                "trailing_eps": info.get("trailingEps", 0.0),
                "price_to_sales": info.get("priceToSalesTrailing12Months", 0.0),
                "operating_margins": info.get("operatingMargins", 0.0),
                "asset_turnover": info.get("assetTurnover", 0.0),
                "shares_outstanding": info.get("sharesOutstanding", 0),
                "ebitda": info.get("ebitda", 0),
                "total_debt": info.get("totalDebt", 0),
                "total_cash": info.get("totalCash", 0),
                "debt_to_equity": info.get("debtToEquity", 0.0),
                "roa": info.get("returnOnAssets", 0.0),
                "roe": info.get("returnOnEquity", 0.0),
                "current_ratio": info.get("currentRatio", 0.0),
                "gross_margins": info.get("grossMargins", 0.0),
                "ebitda_margins": info.get("ebitdaMargins", 0.0),
                "profit_margins": info.get("profitMargins", 0.0),
                "net_income_to_common": info.get("netIncomeToCommon", 0),
                "extraction_date": exec_date,
            }

            metadata_records.append(record)
            time.sleep(0.1)
        except Exception as e:
            logger.warning(f"Error grabbing data from ticker {t}: {e}")

    logger.info(f"Successfully retrieved metadata for {len(metadata_records)}/{len(tickers)} tickers.")

    if not metadata_records:
        logger.warning("No metadata records were successfully retrieved. Exiting.")
        sys.exit(0)

    df_metadata = pd.DataFrame(metadata_records)
    metadata_path = LANDING_METADATA_DIR / f"ticker_metadata_{exec_date}.parquet"

    try:
        # Save the dataframe in parquet format
        df_metadata.to_parquet(metadata_path, index=False, compression="snappy")
        logger.success(f"✅ Metadata successfully saved to {metadata_path}")
    except Exception:
        logger.opt(exception=True).critical(f"Failed to write parquet file: {metadata_path}")
        sys.exit(1)


def main() -> None:
    """Main entry point to execute the metadata generator CLI.

    Parses CLI arguments for the target date, validates the date format,
    and runs the metadata generator.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date", type=str, default=date.today().isoformat(), help="Date to download metadata (format: YYYY-MM-DD)"
    )
    args = parser.parse_args()

    try:
        date.fromisoformat(args.date)
        run_metadata_generator(args.date)
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD format.")
        sys.exit(1)


if __name__ == "__main__":
    main()
