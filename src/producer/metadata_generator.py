import argparse
import sys
import time
from datetime import date

import pandas as pd
import yfinance as yf

from src.producer.config import ARCHIVE_METADATA_DIR, LANDING_METADATA_DIR
from src.producer.tickers import get_all_tickers
from src.utils.logger import logger


def clean_float(val: object, default: float | None = None) -> float | None:
    """Safely convert a value to float, handling infinity and null strings."""
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return float(val)

    val_str = str(val).strip().lower()
    if "inf" in val_str:
        return float("-inf") if "-" in val_str else float("inf")

    if val_str not in ("nan", "n/a", "null", "none", ""):
        try:
            return float(val_str)
        except (ValueError, TypeError):
            pass
    return default


def clean_int(val: object, default: int | None = None) -> int | None:
    """Safely convert a value to integer, handling null strings."""
    if val is None:
        return default
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)

    val_str = str(val).strip().lower()
    if val_str not in ("infinity", "inf", "-infinity", "-inf", "nan", "n/a", "null", "none", ""):
        try:
            return int(float(val_str))
        except (ValueError, TypeError):
            pass
    return default


def cast_int_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Cast integer columns to pandas Int64 to support nulls while preserving types."""
    int_cols = [
        "full_time_employees",
        "shares_outstanding",
        "market_cap",
        "ebitda",
        "total_debt",
        "total_cash",
        "net_income_to_common",
    ]
    for col_name in int_cols:
        if col_name in df.columns:
            df[col_name] = df[col_name].astype("Int64")
    return df


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

    # Check if a file for the execution date already exists in landing or archive directory
    target_path = f"ticker_metadata_{exec_date}.parquet"
    in_landing = (LANDING_METADATA_DIR / target_path).exists()
    in_archive = (ARCHIVE_METADATA_DIR / target_path).exists()

    if in_landing or in_archive:
        location = "landing" if in_landing else "archive"
        logger.info(f"Metadata file for {exec_date} already exists in {location}. Skipping download.")
        return

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
                "short_name": str(info.get("shortName") or "N/A"),
                "sector": str(info.get("sector") or "N/A"),
                "industry": str(info.get("industry") or "N/A"),
                "country": str(info.get("country") or "N/A"),
                "isin": str(info.get("isin") or "N/A"),
                "full_time_employees": clean_int(info.get("fullTimeEmployees")),
                "exchange": str(info.get("exchange") or "N/A"),
                "market_cap": clean_int(info.get("marketCap")),
                "currency": str(info.get("currency") or "N/A"),
                "dividend_yield": clean_float(info.get("dividendYield")),
                "trailing_pe": clean_float(info.get("trailingPE")),
                "peg_ratio": clean_float(info.get("pegRatio")),
                "price_to_book": clean_float(info.get("priceToBook")),
                "enterprise_to_ebitda": clean_float(info.get("enterpriseToEbitda")),
                "enterprise_to_ebit": clean_float(info.get("enterpriseToEbit")),
                "book_value": clean_float(info.get("bookValue")),
                "trailing_eps": clean_float(info.get("trailingEps")),
                "price_to_sales": clean_float(info.get("priceToSalesTrailing12Months")),
                "operating_margins": clean_float(info.get("operatingMargins")),
                "asset_turnover": clean_float(info.get("assetTurnover")),
                "shares_outstanding": clean_int(info.get("sharesOutstanding")),
                "ebitda": clean_int(info.get("ebitda")),
                "total_debt": clean_int(info.get("totalDebt")),
                "total_cash": clean_int(info.get("totalCash")),
                "debt_to_equity": clean_float(info.get("debtToEquity")),
                "roa": clean_float(info.get("returnOnAssets")),
                "roe": clean_float(info.get("returnOnEquity")),
                "current_ratio": clean_float(info.get("currentRatio")),
                "gross_margins": clean_float(info.get("grossMargins")),
                "ebitda_margins": clean_float(info.get("ebitdaMargins")),
                "profit_margins": clean_float(info.get("profitMargins")),
                "net_income_to_common": clean_int(info.get("netIncomeToCommon")),
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
    df_metadata = cast_int_columns(df_metadata)
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
