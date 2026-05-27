import io
import json
import requests
import pandas as pd
from pathlib import Path
from src.utils.logger import logger

# Browser-like headers
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}


def _fetch_table(url: str, table_id: str = None, match: str = None, ticker_col: str = "Symbol") -> list[str]:
    """Fetch an HTML page with browser headers and extract a table by id or matching text."""
    html = requests.get(url, headers=_HEADERS, timeout=15).text
    
    kwargs = {}
    if table_id:
        kwargs["attrs"] = {"id": table_id}
    if match:
        kwargs["match"] = match
        
    try:
        df = pd.read_html(io.StringIO(html), flavor="lxml", **kwargs)[0]
    except Exception:
        df = pd.read_html(io.StringIO(html), **kwargs)[0]
        
    return df[ticker_col].tolist()


# Global cache variables
_cached_tickers = None


def get_all_tickers() -> dict[str, list[str]]:
    """Fetch B3, NASDAQ, and NYSE tickers dynamically with fallbacks and cache the result."""
    global _cached_tickers
    if _cached_tickers is not None:
        return _cached_tickers

    logger.info("Fetching tickers list from external sources...")

    # ── NASDAQ-100 ────────────────────────────────────────────────────────────────
    try:
        nasdaq100_tickers = _fetch_table(
            url="https://en.wikipedia.org/wiki/Nasdaq-100",
            table_id="constituents",
            ticker_col="Ticker",
        )
        logger.debug(f"Fetched {len(nasdaq100_tickers)} NASDAQ-100 tickers from Wikipedia.")
    except Exception as e:
        logger.warning(f"Failed to fetch NASDAQ-100 tickers from Wikipedia ({e}). Using default fallback list.")
        nasdaq100_tickers = ["AAPL", "MSFT", "AMZN", "NVDA", "META", "GOOGL", "TSLA", "AVGO", "PEP", "COST", "ADBE"]

    # ── S&P 500 → NYSE-only (removes tickers already in NASDAQ-100) ───────────────
    try:
        sp500_tickers = _fetch_table(
            url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            table_id="constituents",
        )
        logger.debug(f"Fetched {len(sp500_tickers)} S&P 500 tickers from Wikipedia.")
    except Exception as e:
        logger.warning(f"Failed to fetch S&P 500 tickers from Wikipedia ({e}). Using default fallback list.")
        sp500_tickers = ["KO", "JNJ", "WMT", "PG", "JPM", "V", "MA", "DIS", "HD", "BAC", "XOM", "CVX"]

    nyse_tickers = list(set(sp500_tickers) - set(nasdaq100_tickers))

    # ── B3 — Ibovespa Proxy (Top 90 most liquid B3 assets via Brapi) ───────────────
    _b3_url = "https://brapi.dev/api/quote/list?sortBy=volume&sortOrder=desc&limit=90"
    try:
        _b3_data = requests.get(_b3_url, timeout=15).json()
        _b3_tickers_raw = [t["stock"] for t in _b3_data.get("stocks", [])]
    except Exception as e:
        # Safe fallback if API is unreachable
        logger.warning(f"BRAPI API unreachable {e}. Falling back to default B3 ticker list.")
        _b3_tickers_raw = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3", "BBAS3", "B3SA3", "WEGE3"]

    # Add the .SA suffix for Yahoo Finance
    b3_tickers = [f"{t}.SA" for t in _b3_tickers_raw]

    # ── Unified dict ──────────────────────────────────────────────────────────────
    _cached_tickers = {
        # Yahoo Finance expects BRK-B instead of BRK.B
        "NASDAQ": [t.replace(".", "-") for t in nasdaq100_tickers],
        "NYSE":   [t.replace(".", "-") for t in nyse_tickers],
        "B3":     b3_tickers,
    }
    return _cached_tickers


if __name__ == "__main__":
    for exchange, tickers in get_all_tickers().items():
        logger.info(f"{exchange}: {len(tickers)} tickers — {tickers[:5]}...")