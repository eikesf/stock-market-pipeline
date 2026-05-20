import io
import json
import requests
import pandas as pd
from pathlib import Path

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


# ── NASDAQ-100 ────────────────────────────────────────────────────────────────
nasdaq100_tickers = _fetch_table(
    url="https://en.wikipedia.org/wiki/Nasdaq-100",
    table_id="constituents",
    ticker_col="Ticker",
)

# ── S&P 500 → NYSE-only (removes tickers already in NASDAQ-100) ───────────────
sp500_tickers = _fetch_table(
    url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
    table_id="constituents",
)
nyse_tickers = list(set(sp500_tickers) - set(nasdaq100_tickers))

# ── B3 — Ibovespa Proxy (Top 90 most liquid B3 assets via Brapi) ───────────────
_b3_url = "https://brapi.dev/api/quote/list?sortBy=volume&sortOrder=desc&limit=90"
try:
    _b3_data = requests.get(_b3_url, timeout=15).json()
    _b3_tickers_raw = [t["stock"] for t in _b3_data.get("stocks", [])]
except Exception:
    # Safe fallback if API is unreachable
    _b3_tickers_raw = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3", "BBAS3", "B3SA3", "WEGE3"]

# Add the .SA suffix for Yahoo Finance
b3_tickers = [f"{t}.SA" for t in _b3_tickers_raw]

# ── Unified dict consumed by generator.py ────────────────────────────────────
ALL_TICKERS: dict[str, list[str]] = {
    # Yahoo Finance expects BRK-B instead of BRK.B
    "NASDAQ": [t.replace(".", "-") for t in nasdaq100_tickers],
    "NYSE":   [t.replace(".", "-") for t in nyse_tickers],
    "B3":     b3_tickers,
}


if __name__ == "__main__":
    for exchange, tickers in ALL_TICKERS.items():
        print(f"{exchange}: {len(tickers)} tickers — {tickers[:5]}...")