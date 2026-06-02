from unittest.mock import MagicMock, patch

import pytest

from src.producer.tickers import _fetch_table, get_all_tickers


# Clean the cache before the test
@pytest.fixture(autouse=True)
def clear_ticker_cache():
    import src.producer.tickers as tk

    tk._cached_tickers = None


def mock_requests_get(url, *args, **kwargs):
    response_mock = MagicMock()

    if "Nasdaq-100" in url:
        response_mock.text = """
        <table id="constituents">
            <thead><tr><th>Ticker</th></tr></thead>
            <tbody><tr><td>AAPL</td></tr><tr><td>BRK.B</td></tr></tbody>
        </table>
        """
    elif "List_of_S%26P_500_companies" in url or "S%26P_500" in url:
        response_mock.text = """
        <table id="constituents">
            <thead><tr><th>Symbol</th></tr></thead>
            <tbody><tr><td>KO</td></tr></tbody>
        </table>
        """
    else:
        response_mock.json.return_value = {"stocks": [{"stock": "PETR4"}, {"stock": "VALE3"}]}
    return response_mock


@patch("src.producer.tickers.requests.get")
def test_get_all_tickers_success(mock_get):
    mock_get.side_effect = mock_requests_get

    tickers = get_all_tickers()

    assert "NASDAQ" in tickers
    assert "AAPL" in tickers["NASDAQ"]

    assert "BRK-B" in tickers["NASDAQ"]
    assert "BRK.B" not in tickers["NASDAQ"]

    assert "NYSE" in tickers
    assert "KO" in tickers["NYSE"]

    assert "B3" in tickers
    assert "PETR4.SA" in tickers["B3"]
    assert "VALE3.SA" in tickers["B3"]


@patch("src.producer.tickers.requests.get")
def test_get_all_tickers_wikipedia_fails(mock_get):
    mock_get.side_effect = Exception("Connection error/Timeout")

    tickers = get_all_tickers()

    assert "NASDAQ" in tickers
    assert "AAPL" in tickers["NASDAQ"]


@patch("src.producer.tickers.requests.get")
def test_get_all_tickers_caching(mock_get):
    """
    Verify that get_all_tickers caches the fetched data in-memory on the first call,
    returning the exact same object reference and bypassing network requests subsequently.
    """
    mock_get.side_effect = mock_requests_get

    tickers1 = get_all_tickers()
    tickers2 = get_all_tickers()

    # Confirm object identity to ensure the in-memory dictionary is reused without reallocation
    assert tickers1 is tickers2

    # Confirm that subsequent invocations do not trigger further HTTP requests
    initial_calls = mock_get.call_count
    get_all_tickers()
    assert mock_get.call_count == initial_calls


@patch("src.producer.tickers.requests.get")
@patch("src.producer.tickers.pd.read_html")
def test_fetch_table_match_and_flavor_fallback(mock_read_html, mock_get):
    """
    Verify that _fetch_table correctly falls back to Pandas' default parser selection
    when the explicitly requested 'lxml' parser raises an exception.
    """
    mock_response = MagicMock()
    mock_response.text = "<html><body><table><tr><th>Symbol</th></tr><tr><td>MSFT</td></tr></table></body></html>"
    mock_get.return_value = mock_response

    mock_df = MagicMock()
    mock_df.columns = ["Symbol"]
    mock_df.__getitem__.return_value.tolist.return_value = ["MSFT"]

    # Simulate parser recovery: the first call with flavor="lxml" fails,
    # forcing a fallback attempt without flavor restrictions on the second call.
    mock_read_html.side_effect = [Exception("lxml failed"), [mock_df]]

    res = _fetch_table("http://dummy.url", match="constituents")
    assert res == ["MSFT"]
    assert mock_read_html.call_count == 2
