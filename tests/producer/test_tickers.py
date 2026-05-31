import pytest
from unittest.mock import patch, MagicMock
from src.producer.tickers import get_all_tickers

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
        response_mock.json.return_value = {
            "stocks": [{"stock":"PETR4"}, {"stock":"VALE3"}]
        }
    return response_mock
        

@patch("src.producer.tickers.requests.get")
def test_get_all_tickers_success(mock_get):
    mock_get.side_effect = mock_requests_get
    
    # Execute the function
    tickers = get_all_tickers()
    
    # Assertions
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