from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.producer.metadata_generator import run_metadata_generator


@patch("src.producer.metadata_generator.get_all_tickers")
@patch("src.producer.metadata_generator.yf.Ticker")
@patch("src.producer.metadata_generator.pd.DataFrame.to_parquet")
class TestMetadataGenerator:
    def test_run_metadata_generator_success(self, mock_to_parquet, mock_ticker, mock_get_all_tickers):
        """
        Test that metadata is successfully retrieved, structured, and saved as a Parquet file.
        """
        # Return 10 tickers to trigger the periodic log statements (i % 10 == 0)
        mock_get_all_tickers.return_value = {
            "NASDAQ": ["AAPL", "MSFT", "AMZN", "NVDA", "META", "GOOGL", "TSLA", "AVGO", "PEP"],
            "B3": ["BBSE3.SA"],
        }

        mock_aapl_info = {
            "shortName": "Apple Inc.",
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "country": "United States",
            "isin": "US0378331005",
            "fullTimeEmployees": 160000,
            "exchange": "NASDAQ",
            "marketCap": 2600000000000,
            "currency": "USD",
            "dividendYield": 0.005,
        }

        mock_bbse3_info = {
            "shortName": "BB Seguridade Participacoes SA",
            "sector": "Financials",
            "industry": "Insurance - Diversified",
            "country": "Brazil",
            "isin": "BRBBSEACNOR1",
            "fullTimeEmployees": 3000,
            "exchange": "B3",
            "marketCap": 50000000000,
            "currency": "BRL",
            "dividendYield": 0.03,
        }

        mock_aapl = MagicMock()
        mock_aapl.info = mock_aapl_info

        mock_bbse3 = MagicMock()
        mock_bbse3.info = mock_bbse3_info

        # Fallback to returning mock_aapl for the other dummy tickers
        def ticker_side_effect(x):
            if x == "BBSE3.SA":
                return mock_bbse3
            return mock_aapl

        mock_ticker.side_effect = ticker_side_effect

        df_final = None

        def get_df(df_self, *args, **kwargs):
            nonlocal df_final
            df_final = df_self

        with patch("pandas.DataFrame.to_parquet", get_df):
            run_metadata_generator()

        assert df_final is not None
        assert df_final.shape[0] == 10
        assert set(df_final["ticker"]) == {
            "AAPL",
            "MSFT",
            "AMZN",
            "NVDA",
            "META",
            "GOOGL",
            "TSLA",
            "AVGO",
            "PEP",
            "BBSE3.SA",
        }

        aapl_row = df_final[df_final["ticker"] == "AAPL"].iloc[0]
        assert aapl_row["short_name"] == "Apple Inc."
        assert aapl_row["sector"] == "Technology"
        assert aapl_row["full_time_employees"] == 160000
        assert aapl_row["market_cap"] == 2600000000000

        bbse_row = df_final[df_final["ticker"] == "BBSE3.SA"].iloc[0]
        assert bbse_row["short_name"] == "BB Seguridade Participacoes SA"
        assert bbse_row["sector"] == "Financials"
        assert bbse_row["full_time_employees"] == 3000
        assert bbse_row["market_cap"] == 50000000000

    def test_run_generator_metadata_partial_failure(self, mock_to_parquet, mock_ticker, mock_get_all_tickers):
        """
        Test that if retrieval fails for one ticker, the pipeline continues and saves data for the successful ones.
        """
        mock_get_all_tickers.return_value = {"NASDAQ": ["AAPL"], "B3": ["BBSE3.SA"]}

        mock_aapl = MagicMock()
        mock_aapl.info = {
            "shortName": "Apple Inc.",
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "country": "United States",
            "isin": "US0378331005",
            "fullTimeEmployees": 160000,
            "exchange": "NASDAQ",
            "marketCap": 2600000000000,
            "currency": "USD",
            "dividendYield": 0.005,
        }

        def side_effect_func(ticker):
            if ticker == "AAPL":
                return mock_aapl
            raise Exception("Connection timeout for BBSE3.SA")

        mock_ticker.side_effect = side_effect_func

        df_final = None

        def get_df(df_self, *args, **kwargs):
            nonlocal df_final
            df_final = df_self

        with patch("pandas.DataFrame.to_parquet", get_df):
            run_metadata_generator()

        assert df_final is not None
        assert df_final.shape[0] == 1
        assert df_final["ticker"].iloc[0] == "AAPL"
        assert "BBSE3.SA" not in df_final["ticker"].values

        aapl_row = df_final[df_final["ticker"] == "AAPL"].iloc[0]
        assert aapl_row["short_name"] == "Apple Inc."
        assert aapl_row["sector"] == "Technology"
        assert aapl_row["full_time_employees"] == 160000
        assert aapl_row["market_cap"] == 2600000000000

    def test_run_metadata_generator_empty_tickers(self, mock_to_parquet, mock_ticker, mock_get_all_tickers):
        """
        Test that if the ticker list is empty, the generator exits cleanly with code 0.
        """
        mock_get_all_tickers.return_value = {"NASDAQ": [], "B3": []}

        with pytest.raises(SystemExit) as exc_info:
            run_metadata_generator()

        assert exc_info.value.code == 0
        mock_to_parquet.assert_not_called()

    def test_run_metadata_generator_all_failures(self, mock_to_parquet, mock_ticker, mock_get_all_tickers):
        """
        Test that if all tickers fail to fetch metadata, the generator exits cleanly with code 0.
        """
        mock_get_all_tickers.return_value = {"NASDAQ": ["AAPL"]}
        mock_ticker.side_effect = Exception("API offline")

        with pytest.raises(SystemExit) as exc_info:
            run_metadata_generator()

        assert exc_info.value.code == 0
        mock_to_parquet.assert_not_called()

    def test_run_metadata_generator_missing_fields_fallback(self, mock_to_parquet, mock_ticker, mock_get_all_tickers):
        """
        Test that missing API fields are correctly replaced by their default values (e.g. 'N/A' or 0).
        """
        mock_get_all_tickers.return_value = {"NASDAQ": ["AAPL"]}

        mock_ticker_info = {
            "shortName": "Apple Inc.",
            "industry": "Consumer Electronics",
            "isin": "US0378331005",
            "fullTimeEmployees": 160000,
            "exchange": "NASDAQ",
            "currency": "USD",
        }

        mock_ticker_instance = mock_ticker.return_value
        mock_ticker_instance.info = mock_ticker_info

        df_final = None

        def get_df(df_self, *args, **kwargs):
            nonlocal df_final
            df_final = df_self

        with patch("pandas.DataFrame.to_parquet", get_df):
            run_metadata_generator()

        assert df_final is not None
        assert df_final.shape[0] == 1
        assert df_final["ticker"].iloc[0] == "AAPL"
        assert df_final["country"].iloc[0] == "N/A"
        assert df_final["sector"].iloc[0] == "N/A"
        assert df_final["market_cap"].iloc[0] == 0
        assert df_final["dividend_yield"].iloc[0] == 0.0
        assert df_final["extraction_date"].iloc[0] == datetime.now().strftime("%Y-%m-%d")

    def test_run_metadata_generator_write_failure(self, mock_to_parquet, mock_ticker, mock_get_all_tickers):
        """
        Test that a parquet write failure is caught and causes the program to exit with code 1.
        """
        mock_get_all_tickers.return_value = {"NASDAQ": ["AAPL"]}

        mock_ticker_info = {
            "shortName": "Apple Inc.",
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "country": "United States",
            "isin": "US0378331005",
            "fullTimeEmployees": 160000,
            "exchange": "NASDAQ",
            "marketCap": 2600000000000,
            "currency": "USD",
            "dividendYield": 0.005,
        }

        mock_ticker_instance = mock_ticker.return_value
        mock_ticker_instance.info = mock_ticker_info

        mock_to_parquet.side_effect = Exception("Disk full")

        with pytest.raises(SystemExit) as exc_info:
            run_metadata_generator()

        assert exc_info.value.code == 1
        mock_to_parquet.assert_called_once()
