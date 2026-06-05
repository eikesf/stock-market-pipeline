from unittest.mock import patch

import pandas as pd
import pytest

from src.producer.generator import run_generator


@patch("src.producer.generator.get_all_tickers")
@patch("src.producer.generator.yf.download")
@patch("src.producer.generator.pd.DataFrame.to_parquet")
class TestPriceGenerator:
    def test_run_generator_success_multi_index(self, mock_to_parquet, mock_download, mock_get_all_tickers):
        """
        Test that MultiIndex columns returned by yfinance are stacked, cleaned,
        and written to a Parquet file correctly.
        """
        mock_get_all_tickers.return_value = {"NASDAQ": ["AAPL"], "B3": ["PETR4.SA"]}

        columns = pd.MultiIndex.from_tuples(
            [
                ("Open", "AAPL"),
                ("Open", "PETR4.SA"),
                ("Close", "AAPL"),
                ("Close", "PETR4.SA"),
                ("High", "AAPL"),
                ("High", "PETR4.SA"),
                ("Low", "AAPL"),
                ("Low", "PETR4.SA"),
                ("Adj Close", "AAPL"),
                ("Adj Close", "PETR4.SA"),
                ("Volume", "AAPL"),
                ("Volume", "PETR4.SA"),
                ("Dividends", "AAPL"),
                ("Dividends", "PETR4.SA"),
                ("Stock Splits", "AAPL"),
                ("Stock Splits", "PETR4.SA"),
            ]
        )

        data = [[150.0, 30.0, 152.0, 31.0, 153.0, 32.0, 149.0, 29.0, 152.0, 31.0, 1000, 5000, 0.0, 0.0, 0.0, 0.0]]

        mock_df = pd.DataFrame(data, columns=columns, index=pd.to_datetime(["2026-05-26"]))
        mock_df.index.name = "Date"
        mock_download.return_value = mock_df

        run_generator()

        mock_to_parquet.assert_called_once()
        call_args, call_kwargs = mock_to_parquet.call_args

        dest_path = call_args[0]
        assert "tickers_2026-05-26.parquet" in str(dest_path)
        assert call_kwargs["index"] is False
        assert call_kwargs["compression"] == "snappy"
        assert call_kwargs["engine"] == "pyarrow"
        assert call_kwargs["coerce_timestamps"] == "us"

    def test_run_generator_success_single_index(self, mock_to_parquet, mock_download, mock_get_all_tickers):
        """
        Test price generator with a single ticker to verify flat columns logic.
        """
        mock_get_all_tickers.return_value = {"NASDAQ": ["AAPL"]}

        columns = pd.Index(["Open", "Close", "High", "Low", "Adj Close", "Volume", "Dividends", "Stock Splits"])
        data = [[150.0, 152.0, 153.0, 149.0, 152.0, 1000, 0.0, 0.0]]

        mock_df = pd.DataFrame(data, columns=columns, index=pd.to_datetime(["2026-05-26"]))
        mock_df.index.name = "Date"
        mock_download.return_value = mock_df

        run_generator()

        mock_to_parquet.assert_called_once()
        call_args, call_kwargs = mock_to_parquet.call_args

        dest_path = call_args[0]
        assert "tickers_2026-05-26.parquet" in str(dest_path)
        assert call_kwargs["index"] is False
        assert call_kwargs["compression"] == "snappy"
        assert call_kwargs["engine"] == "pyarrow"
        assert call_kwargs["coerce_timestamps"] == "us"

    def test_run_generator_download_error(self, mock_to_parquet, mock_download, mock_get_all_tickers):
        """
        Test that yfinance download exceptions are caught and exit with code 1.
        """
        mock_get_all_tickers.return_value = {"NASDAQ": ["AAPL"], "B3": ["PETR4.SA"]}
        mock_download.side_effect = Exception("Network error")

        with pytest.raises(SystemExit) as exc_info:
            run_generator()

        assert exc_info.value.code == 1
        mock_to_parquet.assert_not_called()

    def test_run_generator_empty_download(self, mock_to_parquet, mock_download, mock_get_all_tickers):
        """
        Test clean exit with code 0 on weekend or holiday empty downloads.
        """
        mock_get_all_tickers.return_value = {"NASDAQ": ["AAPL"], "B3": ["PETR4.SA"]}
        mock_download.return_value = pd.DataFrame()

        with pytest.raises(SystemExit) as exc_info:
            run_generator()

        assert exc_info.value.code == 0
        mock_to_parquet.assert_not_called()

    def test_run_generator_missing_dividends_splits(self, mock_to_parquet, mock_download, mock_get_all_tickers):
        """
        Test that missing 'Dividends' or 'Stock Splits' columns are defaulted to 0.0.
        """
        mock_get_all_tickers.return_value = {"NASDAQ": ["AAPL"]}

        mock_df = pd.DataFrame(
            {
                "Open": [150.0],
                "Close": [152.0],
                "High": [153.0],
                "Low": [149.0],
                "Adj Close": [152.0],
                "Volume": [1000],
            },
            columns=["Open", "Close", "High", "Low", "Adj Close", "Volume"],
            index=pd.to_datetime(["2026-05-26"]),
        )
        mock_df.index.name = "Date"
        mock_download.return_value = mock_df

        df_final = None

        def get_df(df_self, *args, **kwargs):
            nonlocal df_final
            df_final = df_self

        # Intercept DataFrame save to inspect processed columns
        with patch("pandas.DataFrame.to_parquet", get_df):
            run_generator()

        assert df_final is not None
        assert "dividends" in df_final.columns
        assert "stock_splits" in df_final.columns
        assert (df_final["dividends"] == 0.0).all()
        assert (df_final["stock_splits"] == 0.0).all()

    def test_run_generator_missing_adj_close(self, mock_to_parquet, mock_download, mock_get_all_tickers):
        """
        Test fallback logic when 'Adj Close' is missing from downloaded tickers.
        """
        mock_get_all_tickers.return_value = {"NASDAQ": ["AAPL"]}

        mock_df = pd.DataFrame(
            {"Open": [140.0], "Close": [183.45], "High": [185.10], "Low": [181.12], "Volume": [2345678]},
            index=pd.to_datetime(["2025-05-26"]),
        )
        mock_df.index.name = "Date"
        mock_download.return_value = mock_df

        df_final = None

        def get_df(df_self, *args, **kwargs):
            nonlocal df_final
            df_final = df_self

        with patch("pandas.DataFrame.to_parquet", get_df):
            run_generator()

        assert df_final is not None
        assert "adj_close" in df_final.columns
        assert (df_final["adj_close"] == df_final["close"]).all()

    def test_run_generator_volume_cleaning(self, mock_to_parquet, mock_download, mock_get_all_tickers):
        """
        Test that volume NaN/None values are filled with 0 and cast to int64.
        """
        mock_get_all_tickers.return_value = {"NASDAQ": ["AAPL"]}

        mock_df = pd.DataFrame(
            {"Open": [140.0], "Close": [183.45], "High": [185.10], "Low": [181.12], "Volume": None},
            index=pd.to_datetime(["2025-05-26"]),
        )
        mock_df.index.name = "Date"
        mock_download.return_value = mock_df

        df_final = None

        def get_df(df_self, *args, **kwargs):
            nonlocal df_final
            df_final = df_self

        with patch("pandas.DataFrame.to_parquet", get_df):
            run_generator()

        assert df_final is not None
        assert df_final["volume"].dtype == "int64"
        assert (df_final["volume"] == 0).all()

    def test_run_generator_latest_date_filtering(self, mock_to_parquet, mock_download, mock_get_all_tickers):
        """
        Test that only the latest date record for each ticker is kept.
        """
        mock_get_all_tickers.return_value = {"NASDAQ": ["AAPL", "NVDA"]}

        columns = pd.MultiIndex.from_tuples(
            [
                ("Open", "AAPL"),
                ("Open", "NVDA"),
                ("Close", "AAPL"),
                ("Close", "NVDA"),
                ("High", "AAPL"),
                ("High", "NVDA"),
                ("Low", "AAPL"),
                ("Low", "NVDA"),
                ("Adj Close", "AAPL"),
                ("Adj Close", "NVDA"),
                ("Volume", "AAPL"),
                ("Volume", "NVDA"),
            ]
        )

        data = [
            [150.0, 100.0, 152.0, 102.0, 153.0, 103.0, 149.0, 99.0, 152.0, 102.0, 1000, 2000],
            [151.0, 101.0, 153.0, 103.0, 154.0, 104.0, 150.0, 100.0, 151.0, 101.0, 1001, 2001],
            [152.0, 102.0, 154.0, 104.0, 155.0, 105.0, 151.0, 101.0, 152.0, 102.0, 1002, 2002],
            [153.0, 103.0, 155.0, 105.0, 156.0, 106.0, 152.0, 102.0, 153.0, 103.0, 1003, 2003],
        ]

        index = pd.to_datetime(["2026-05-26", "2026-05-27", "2026-05-28", "2026-05-29"])
        mock_df = pd.DataFrame(data, columns=columns, index=index)
        mock_df.index.name = "Date"
        mock_download.return_value = mock_df

        df_final = None

        def get_df(df_self, *args, **kwargs):
            nonlocal df_final
            df_final = df_self

        with patch("pandas.DataFrame.to_parquet", get_df):
            run_generator()

        assert df_final is not None
        assert df_final.shape[0] == 2
        assert set(df_final["ticker"]) == {"AAPL", "NVDA"}
        assert (df_final["date"] == pd.to_datetime("2026-05-29")).all()

        aapl_row = df_final[df_final["ticker"] == "AAPL"].iloc[0]
        assert aapl_row["open"] == 153.0
        assert aapl_row["close"] == 155.0
        assert aapl_row["volume"] == 1003

        nvda_row = df_final[df_final["ticker"] == "NVDA"].iloc[0]
        assert nvda_row["open"] == 103.0
        assert nvda_row["close"] == 105.0
        assert nvda_row["volume"] == 2003

    def test_run_generator_write_failure(self, mock_to_parquet, mock_download, mock_get_all_tickers):
        """
        Test that parquet write failures raise SystemExit with exit code 1.
        """
        mock_get_all_tickers.return_value = {"NASDAQ": ["AAPL"]}

        mock_df = pd.DataFrame(
            {
                "Open": [325.12],
                "Close": [328.92],
                "High": [330.00],
                "Low": [324.50],
                "Adj Close": [328.92],
                "Volume": [12345678],
            },
            index=pd.to_datetime(["2026-05-26"]),
        )
        mock_df.index.name = "Date"
        mock_download.return_value = mock_df

        mock_to_parquet.side_effect = Exception("Write error: Disk full")

        with pytest.raises(SystemExit) as exc_info:
            run_generator()

        assert exc_info.value.code == 1
        mock_to_parquet.assert_called_once()

    def test_run_generator_empty_tickers_list(self, mock_to_parquet, mock_download, mock_get_all_tickers):
        """
        Test that run_generator exits with code 1 if no tickers are configured.
        """
        mock_get_all_tickers.return_value = {"NASDAQ": [], "B3": []}

        with pytest.raises(SystemExit) as exc_info:
            run_generator()

        assert exc_info.value.code == 1
        mock_download.assert_not_called()
