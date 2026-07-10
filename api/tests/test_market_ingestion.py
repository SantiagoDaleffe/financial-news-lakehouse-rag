import os
import sys
import pytest
import pandas as pd
from unittest.mock import MagicMock
from datetime import datetime
import pytz
from cron_jobs.run_market_data import fetch_daily_market_data

def test_fetch_daily_market_data_cleaning_and_cutoff(monkeypatch):
    """Validate cleaning and cutoff logic for daily market data ingestion.

    The test simulates raw market data from yfinance, including an incomplete last
    trading day and an excluded ticker symbol. It verifies that the function
    filters out the incomplete latest date and non-standard ticker names before
    saving the cleaned dataset.
    """

    dates = pd.to_datetime(["2026-07-08", "2026-07-09"])
    spy_data = {
        "Open": [500, 505],
        "High": [505, 510],
        "Low": [495, 500],
        "Close": [502, 508],
        "Volume": [1000, 2000],
    }
    qqq_data = {
        "Open": [400, 405],
        "High": [405, 410],
        "Low": [395, 400],
        "Close": [None, 408],
        "Volume": [1000, 2000],
    }
    vix_data = {
        "Open": [15, 16],
        "High": [16, 17],
        "Low": [14, 15],
        "Close": [15.5, 16.5],
        "Volume": [0, 0],
    }

    mock_raw_df = pd.concat(
        {
            "SPY": pd.DataFrame(spy_data, index=dates),
            "QQQ": pd.DataFrame(qqq_data, index=dates),
            "^VIX": pd.DataFrame(vix_data, index=dates),
        },
        axis=1,
    )
    mock_raw_df.index.name = "Date"
    monkeypatch.setattr(
        "cron_jobs.run_market_data.yf.download", MagicMock(return_value=mock_raw_df)
    )
    monkeypatch.setattr(
        "cron_jobs.run_market_data.TICKERS_UNIVERSE", ["SPY", "QQQ", "^VIX"]
    )

    class FakeDatetime:
        @classmethod
        def now(cls, tz=None):
            return datetime(
                2026, 7, 9, 10, 0, 0, tzinfo=pytz.timezone("America/New_York")
            )

    monkeypatch.setattr("cron_jobs.run_market_data.datetime", FakeDatetime)
    monkeypatch.setenv("DATABASE_URL", "postgresql://fake:fake@localhost/db")
    monkeypatch.setattr("cron_jobs.run_market_data.create_engine", MagicMock())
    captured_dfs = []

    def mock_to_sql(self, name, con, **kwargs):
        captured_dfs.append(self)

    monkeypatch.setattr("pandas.DataFrame.to_sql", mock_to_sql)
    fetch_daily_market_data()

    assert len(captured_dfs) == 1, "The script did not attempt to save to the database."
    final_df = captured_dfs[0]

    saved_dates = final_df["date"].astype(str).unique()
    assert "2026-07-09" not in saved_dates
    assert "2026-07-08" in saved_dates

    saved_tickers = final_df["ticker"].unique()
    assert "SPY" in saved_tickers
    assert "VIX" in saved_tickers
    assert "^VIX" not in saved_tickers
    assert "QQQ" not in saved_tickers

    assert len(final_df) == 2
