import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime, timedelta
import pytz
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

TICKERS_UNIVERSE = ["SPY", "QQQ", "DIA", "IWM", "GLD", "TLT", "^VIX"]


def fetch_daily_market_data():
    """Fetch daily market data for specified tickers and store in database.

    Downloads 5 days of historical OHLCV data from Yahoo Finance for a predefined
    universe of tickers. Filters data based on market hours (NYC timezone) and
    removes existing records before inserting new ones to avoid duplicates.

    Raises:
        ValueError: If DATABASE_URL environment variable is not set.
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("CRITICAL: DATABASE_URL is missing.")
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://")

    engine = create_engine(db_url)

    ny_tz = pytz.timezone("America/New_York")
    ny_time = datetime.now(ny_tz)

    if ny_time.hour < 16:
        cutoff_date = ny_time.date() - timedelta(days=1)
        logging.info(f"Market open in NY. Forcing candle cut-off to: {cutoff_date}")
    else:
        cutoff_date = ny_time.date()
        logging.info(f"Market closed in NY. Taking candles up to today: {cutoff_date}")

    df_raw = yf.download(
        TICKERS_UNIVERSE,
        period="5d",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=False,
    )

    records_to_insert = []

    for ticker in TICKERS_UNIVERSE:
        df_ticker = (
            df_raw.copy() if len(TICKERS_UNIVERSE) == 1 else df_raw[ticker].copy()
        )
        df_ticker.reset_index(inplace=True)

        df_ticker = df_ticker.dropna(subset=["Close", "Date"])
        df_ticker["Date"] = pd.to_datetime(df_ticker["Date"]).dt.date

        df_ticker = df_ticker[df_ticker["Date"] <= cutoff_date]

        if ticker != "^VIX":
            df_ticker = df_ticker[df_ticker["Volume"] > 0]

        for _, row in df_ticker.iterrows():
            clean_ticker = "VIX" if ticker == "^VIX" else ticker
            records_to_insert.append(
                {
                    "ticker": clean_ticker,
                    "date": row["Date"],
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": float(row["Volume"]),
                    "adj_close": float(row["Adj Close"])
                    if "Adj Close" in row
                    else float(row["Close"]),
                }
            )

    df = pd.DataFrame(records_to_insert)

    if not df.empty:
        tickers = tuple(df["ticker"].unique())
        min_dt = df["date"].min()
        max_dt = df["date"].max()

        with engine.begin() as conn:
            delete_query = text("""
                DELETE FROM market_data 
                WHERE ticker IN :tickers 
                AND date >= :f_min AND date <= :f_max
            """)
            result = conn.execute(
                delete_query, {"tickers": tickers, "f_min": min_dt, "f_max": max_dt}
            )
            logging.info(
                f"{result.rowcount} old records deleted from market_data table for tickers {tickers} between {min_dt} and {max_dt}."
            )

        df.to_sql("market_data", con=engine, if_exists="append", index=False)
        logging.info(f"Saved {len(df)} records to market_data table.")
    else:
        logging.info("No new data to process today.")


if __name__ == "__main__":
    fetch_daily_market_data()
