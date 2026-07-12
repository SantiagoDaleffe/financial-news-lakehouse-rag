import os
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

TICKERS_UNIVERSE = ["SPY", "QQQ", "DIA", "IWM", "GLD", "TLT", "^VIX"]


def backfill_market_data():
    """Download and backfill 5 years of historical market data for major ETFs and indices.

    Fetches OHLCV data from yfinance for the predefined ticker universe (SPY, QQQ, DIA, IWM, GLD, TLT, VIX),
    transforms it into a standardized format, and inserts it into the market_data table in PostgreSQL.
    Clears existing data before insertion to maintain data consistency.

    Raises:
        EnvironmentError: If the DATABASE_URL environment variable is not set or missing.
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise EnvironmentError("Missing DATABASE_URL environment variable.")

    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://")

    engine = create_engine(db_url)

    print(
        f"{len(TICKERS_UNIVERSE)} tickers will be backfilled with 5 years of historical data from yfinance...",
        flush=True,
    )

    df_raw = yf.download(
        TICKERS_UNIVERSE,
        period="5y",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
    )

    records_to_insert = []

    for ticker in TICKERS_UNIVERSE:
        if len(TICKERS_UNIVERSE) == 1:
            df_ticker = df_raw.copy()
        else:
            df_ticker = df_raw[ticker].copy()

        df_ticker = df_ticker.dropna(subset=["Close"])
        df_ticker.reset_index(inplace=True)

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

    df_final = pd.DataFrame(records_to_insert)

    if not df_final.empty:
        print(
            f"{len(df_final)} records will be inserted into the database.",
            flush=True,
        )

        with engine.begin() as conn:
            conn.execute(text("DELETE FROM market_data;"))

        df_final.to_sql("market_data", con=engine, if_exists="append", index=False)
        print(
            "Backfill completed successfully. Systems ready for inference and RAG.",
            flush=True,
        )
    else:
        print("No historical data found to insert.", flush=True)


if __name__ == "__main__":
    backfill_market_data()
