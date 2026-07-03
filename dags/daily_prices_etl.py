from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
import os
from airflow.configuration import conf

TICKERS_UNIVERSE = ["SPY", "QQQ", "DIA", "IWM", "GLD", "TLT", "^VIX"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def fetch_daily_market_data():
    """
    Download the candle chart for the current day (EOD) and insert it into Postgres.
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        db_url = conf.get("database", "sql_alchemy_conn")

    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://")

    engine = create_engine(db_url)

    print(
        f"Looking for the day's closing figures for active {len(TICKERS_UNIVERSE)}",
        flush=True,
    )

    df_raw = yf.download(
        TICKERS_UNIVERSE,
        period="5d",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=False   
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

    df = pd.DataFrame(records_to_insert)

    if not df.empty:
        print("Cleaning up pre-existing records to ensure idempotency", flush=True)

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
            print(f"Deleted {result.rowcount} duplicate/old records.")

        print(f"Inserting {len(df)} fresh records...", flush=True)
        df.to_sql("market_data", con=engine, if_exists="append", index=False)

        print("Daily intake completed.", flush=True)
    else:
        print("The market is closed or there is no new data today.", flush=True)


with DAG(
    dag_id="daily_market_data_etl",
    default_args=default_args,
    description="Daily EOD data ingestion",
    # 21:00 UTC post ny close
    schedule="0 21 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "daily", "market_data"],
) as dag:
    daily_task = PythonOperator(
        task_id="run_daily_ingestion",
        python_callable=fetch_daily_market_data,
    )
