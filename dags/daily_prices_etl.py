from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@airflow-postgres:5432/airflow")


TICKERS_UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
    "SPY", "QQQ", "DIA", "IWM",
    "BTC-USD", "ETH-USD", "SOL-USD",
    "GLD", "USO"
]

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
    engine = create_engine(DATABASE_URL)
    
    print(f"Looking for the day's closing figures for active {len(TICKERS_UNIVERSE)}...", flush=True)
    
    df_raw = yf.download(TICKERS_UNIVERSE, period="1d", interval="1d", group_by="ticker", auto_adjust=False)
    
    records_to_insert = []
    
    for ticker in TICKERS_UNIVERSE:
        if len(TICKERS_UNIVERSE) == 1:
            df_ticker = df_raw.copy()
        else:
            df_ticker = df_raw[ticker].copy()
            
        df_ticker = df_ticker.dropna(subset=['Close'])
        df_ticker.reset_index(inplace=True)
        
        for _, row in df_ticker.iterrows():
            records_to_insert.append({
                "ticker": ticker,
                "date": row['Date'],
                "open": float(row['Open']),
                "high": float(row['High']),
                "low": float(row['Low']),
                "close": float(row['Close']),
                "volume": float(row['Volume']),
                "adj_close": float(row['Adj Close']) if 'Adj Close' in row else float(row['Close'])
            })

    df_final = pd.DataFrame(records_to_insert)
    
    if not df_final.empty:
        print(f"Inserting {len(df_final)} new records into the database...", flush=True)
        # upsert for prod
        df_final.to_sql("market_data", con=engine, if_exists="append", index=False)
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