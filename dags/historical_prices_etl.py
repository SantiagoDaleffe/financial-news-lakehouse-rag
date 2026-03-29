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
    "retry_delay": timedelta(minutes=2),
}

def backfill_market_data():
    """
    Downloads 5 years of history (OHLCV) and performs a bulk insert into Postgres.
    """
    engine = create_engine(DATABASE_URL)
    
    print(f"Starting historical download for active {len(TICKERS_UNIVERSE)}...", flush=True)
    

    df_raw = yf.download(TICKERS_UNIVERSE, period="5y", interval="1d", group_by="ticker", auto_adjust=False)
    
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
        print(f"Inserting {len(df_final)} records into the database...", flush=True)
        df_final.to_sql("market_data", con=engine, if_exists="append", index=False)
        print("Backfill completed successfully.", flush=True)
    else:
        print("No data was found to insert.", flush=True)

with DAG(
    dag_id="historical_market_data_backfill",
    default_args=default_args,
    description="One-time historical data ingestion (5 years)",
    schedule_interval=None, 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "historical", "market_data"],
) as dag:
    
    backfill_task = PythonOperator(
        task_id="run_5y_backfill",
        python_callable=backfill_market_data,
    )