from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
import os
from airflow.configuration import conf

TICKERS_UNIVERSE = ["SPY", "QQQ", "DIA", "IWM", "GLD", "TLT", "^VIX"]

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
    Intended to run manually strictly ONCE upon deployment to initialize the database.
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        db_url = conf.get('database', 'sql_alchemy_conn')

    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://")

    engine = create_engine(db_url)
    
    print(f"Starting historical download (5 years) for {len(TICKERS_UNIVERSE)} assets...", flush=True)

    df_raw = yf.download(
        TICKERS_UNIVERSE, 
        period="5y", 
        interval="1d", 
        group_by="ticker", 
        auto_adjust=False
    )
    
    records_to_insert = []
    
    for ticker in TICKERS_UNIVERSE:
        if len(TICKERS_UNIVERSE) == 1:
            df_ticker = df_raw.copy()
        else:
            df_ticker = df_raw[ticker].copy()
            
        df_ticker = df_ticker.dropna(subset=['Close'])
        df_ticker.reset_index(inplace=True)
        
        for _, row in df_ticker.iterrows():
            # Align naming convention to match the daily ETL
            clean_ticker = "VIX" if ticker == "^VIX" else ticker
            
            records_to_insert.append({
                "ticker": clean_ticker,
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
        print(f"Inserting {len(df_final)} records into the operational database...", flush=True)
        
        # We drop the table and recreate it just in case someone runs the backfill twice by mistake
        df_final.to_sql("market_data", con=engine, if_exists="replace", index=False)
        print("Backfill completed successfully. Core systems ready for daily ingestion.", flush=True)
    else:
        print("No historical data was found to insert.", flush=True)

with DAG(
    dag_id="historical_market_data_backfill",
    default_args=default_args,
    description="One-time historical data ingestion (5 years)",
    schedule=None,  # trigger is strictly manual
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "historical", "setup"],
) as dag:
    
    backfill_task = PythonOperator(
        task_id="run_5y_backfill",
        python_callable=backfill_market_data,
    )