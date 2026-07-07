import os
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carga las variables de entorno de tu .env local
load_dotenv()

TICKERS_UNIVERSE = ["SPY", "QQQ", "DIA", "IWM", "GLD", "TLT", "^VIX"]

def backfill_market_data():
    """
    Descarga 5 años de historial (OHLCV) y hace un insert masivo en Postgres.
    Uso estrictamente manual para inicializar la base de datos vacía.
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise EnvironmentError("Falta la variable DATABASE_URL en el entorno o .env")

    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://")

    engine = create_engine(db_url)
    
    print(f"Iniciando descarga histórica (5 años) para {len(TICKERS_UNIVERSE)} activos...", flush=True)

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
            # Alinear nomenclatura con el ETL diario
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
        print(f"Insertando {len(df_final)} registros en la base de datos operativa...", flush=True)
        
        # Limpiamos la tabla en vez de hacer 'replace' para no romper el esquema de SQLAlchemy
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM market_data;"))
            
        df_final.to_sql("market_data", con=engine, if_exists="append", index=False)
        print("Backfill completado con éxito. Sistemas listos para inferencia y RAG.", flush=True)
    else:
        print("No se encontró data histórica para insertar.", flush=True)

if __name__ == "__main__":
    backfill_market_data()