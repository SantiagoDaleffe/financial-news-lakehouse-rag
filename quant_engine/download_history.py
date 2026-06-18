import yfinance as yf
import numpy as np
import pandas as pd
import os


TICKERS_UNIVERSE = ["SPY", "QQQ", "DIA", "GLD", "TLT", 'IWM']


def download_historical_data(start_date="2020-01-01", end_date="2024-01-01"):

    df_raw = yf.download(
        TICKERS_UNIVERSE, start=start_date, end=end_date, group_by="ticker"
    )

    os.makedirs("quant_engine/data/raw", exist_ok=True)

    for ticker in TICKERS_UNIVERSE:
        print(f"Processing {ticker}")
        try:
            if isinstance(df_raw.columns, pd.MultiIndex):
                if ticker in df_raw.columns.get_level_values(0):
                    df_ticker = df_raw[ticker].copy()
                elif ticker in df_raw.columns.get_level_values(1):
                    df_ticker = df_raw.xs(ticker, level=1, axis=1).copy()
                else:
                    df_ticker = df_raw.copy()
            else:
                df_ticker = df_raw.copy()

            df_ticker = df_ticker.dropna(subset=["Close"])

            if df_ticker.empty:
                print(f"Empty data {ticker}")
                continue

            df_ticker.columns = [
                str(col).lower().replace(" ", "_") for col in df_ticker.columns
            ]
            df_ticker.index.name = "date"

            file_path = f"quant_engine/data/raw/{ticker.lower()}_historical.parquet"
            df_ticker.to_parquet(file_path, engine="pyarrow")

            print(f"{len(df_ticker)} rows in {file_path}")

        except Exception as e:
            print(f"Error {ticker}: {e}")
            



if __name__ == "__main__":
    download_historical_data()

    print("\nSanity check")
    df_check = pd.read_parquet("quant_engine/data/raw/spy_historical.parquet")
    print(df_check.head(3))
    print(df_check.tail(3))
