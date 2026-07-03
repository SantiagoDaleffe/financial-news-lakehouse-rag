import yfinance as yf
import numpy as np
import pandas as pd

def add_vix_features(base_df: pd.DataFrame, start_date: str = "2020-01-01", end_date: str = "2026-06-27") -> pd.DataFrame:
    """
    Fetches the CBOE Volatility Index (VIX) and computes its log returns to append as a macro feature.

    The VIX represents the market's expectation of 30-day forward-looking volatility. 
    Instead of absolute levels, we compute the momentum of fear using log returns:
    $$R_t = \ln(P_t / P_{t-1})$$

    Args:
        base_df (pd.DataFrame): The primary dataframe containing asset prices. Must have a DatetimeIndex.
        start_date (str, optional): Start date for Yahoo Finance fetch. Defaults to "2020-01-01".
        end_date (str, optional): End date for Yahoo Finance fetch. Defaults to "2026-06-01".

    Returns:
        pd.DataFrame: The merged dataframe containing original features plus 'vix_close' and 'vix_return'.
    """
    print("Downloading ^VIX data...")
    raw_vix = yf.download("^VIX", start=start_date, end=end_date)
    
    vix_df = raw_vix[['Close']].copy()
    vix_df.columns = ['vix_close']
    vix_df.index.name = 'date'
    
    # Calculate fear momentum (Log returns)
    vix_df['vix_return'] = np.log(vix_df['vix_close'] / vix_df['vix_close'].shift(1))
    
    # Left merge ensures we don't lose primary asset data if VIX data is missing for a specific day
    merged_df = base_df.merge(vix_df, left_index=True, right_index=True, how='left')
    
    return merged_df