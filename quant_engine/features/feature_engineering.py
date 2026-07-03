import numpy as np
import pandas as pd

class TechnicalFeatureEngineer:
    """
    Calculates technical indicators and statistical features for quantitative modeling.
    
    This pipeline transforms raw OHLCV data into stationary, normalized features suitable 
    for gradient boosting architectures, including relative strength against the S&P 500, 
    volatility shocks, and rolling distance to moving averages.
    """

    def __init__(self):
        """Initializes the feature engineer and defines data leakage safeguards."""
        # Columns that represent absolute prices or forward-looking data and must be dropped before training
        self.forbidden_columns = ['open', 'high', 'low', 'adj_close', 'volume', 'vix_close', 'ret_spy']
        
    def transform(self, panel_df: pd.DataFrame, is_inference: bool = False) -> pd.DataFrame:
        """
        Applies feature engineering across all tickers in the panel data.

        Args:
            panel_df (pd.DataFrame): Raw OHLCV data with a 'ticker' column and DatetimeIndex.

        Returns:
            pd.DataFrame: A processed dataframe ready for machine learning consumption.
            is_inference (bool): If True, prevents dropping the latest row missing the target.
        """
        df = panel_df.copy()
        
        # --- 1. MACRO BENCHMARK (SPY) ---
        spy_raw_df = df[df['ticker'] == 'SPY'].copy()
        spy_signals_df = pd.DataFrame({
            'ret_spy_1d': np.log(spy_raw_df['close'] / spy_raw_df['close'].shift(1)),
            'ret_spy_14d': np.log(spy_raw_df['close'] / spy_raw_df['close'].shift(14))
        }, index=spy_raw_df.index)
        
        spy_signals_df = spy_signals_df[~spy_signals_df.index.duplicated(keep='first')]
        df = df.join(spy_signals_df, how='left')
        
        processed_groups = []
        
        for ticker, group in df.groupby('ticker', observed=True):
            group = group.sort_index()
            close_px = group['close']
            high_px = group['high']
            low_px = group['low']
            
            # --- MULTI-WINDOW LOG RETURNS ---
            group['ret_1d'] = np.log(close_px / close_px.shift(1))
            group['ret_3d'] = np.log(close_px / close_px.shift(3))
            group['ret_5d'] = np.log(close_px / close_px.shift(5))
            group['ret_10d'] = np.log(close_px / close_px.shift(10))
            group['ret_20d'] = np.log(close_px / close_px.shift(20))
            group['ret_14d'] = np.log(close_px / close_px.shift(14))
            
            # --- CONTEXT & RELATIVE STRENGTH ---
            group['rel_strength_14d'] = group['ret_14d'] - group['ret_spy_14d']
            
            # Beta calculation: Covariance of asset and market divided by Variance of market
            # $$\beta = \frac{\text{Cov}(R_i, R_m)}{\text{Var}(R_m)}$$
            spy_var = group['ret_spy_1d'].rolling(20).var()
            ts_cov = group['ret_1d'].rolling(20).cov(group['ret_spy_1d'])
            
            group['beta_20d'] = ts_cov / (spy_var + 1e-8)
            group['spy_corr_20d'] = group['ret_1d'].rolling(20).corr(group['ret_spy_1d'])
            
            # --- VOLATILITY & SHOCKS ---
            vol_1d = abs(group['ret_1d'])
            group['vol_20d_std'] = group['ret_1d'].rolling(20).std()
            group['vol_shock_z'] = (vol_1d - vol_1d.rolling(20).mean()) / (group['vol_20d_std'] + 1e-8)
            
            # Price Acceleration (Second Derivative of price)
            group['price_accel_1d'] = group['ret_1d'] - group['ret_1d'].shift(1)
            
            if 'vix_return' in group.columns:
                group['vix_divergence'] = group['ret_1d'] * group['vix_return']
                
            # --- MOVING AVERAGES (Normalized Distance) ---
            for period in [9, 20, 50, 200]:
                sma = close_px.rolling(period).mean()
                ema = close_px.ewm(span=period, adjust=False).mean()
                group[f'dist_sma_{period}'] = (close_px - sma) / sma
                group[f'dist_ema_{period}'] = (close_px - ema) / ema
                
            # EMA Cross Ratios
            group['cross_ema_9_20'] = close_px.ewm(span=9).mean() / close_px.ewm(span=20).mean()
            group['cross_sma_50_200'] = close_px.rolling(50).mean() / close_px.rolling(200).mean()
            
            # --- BOLLINGER BANDS ---
            sma_20 = close_px.rolling(20).mean()
            std_20 = close_px.rolling(20).std()
            upper_bb = sma_20 + (std_20 * 2)
            lower_bb = sma_20 - (std_20 * 2)
            
            group['bb_width'] = (upper_bb - lower_bb) / sma_20
            group['bb_pct_b'] = (close_px - lower_bb) / (upper_bb - lower_bb + 1e-8) 
            
            # --- MOMENTUM OSCILLATORS ---
            # Relative Strength Index (RSI)
            delta = close_px.diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            
            for period in [9, 14, 21]:
                rs = gain.rolling(period).mean() / (loss.rolling(period).mean() + 1e-8)
                group[f'rsi_{period}'] = 100 - (100 / (1 + rs))
                
            # MACD (Moving Average Convergence Divergence)
            ema_12 = close_px.ewm(span=12, adjust=False).mean()
            ema_26 = close_px.ewm(span=26, adjust=False).mean()
            macd_line = ema_12 - ema_26
            signal_line = macd_line.ewm(span=9, adjust=False).mean()
            group['macd_hist'] = macd_line - signal_line
            
            # --- CANDLESTICK MICROSTRUCTURE & ATR ---
            candle_range = (high_px - low_px) + 1e-8
            group['candle_body'] = abs(group['open'] - close_px) / candle_range
            group['upper_shadow'] = (high_px - np.maximum(group['open'], close_px)) / candle_range
            group['lower_shadow'] = (np.minimum(group['open'], close_px) - low_px) / candle_range
            
            tr = pd.concat([high_px - low_px, abs(high_px - close_px.shift(1)), abs(low_px - close_px.shift(1))], axis=1).max(axis=1)
            group['natr_14'] = tr.rolling(14).mean() / close_px
            
            # --- TARGET ENGINEERING ---
            # Next day's log return for predictive modeling
            group['fwd_log_return'] = np.log(group['close'].shift(-1) / group['close'])
            processed_groups.append(group)
            
        result_df = pd.concat(processed_groups).sort_index()
        
        if is_inference:
            feature_cols = [col for col in result_df.columns if col != 'fwd_log_return']
            result_df = result_df.dropna(subset=feature_cols)
        else:
            result_df = result_df.dropna()
            
        result_df = result_df.drop(columns=self.forbidden_columns + ['ret_spy_1d', 'ret_spy_14d'], errors='ignore')
        
        return result_df