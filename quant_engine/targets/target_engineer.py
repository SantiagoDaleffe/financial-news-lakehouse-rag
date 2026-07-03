import numpy as np
import pandas as pd

class TargetEngineer:
    """
    Dynamically generates asymmetric target labels based on forward return quantiles.
    
    Instead of using fixed thresholds, it classifies returns dynamically to adapt to 
    changing market volatility regimes.
    
    Target Logic:
    $$y_t = 1 \text{ if } R_{t+1} \geq Q_{high}$$
    $$y_t = -1 \text{ if } R_{t+1} \leq Q_{low}$$
    $$y_t = 0 \text{ otherwise}$$
    """
    def __init__(self, q_high: float = 0.66, q_low: float = 0.33):
        """
        Args:
            q_high (float): The quantile threshold for the 'BUY' class (1).
            q_low (float): The quantile threshold for the 'SELL' class (-1).
        """
        self.q_high = q_high
        self.q_low = q_low
        self.thresholds = {}
        
    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculates temporal thresholds and applies labels."""
        self.thresholds['high'] = df['fwd_log_return'].quantile(self.q_high)
        self.thresholds['low'] = df['fwd_log_return'].quantile(self.q_low)
        return self._apply_labels(df.copy())

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies previously fitted labels to unseen out-of-sample data."""
        return self._apply_labels(df.copy())
        
    def _apply_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        """Vectorized labeling logic."""
        conditions = [
            (df['fwd_log_return'] > self.thresholds['high']),
            (df['fwd_log_return'] < self.thresholds['low'])
        ]
        # 1: Long, -1: Short, 0: Neutral
        df['target'] = np.select(conditions, [1, -1], default=0)
        return df