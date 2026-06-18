import numpy as np

class TargetEngineer:
    def __init__(self, q_high=0.66, q_low=0.33):
        self.q_high = q_high
        self.q_low = q_low
        self.thresholds = {}
        
    def fit_transform(self, df):
        # Ya no calculamos el shift(-1) acá, usamos la columna que preparó el FeatureEngineer
        self.thresholds['high'] = df['fwd_log_return'].quantile(self.q_high)
        self.thresholds['low'] = df['fwd_log_return'].quantile(self.q_low)
        return self._apply_labels(df.copy())

    def transform(self, df):
        return self._apply_labels(df.copy())
        
    def _apply_labels(self, df):
        condiciones = [
            (df['fwd_log_return'] > self.thresholds['high']),
            (df['fwd_log_return'] < self.thresholds['low'])
        ]
        df['target'] = np.select(condiciones, [1, -1], default=0)
        return df