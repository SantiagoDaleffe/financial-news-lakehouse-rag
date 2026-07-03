import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import accuracy_score
from typing import Dict, Any

from targets.target_engineer import TargetEngineer

class TickerLeaveOneOut:
    """
    Tests spatial generalization using a Leave-One-Out Cross-Validation (LOOCV) approach 
    over the cross-sectional universe (Tickers).
    
    Verifies if the model learns universal market microstructures rather than memorizing 
    idiosyncratic behaviors of specific assets.
    """
    def __init__(self, backtester_instance: Any):
        self.backtester = backtester_instance
        
    def run(self, panel_df: pd.DataFrame, lgbm_params: Dict[str, Any]) -> Dict[str, float]:
        """
        Iteratively holds out one ticker, trains on the rest, and evaluates on the hold-out.
        """
        tickers = panel_df['ticker'].unique()
        print(f"\n--- INITIATING GENERALIZATION TEST (TICKER LEAVE-ONE-OUT) ---")
        print(f"Available Universe: {list(tickers)}")
        
        loo_results = {}
        
        for test_ticker in tickers:
            print(f"\n>> Hold-out Ticker: {test_ticker}")
            
            mask_test = panel_df['ticker'] == test_ticker
            train_pool_df = panel_df[~mask_test].copy()
            test_pool_df = panel_df[mask_test].copy()
            
            unique_dates = panel_df.index.unique().sort_values()
            loop_start = self.backtester.train_days + self.backtester.embargo_days
            
            isolated_y_true = []
            isolated_y_pred = []
            
            for i in range(loop_start, len(unique_dates), self.backtester.step_days):
                idx_train_start = i - self.backtester.embargo_days - self.backtester.train_days
                idx_train_end = i - self.backtester.embargo_days - 1
                idx_test_start = i
                idx_test_end = min(i + self.backtester.step_days - 1, len(unique_dates) - 1)
                
                # Train subset: All tickers EXCEPT the hold-out
                train_mask = (train_pool_df.index >= unique_dates[idx_train_start]) & (train_pool_df.index <= unique_dates[idx_train_end])
                train_df = train_pool_df.loc[train_mask].copy()
                
                # Test subset: ONLY the hold-out ticker
                test_mask = (test_pool_df.index >= unique_dates[idx_test_start]) & (test_pool_df.index <= unique_dates[idx_test_end])
                test_df = test_pool_df.loc[test_mask].copy()
                
                if len(train_df) < 100 or len(test_df) < 5:
                    continue 
                
                target_maker = TargetEngineer(q_high=self.backtester.q_high, q_low=self.backtester.q_low)
                train_df = target_maker.fit_transform(train_df)
                test_df = target_maker.transform(test_df)
                
                drop_cols = ['target', 'fwd_log_return', 'close']
                X_train = train_df.drop(columns=drop_cols, errors='ignore')
                y_train = train_df['target']
                X_test = test_df.drop(columns=drop_cols, errors='ignore')
                y_test = test_df['target']
                
                model = lgb.LGBMClassifier(**lgbm_params)
                model.fit(X_train, y_train)
                
                preds = model.predict(X_test)
                isolated_y_pred.extend(preds)
                isolated_y_true.extend(y_test)
                
            ticker_acc = accuracy_score(isolated_y_true, isolated_y_pred)
            loo_results[test_ticker] = ticker_acc
            print(f"Zero-shot accuracy for {test_ticker}: {ticker_acc:.4f}")
            
        return loo_results