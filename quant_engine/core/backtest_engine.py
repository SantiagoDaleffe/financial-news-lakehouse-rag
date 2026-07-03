import pandas as pd
import lightgbm as lgb
import numpy as np
from typing import Tuple, Dict, Any
from targets.target_engineer import TargetEngineer
from features.feature_selector import QuantFeatureSelector

class WalkForwardBacktester:
    """
    Robust time-series validation framework for quantitative models.
    
    Implements Walk-Forward Optimization (WFO) with embargo periods to prevent 
    data leakage and look-ahead bias across overlapping temporal windows.
    Includes dynamic feature selection per regime.
    """
    def __init__(
        self, 
        train_days: int = 500, 
        step_days: int = 20, 
        embargo_days: int = 5, 
        q_high: float = 0.66, 
        q_low: float = 0.33, 
        n_features: int = 15
    ):
        self.train_days = train_days
        self.step_days = step_days
        self.embargo_days = embargo_days
        self.q_high = q_high
        self.q_low = q_low
        self.n_features = n_features

    def run(
        self, 
        panel_df: pd.DataFrame, 
        lgbm_params: Dict[str, Any], 
        shuffle_target: bool = False, 
        buy_threshold: float = 0.50, 
        sell_threshold: float = 0.50
    ) -> Tuple[pd.DataFrame, lgb.LGBMClassifier]:
        """
        Executes the rolling training and testing loop.

        Args:
            panel_df (pd.DataFrame): The engineered features dataset.
            lgbm_params (dict): Hyperparameters for the LightGBM classifier.
            shuffle_target (bool): If True, breaks the time-series correlation (useful for White's Reality Check).
            buy_threshold (float): Probability boundary to trigger a long position.
            sell_threshold (float): Probability boundary to trigger a short position.

        Returns:
            Tuple: A consolidated DataFrame of all Out-of-Sample predictions, and the last trained model object.
        """
        unique_dates = panel_df.index.unique().sort_values()
        
        block_results = []
        last_model = None
        selector = QuantFeatureSelector(n_features=self.n_features)
        
        loop_start = self.train_days + self.embargo_days
        print(f"Initiating Walk-Forward Validation with a {self.embargo_days}-day embargo...")

        for i in range(loop_start, len(unique_dates), self.step_days):
            idx_train_start = i - self.embargo_days - self.train_days
            idx_train_end = i - self.embargo_days - 1
            
            idx_test_start = i
            idx_test_end = min(i + self.step_days - 1, len(unique_dates) - 1)
            
            train_mask = (panel_df.index >= unique_dates[idx_train_start]) & (panel_df.index <= unique_dates[idx_train_end])
            test_mask = (panel_df.index >= unique_dates[idx_test_start]) & (panel_df.index <= unique_dates[idx_test_end])
            
            train_df = panel_df.loc[train_mask].copy()
            test_df = panel_df.loc[test_mask].copy()
            
            if len(train_df) < 100 or len(test_df) < 5:
                continue 
                
            target_maker = TargetEngineer(q_high=self.q_high, q_low=self.q_low)
            train_df = target_maker.fit_transform(train_df)
            test_df = target_maker.transform(test_df)
            
            drop_cols = ['target', 'fwd_log_return', 'close', 'ticker', 'date']
            X_train = train_df.drop(columns=drop_cols, errors='ignore')
            y_train = train_df['target']
            X_test = test_df.drop(columns=drop_cols, errors='ignore')
            y_test = test_df['target']
            
            # --- DYNAMIC FEATURE SELECTION ---
            # Selects the top predictive features exclusively for this specific temporal regime
            top_cols = selector.select(X_train, y_train)
            X_train = X_train[top_cols]
            X_test = X_test[top_cols]
            
            if shuffle_target:
                y_train = y_train.sample(frac=1, random_state=1).reset_index(drop=True)
            
            model = lgb.LGBMClassifier(**lgbm_params)
            model.fit(X_train, y_train)
            
            probs = model.predict_proba(X_test)
            classes = model.classes_
            idx_sell = np.where(classes == -1)[0][0]
            idx_buy = np.where(classes == 1)[0][0]
            
            preds = np.zeros(len(probs))
            
            # Dynamic boundaries assigned by Optuna (or defaults)
            preds[probs[:, idx_buy] >= buy_threshold] = 1
            preds[probs[:, idx_sell] >= sell_threshold] = -1
            
            res_df = pd.DataFrame({
                'ticker': test_df['ticker'],
                'y_true': y_test,
                'y_pred': preds,
                'fwd_log_return': test_df['fwd_log_return']
            }, index=test_df.index)
            
            block_results.append(res_df)
            last_model = model

        total_results_df = pd.concat(block_results)
        print(f"Backtest completed. Evaluated {len(total_results_df)} simulated trades across OOS blocks.")
        
        return total_results_df, last_model