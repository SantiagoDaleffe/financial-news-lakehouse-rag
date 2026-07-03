import joblib
import json
import pandas as pd
import numpy as np
import os
from core.portfolio_metrics import PortfolioEvaluator
from typing import Tuple, Dict

class OOSQuantSimulator:
    """
    Simulates a strictly Out-of-Sample (OOS) binary portfolio based on production artifacts.
    
    Implements a State Machine for position sizing (100% Cash vs 100% Asset) 
    preventing look-ahead bias and simulating realistic daily transitions.
    """
    def __init__(self, model_dir: str = "../models/etf/production"):
        self.model_dir = model_dir
        self.evaluator = PortfolioEvaluator()
        self._load_production_assets()
        
    def _load_production_assets(self) -> None:
        """Loads the frozen model and rules without peeking into the data."""
        print("Loading production artifacts...")
        self.model = joblib.load(os.path.join(self.model_dir, "etf_baseline_1.joblib"))
        
        with open(os.path.join(self.model_dir, "quant_config.json"), "r") as f:
            config = json.load(f)
            
        self.buy_threshold = config['buy_threshold']
        self.sell_threshold = config['sell_threshold']
        self.required_features = config['req_features']

    def simulate_portfolio(self, oos_panel_df: pd.DataFrame) -> Tuple[Dict[str, float], pd.DataFrame]:
        """
        Runs the state machine simulation across the OOS temporal axis.
        """
        print("Initiating binary portfolio simulation for Out-of-Sample period...")
        df = oos_panel_df.copy()
        
        # 1. Vectorized Inference
        X_input = df[self.required_features]
        probs = self.model.predict_proba(X_input)
        classes = self.model.classes_
        
        idx_sell = np.where(classes == -1)[0][0]
        idx_buy = np.where(classes == 1)[0][0]
        
        df['prob_buy'] = probs[:, idx_buy]
        df['prob_sell'] = probs[:, idx_sell]
        
        df['y_pred'] = 0
        df.loc[df['prob_buy'] >= self.buy_threshold, 'y_pred'] = 1
        df.loc[df['prob_sell'] >= self.sell_threshold, 'y_pred'] = -1
        
        # 2. Sequential loop for the Portfolio State Machine
        signals_df = df.pivot_table(index=df.index, columns='ticker', values='y_pred', observed=False).fillna(0)
        returns_df = df.pivot_table(index=df.index, columns='ticker', values='fwd_log_return', observed=False).fillna(0)
        
        dates = signals_df.index.sort_values()
        tickers = signals_df.columns
        
        # Initial State: 100% Liquid (0 = Cash, 1 = Long)
        position_states = {ticker: 0 for ticker in tickers}
        daily_portfolio_returns = []
        
        for date in dates:
            daily_asset_pnls = []
            
            for ticker in tickers:
                signal_today = signals_df.loc[date, ticker]
                return_today = returns_df.loc[date, ticker]
                current_state = position_states[ticker]
                
                # State Machine Transitions
                if current_state == 0 and signal_today == 1:
                    # Transition: Go Long
                    position_states[ticker] = 1
                    asset_pnl = return_today 
                elif current_state == 1 and signal_today == -1:
                    # Transition: Liquidate to Cash
                    position_states[ticker] = 0
                    asset_pnl = 0.0 
                elif current_state == 1:
                    # State: Hold Long Position
                    asset_pnl = return_today
                else:
                    # State: Hold Cash
                    asset_pnl = 0.0
                    
                daily_asset_pnls.append(asset_pnl)
            
            # Equal-weight portfolio return for the day
            daily_portfolio_returns.append(np.mean(daily_asset_pnls))
            
        # 3. Consolidate results for PortfolioEvaluator
        portfolio_res_df = pd.DataFrame({
            'y_true': 0, 
            'y_pred': 1, # Trick the evaluator to use our pre-calculated PnL directly
            'fwd_log_return': daily_portfolio_returns
        }, index=dates)
        
        metrics = self.evaluator.calculate_metrics(portfolio_res_df)
        
        # Benchmark comparison
        daily_market_returns = returns_df.mean(axis=1).values
        metrics['Total_Return_Market'] = np.exp(np.sum(daily_market_returns)) - 1
        
        return metrics, portfolio_res_df