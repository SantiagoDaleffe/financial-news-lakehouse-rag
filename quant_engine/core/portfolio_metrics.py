import numpy as np
import pandas as pd
from typing import Dict

class PortfolioEvaluator:
    """
    Computes core financial performance metrics for the trading strategy.
    
    Evaluates the Out-of-Sample predictions against actual forward returns, 
    accounting for compounding logic and panel data aggregations.
    """
    def __init__(self, risk_free_rate: float = 0.0):
        """
        Args:
            risk_free_rate (float): The annualized risk-free rate for Sharpe calculation.
        """
        self.rf = risk_free_rate 
        
    def calculate_metrics(self, results_df: pd.DataFrame) -> Dict[str, float]:
        """
        Calculates Total Return, Max Drawdown, and Sharpe Ratio.
        
        Args:
            results_df (pd.DataFrame): DataFrame containing 'y_pred' and 'fwd_log_return'.

        Returns:
            Dict[str, float]: A dictionary of the computed financial metrics.
        """
        df = results_df.copy()
        
        # Individual trade return based on signal
        df['strategy_return'] = np.where(
            df['y_pred'] == 1, df['fwd_log_return'],
            np.where(df['y_pred'] == -1, -df['fwd_log_return'], 0.0)
        )
        
        # Aggregate cross-sectionally by date to simulate a daily rebalanced equal-weight portfolio
        daily_returns = df.groupby(level=0)[['fwd_log_return', 'strategy_return']].mean()
        
        strat_returns = daily_returns['strategy_return'].values
        market_returns = daily_returns['fwd_log_return'].values
        
        # 1. Cumulative Return
        cum_strat_return = np.exp(np.sum(strat_returns)) - 1
        cum_market_return = np.exp(np.sum(market_returns)) - 1
        
        # 2. Maximum Drawdown
        # $$MDD = \min \left( \frac{V_t - \max_{\tau \leq t} V_\tau}{\max_{\tau \leq t} V_\tau} \right)$$
        equity_curve = np.exp(np.cumsum(strat_returns))
        running_max = np.maximum.accumulate(equity_curve)
        drawdowns = (equity_curve - running_max) / running_max
        max_drawdown = np.min(drawdowns) 
        
        # 3. Annualized Sharpe Ratio
        # $$\text{Sharpe} = \frac{\sqrt{252} \times (\mu_p - R_f)}{\sigma_p}$$
        daily_mean = np.mean(strat_returns)
        daily_std = np.std(strat_returns) + 1e-8
        sharpe_ratio = np.sqrt(252) * (daily_mean - (self.rf / 252)) / daily_std
        
        return {
            "Total_Return_Strat": cum_strat_return,
            "Total_Return_Market": cum_market_return,
            "Max_Drawdown": max_drawdown,
            "Sharpe_Ratio": sharpe_ratio
        }