import optuna
import pandas as pd
import numpy as np
from typing import Dict, Any
from core.backtest_engine import WalkForwardBacktester
from core.portfolio_metrics import PortfolioEvaluator

class QuantHyperTuner:
    """
    Bayesian optimization wrapper for the quantitative pipeline.
    
    Uses Optuna's Tree-structured Parzen Estimator (TPE) to find the global minimum 
    of the custom loss function (penalizing drawdowns and rewarding Sharpe).
    """
    def __init__(self, panel_df: pd.DataFrame, backtester_args: Dict[str, Any], search_space: Dict[str, tuple]):
        self.panel_df = panel_df
        self.b_args = backtester_args
        self.search_space = search_space
        self.evaluator = PortfolioEvaluator()

    def objective(self, trial: optuna.Trial) -> float:
        """
        The objective function to maximize. Runs a full Walk-Forward evaluation.
        """
        sp = self.search_space
        
        q_high = trial.suggest_float("q_high", *sp["q_high"]) 
        q_low = trial.suggest_float("q_low", *sp["q_low"])
        u_buy = trial.suggest_float("buy_threshold", *sp["buy_threshold"])
        u_sell = trial.suggest_float("sell_threshold", *sp["sell_threshold"])
        
        weight_buy = trial.suggest_float("weight_buy", *sp["weight_buy"])
        weight_sell = trial.suggest_float("weight_sell", *sp["weight_sell"]) 
        weight_neutral = trial.suggest_float("weight_neutral", *sp["weight_neutral"])
        
        lgbm_params = {
            "objective": "multiclass",
            "num_class": 3,
            "metric": "multi_logloss",
            "verbosity": -1,
            "device_type": "cpu",
            "n_estimators": trial.suggest_int("n_estimators", *sp["n_estimators"]),
            'random_state': 1,
            "class_weight": {-1: weight_sell, 0: weight_neutral, 1: weight_buy},
            "max_depth": trial.suggest_int("max_depth", *sp["max_depth"]),
            "num_leaves": trial.suggest_int("num_leaves", *sp["num_leaves"]),
            "learning_rate": trial.suggest_float("learning_rate", *sp["learning_rate"], log=True),
            "min_child_samples": trial.suggest_int("min_child_samples", *sp["min_child_samples"]),
            "reg_alpha": trial.suggest_float("reg_alpha", *sp["reg_alpha"], log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", *sp["reg_lambda"], log=True),
        }
        
        backtester = WalkForwardBacktester(
            train_days=self.b_args['train_days'],
            step_days=self.b_args['step_days'],
            embargo_days=self.b_args['embargo_days'],
            q_high=q_high,
            q_low=q_low
        )
        
        res_df, _ = backtester.run(
            self.panel_df, 
            lgbm_params, 
            shuffle_target=False, 
            buy_threshold=u_buy, 
            sell_threshold=u_sell
        )
        
        if len(res_df) < 100:
            return -9999.0
            
        metrics = self.evaluator.calculate_metrics(res_df)
        total_return = metrics['Total_Return_Strat']
        drawdown = metrics['Max_Drawdown']
        sharpe = metrics['Sharpe_Ratio']
        
        final_score = (total_return * 100) + (sharpe * 5)
        
        actionable_trades = len(res_df[res_df['y_pred'] != 0])
        coverage = actionable_trades / len(res_df)
        
        if coverage < 0.20:
            final_score -= (0.20 - coverage) * 1000 

        if drawdown < -0.20:
            final_score -= 500
        if total_return < 0:
            final_score -= 200

        return final_score

    def optimize(self, n_trials: int = 50, seed: int = 1) -> Dict[str, Any]:
        """
        Executes the hyperparameter search.
        """
        print(f"Initiating Optuna optimization with {n_trials} trials...")
        sampler = optuna.samplers.TPESampler(seed=seed)
        study = optuna.create_study(direction="maximize", sampler=sampler)
        study.optimize(self.objective, n_trials=n_trials, show_progress_bar=True)
        
        print("\n--- BEST HYPERPARAMETERS DISCOVERED ---")
        for k, v in study.best_params.items():
            print(f"{k}: {v}")
            
        return study.best_params