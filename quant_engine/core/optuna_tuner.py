import optuna
import pandas as pd
import numpy as np
from sklearn.metrics import confusion_matrix
from core.backtest_engine import WalkForwardBacktester
from core.portfolio_metrics import PortfolioEvaluator

class QuantHyperTuner:
    def __init__(self, df_panel, backtester_args):
        self.df_panel = df_panel
        self.b_args = backtester_args
        self.evaluator = PortfolioEvaluator()

    def objective(self, trial):
        # Relajamos los umbrales del target para que haya más muestras de Venta reales
        q_high = trial.suggest_float("q_high", 0.60, 0.75) 
        q_low = trial.suggest_float("q_low", 0.25, 0.40)
        
        # Umbrales de confianza que decide Optuna
        u_compra = trial.suggest_float("umbral_compra", 0.40, 0.60)
        u_venta = trial.suggest_float("umbral_venta", 0.40, 0.60)
        
        # Pesos asimétricos moderados
        peso_compra = trial.suggest_float("peso_compra", 1.0, 1.5)
        peso_venta = trial.suggest_float("peso_venta", 1.5, 2.5) 
        
        lgbm_params = {
            "objective": "multiclass",
            "num_class": 3,
            "metric": "multi_logloss",
            "verbosity": -1,
            "device_type": "cpu",
            "class_weight": {-1: peso_venta, 0: 0.8, 1: peso_compra},
            "max_depth": trial.suggest_int("max_depth", 3, 7),
            "num_leaves": trial.suggest_int("num_leaves", 10, 50),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.1, log=True),
            "min_child_samples": trial.suggest_int("min_child_samples", 50, 300),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-3, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-3, 10.0, log=True),
        }

        backtester = WalkForwardBacktester(
            dias_entrenamiento=self.b_args['dias_entrenamiento'],
            dias_paso=self.b_args['dias_paso'],
            dias_embargo=self.b_args['dias_embargo'],
            q_high=q_high,
            q_low=q_low
        )
        
        df_res, _ = backtester.run(self.df_panel, lgbm_params, shuffle_target=False, 
                                   umbral_compra=u_compra, umbral_venta=u_venta)
        
        if len(df_res) < 100:
            return -9999.0
            
        metricas = self.evaluator.calculate_metrics(df_res)
        retorno = metricas['Total_Return_Strat']
        drawdown = metricas['Max_Drawdown']
        sharpe = metricas['Sharpe_Ratio']
        
        # Función de costo limpia: Sharpe como rey, Retorno como multiplicador.
        # Quitamos la penalización destructiva de la matriz para que no haga "Reward Hacking".
        score_final = (retorno * 100) + (sharpe * 5)
        
        # Solo castigamos la ruina total
        if drawdown < -0.20:
            score_final -= 500
        if retorno < 0:
            score_final -= 200

        return score_final

    def optimize(self, n_trials=50):
        print(f"Iniciando Optuna con {n_trials} iteraciones...")
        study = optuna.create_study(direction="maximize")
        study.optimize(self.objective, n_trials=n_trials, show_progress_bar=True)
        
        print("\n--- MEJORES HIPERPARÁMETROS ENCONTRADOS ---")
        for k, v in study.best_params.items():
            print(f"{k}: {v}")
            
        return study.best_params