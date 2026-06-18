import numpy as np
from sklearn.metrics import accuracy_score

class TickerLeaveOneOut:
    def __init__(self, backtester_instance):
        self.backtester = backtester_instance
        
    def run(self, df_panel, lgbm_params):
        tickers = df_panel['ticker'].unique()
        print(f"\n--- INICIANDO TEST DE GENERALIZACIÓN (TICKER LEAVE-ONE-OUT) ---")
        print(f"Tickers disponibles: {list(tickers)}")
        
        resultados_loo = {}
        
        for ticker_test in tickers:
            print(f"\n>> Dejando afuera: {ticker_test}")
            
            # 1. Separar universos
            mask_test = df_panel['ticker'] == ticker_test
            df_train_pool = df_panel[~mask_test].copy()
            df_test_pool = df_panel[mask_test].copy()
            
            # 2. Recreamos la lógica del tiempo del Walk-Forward
            # Pero entrenamos en el pool general y predecimos en el ticker aislado
            fechas_unicas = df_panel.index.unique().sort_values()
            inicio_bucle = self.backtester.dias_entrenamiento + self.backtester.dias_embargo
            
            y_true_aislado = []
            y_pred_aislado = []
            
            for i in range(inicio_bucle, len(fechas_unicas), self.backtester.dias_paso):
                idx_train_start = i - self.backtester.dias_embargo - self.backtester.dias_entrenamiento
                idx_train_end = i - self.backtester.dias_embargo - 1
                
                idx_test_start = i
                idx_test_end = min(i + self.backtester.dias_paso - 1, len(fechas_unicas) - 1)
                
                fecha_inicio_train = fechas_unicas[idx_train_start]
                fecha_fin_train = fechas_unicas[idx_train_end]
                fecha_inicio_test = fechas_unicas[idx_test_start]
                fecha_fin_test = fechas_unicas[idx_test_end]
                
                # Train: Universo Menos el Ticker
                train_mask = (df_train_pool.index >= fecha_inicio_train) & (df_train_pool.index <= fecha_fin_train)
                df_train = df_train_pool.loc[train_mask].copy()
                
                # Test: SOLAMENTE el Ticker dejado afuera
                test_mask = (df_test_pool.index >= fecha_inicio_test) & (df_test_pool.index <= fecha_fin_test)
                df_test = df_test_pool.loc[test_mask].copy()
                
                if len(df_train) < 100 or len(df_test) < 5:
                    continue 
                
                # Target dinámico basado SOLO en el pool de entrenamiento
                from targets.target_engineer import TargetEngineer 
                target_maker = TargetEngineer(q_high=self.backtester.q_high, q_low=self.backtester.q_low)
                df_train = target_maker.fit_transform(df_train)
                # Aplicamos esos mismos umbrales al ticker aislado
                df_test = target_maker.transform(df_test)
                
                cols_a_borrar = ['target', 'fwd_log_return', 'close']
                X_train = df_train.drop(columns=cols_a_borrar, errors='ignore')
                y_train = df_train['target']
                X_test = df_test.drop(columns=cols_a_borrar, errors='ignore')
                y_test = df_test['target']
                
                # Entrenar modelo (Ciego al ticker de test)
                import lightgbm as lgb
                model = lgb.LGBMClassifier(**lgbm_params)
                model.fit(X_train, y_train)
                
                preds = model.predict(X_test)
                y_pred_aislado.extend(preds)
                y_true_aislado.extend(y_test)
                
            acc_ticker = accuracy_score(y_true_aislado, y_pred_aislado)
            resultados_loo[ticker_test] = acc_ticker
            print(f"Accuracy prediciendo {ticker_test} sin haberlo visto nunca: {acc_ticker:.4f}")
            
        return resultados_loo