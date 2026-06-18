import pandas as pd
import lightgbm as lgb
import numpy as np
from targets.target_engineer import TargetEngineer
from features.feature_selector import QuantFeatureSelector

class WalkForwardBacktester:
    def __init__(self, dias_entrenamiento=500, dias_paso=20, dias_embargo=5, q_high=0.66, q_low=0.33, n_features=15):
        self.dias_entrenamiento = dias_entrenamiento
        self.dias_paso = dias_paso
        self.dias_embargo = dias_embargo
        self.q_high = q_high
        self.q_low = q_low
        self.n_features = n_features

    def run(self, df_panel, lgbm_params, shuffle_target=False, umbral_compra=0.50, umbral_venta=0.50):
        fechas_unicas = df_panel.index.unique().sort_values()
        
        resultados_bloques = []
        ultimo_modelo = None
        selector = QuantFeatureSelector(n_features=self.n_features)
        
        inicio_bucle = self.dias_entrenamiento + self.dias_embargo
        print(f"Iniciando Walk-Forward con Embargo de {self.dias_embargo} días...")

        for i in range(inicio_bucle, len(fechas_unicas), self.dias_paso):
            idx_train_start = i - self.dias_embargo - self.dias_entrenamiento
            idx_train_end = i - self.dias_embargo - 1
            
            idx_test_start = i
            idx_test_end = min(i + self.dias_paso - 1, len(fechas_unicas) - 1)
            
            fecha_inicio_train = fechas_unicas[idx_train_start]
            fecha_fin_train = fechas_unicas[idx_train_end]
            fecha_inicio_test = fechas_unicas[idx_test_start]
            fecha_fin_test = fechas_unicas[idx_test_end]
            
            train_mask = (df_panel.index >= fecha_inicio_train) & (df_panel.index <= fecha_fin_train)
            test_mask = (df_panel.index >= fecha_inicio_test) & (df_panel.index <= fecha_fin_test)
            
            df_train = df_panel.loc[train_mask].copy()
            df_test = df_panel.loc[test_mask].copy()
            
            if len(df_train) < 100 or len(df_test) < 5:
                continue 
                
            target_maker = TargetEngineer(q_high=self.q_high, q_low=self.q_low)
            df_train = target_maker.fit_transform(df_train)
            df_test = target_maker.transform(df_test)
            
            cols_a_borrar = ['target', 'fwd_log_return', 'close', 'ticker']
            X_train = df_train.drop(columns=cols_a_borrar, errors='ignore')
            y_train = df_train['target']
            X_test = df_test.drop(columns=cols_a_borrar, errors='ignore')
            y_test = df_test['target']
            
            # --- FEATURE SELECTION DINÁMICO ---
            # Elegimos las 15 mejores variables de ESTA ventana temporal
            top_cols = selector.select(X_train, y_train)
            X_train = X_train[top_cols]
            X_test = X_test[top_cols]
            
            if shuffle_target:
                y_train = y_train.sample(frac=1, random_state=1).reset_index(drop=True)
            
            model = lgb.LGBMClassifier(**lgbm_params)
            model.fit(X_train, y_train)
            
            probs = model.predict_proba(X_test)
            clases = model.classes_
            idx_venta = np.where(clases == -1)[0][0]
            idx_compra = np.where(clases == 1)[0][0]
            
            preds = np.zeros(len(probs))
            
            # Usamos los umbrales dinámicos de Optuna (borré el 0.60 fijo que tenías)
            preds[probs[:, idx_compra] >= umbral_compra] = 1
            preds[probs[:, idx_venta] >= umbral_venta] = -1
            
            df_res = pd.DataFrame({
                'ticker': df_test['ticker'],
                'y_true': y_test,
                'y_pred': preds,
                'fwd_log_return': df_test['fwd_log_return']
            }, index=df_test.index)
            
            resultados_bloques.append(df_res)
            ultimo_modelo = model

        df_resultados_total = pd.concat(resultados_bloques)
        print(f"Backtest finalizado. {len(df_resultados_total)} operaciones simuladas.")
        
        return df_resultados_total, ultimo_modelo