import shap
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class ShapExplainer:
    def __init__(self):
        pass

    def generate_diagnostics(self, model, df_panel, class_index=0):
        """
        Genera diagnósticos SHAP auto-alineando las columnas.
        class_index=0 es la clase -1 (Venta).
        """
        print("\n--- GENERANDO DIAGNÓSTICOS SHAP ---")
        
        # 1. El modelo es la fuente de verdad
        features_usadas = model.feature_name_
        
        # 2. Filtramos el panel usando solo esas columnas (Alineación perfecta garantizada)
        # Y NO TOCAMOS los tipos de datos. Dejamos la categoría intacta.
        X_sample = df_panel[features_usadas].tail(1000).copy()
        
        # 3. Calcular Valores SHAP
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_sample)
        
        # Manejo robusto de la salida de SHAP multiclase
        if isinstance(shap_values, list):
            shap_clase_riesgo = shap_values[class_index]
        elif len(shap_values.shape) == 3:
            if shap_values.shape[2] == len(model.classes_):
                shap_clase_riesgo = shap_values[:, :, class_index]
            else:
                shap_clase_riesgo = shap_values[:, class_index, :]
        else:
            shap_clase_riesgo = shap_values
            
        # 4. Generar el DataFrame Numérico
        mean_abs_shap = np.abs(shap_clase_riesgo).mean(axis=0)
        
        df_shap_importance = pd.DataFrame({
            'feature': features_usadas,
            'shap_mean_impact': mean_abs_shap
        })
        df_shap_importance = df_shap_importance.sort_values('shap_mean_impact', ascending=False).reset_index(drop=True)
        
        # 5. Generar el Gráfico Visual
        fig_shap, ax_shap = plt.subplots(figsize=(10, 6))
        shap.summary_plot(shap_clase_riesgo, X_sample, show=False)
        plt.title(f"Impacto SHAP - Clase Venta (-1)")
        plt.tight_layout()
        
        print("Diagnósticos SHAP calculados con éxito.")
        
        return df_shap_importance, fig_shap