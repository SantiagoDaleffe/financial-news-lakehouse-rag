import shap
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Tuple, Any

class ShapExplainer:
    """
    Generates SHAP (SHapley Additive exPlanations) values to interpret model decisions.
    
    Provides a game-theoretic approach to explain the output of the LightGBM trees,
    mapping the marginal contribution of each feature to the final prediction.
    """
    def __init__(self):
        pass

    def generate_diagnostics(self, model: Any, panel_df: pd.DataFrame, class_index: int = 0) -> Tuple[pd.DataFrame, plt.Figure]:
        """
        Auto-aligns features and computes global feature importance via SHAP magnitude.

        Args:
            model (Any): The fitted LightGBM model.
            panel_df (pd.DataFrame): The feature dataset.
            class_index (int): Index of the target class to explain (default 0 corresponds to SELL).

        Returns:
            Tuple[pd.DataFrame, plt.Figure]: SHAP summary dataframe and the matplotlib figure.
        """
        print("\n--- GENERATING SHAP DIAGNOSTICS ---")
        
        used_features = model.feature_name_
        
        # Filter panel preserving exact column order and dtypes
        X_sample = panel_df[used_features].tail(1000).copy()
        
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_sample)
        
        # Robust handling for multiclass SHAP structures across different shap versions
        if isinstance(shap_values, list):
            target_class_shap = shap_values[class_index]
        elif len(shap_values.shape) == 3:
            if shap_values.shape[2] == len(model.classes_):
                target_class_shap = shap_values[:, :, class_index]
            else:
                target_class_shap = shap_values[:, class_index, :]
        else:
            target_class_shap = shap_values
            
        mean_abs_shap = np.abs(target_class_shap).mean(axis=0)
        
        shap_importance_df = pd.DataFrame({
            'feature': used_features,
            'shap_mean_impact': mean_abs_shap
        }).sort_values('shap_mean_impact', ascending=False).reset_index(drop=True)
        
        shap_fig, shap_ax = plt.subplots(figsize=(10, 6))
        shap.summary_plot(target_class_shap, X_sample, show=False)
        plt.title(f"SHAP Impact Summary - Class Index {class_index}")
        plt.tight_layout()
        
        print("SHAP diagnostics successfully computed.")
        
        return shap_importance_df, shap_fig