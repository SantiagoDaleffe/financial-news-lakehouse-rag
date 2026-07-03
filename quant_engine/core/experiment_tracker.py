import mlflow
from sklearn.metrics import classification_report, confusion_matrix, ConfusionMatrixDisplay
import matplotlib.pyplot as plt
import joblib
import tempfile
import os
import pandas as pd
from typing import Dict, Any, Optional

class QuantTracker:
    """
    MLOps integration using MLflow for comprehensive experiment tracking.
    
    Logs hyperparameters, financial KPIs, classification metrics, statistical 
    robustness tests, and stores artifacts (models, plots) for reproducibility.
    """
    def __init__(self, experiment_name: str, tracking_uri: str = "http://localhost:5000"):
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)
        
    def log_walk_forward_run(
        self, 
        run_name: str, 
        model_params: Dict[str, Any], 
        y_true: list, 
        y_pred: list, 
        train_days: int, 
        step_days: int, 
        q_high: float, 
        q_low: float, 
        buy_threshold: float, 
        sell_threshold: float,
        financial_metrics: Optional[Dict[str, float]] = None, 
        final_model: Optional[Any] = None, 
        permutation_metrics: Optional[Dict[str, float]] = None, 
        shap_fig: Optional[plt.Figure] = None, 
        shap_df: Optional[pd.DataFrame] = None, 
        loo_metrics: Optional[Dict[str, float]] = None
    ):
        """Executes the logging payload inside an MLflow run context."""
        with mlflow.start_run(run_name=run_name):
            
            mlflow.log_params(model_params)
            mlflow.log_param("train_window_days", train_days)
            mlflow.log_param("step_days", step_days)
            mlflow.log_param("target_q_high", q_high)
            mlflow.log_param("target_q_low", q_low)
            mlflow.log_param("buy_threshold", buy_threshold)
            mlflow.log_param("sell_threshold", sell_threshold)
            
            # --- 1. FINANCIAL METRICS (Business KPIs) ---
            if financial_metrics is not None:
                mlflow.log_metrics({f"FIN_{k}": v for k, v in financial_metrics.items()})
            
            # --- 2. ML DIAGNOSTICS (Engine KPIs) ---
            report_dict = classification_report(y_true, y_pred, output_dict=True, zero_division=0)
            
            mlflow.log_metric("ML_Accuracy_Global", report_dict['accuracy'])
            
            for class_label in ['-1', '0', '1']:
                if class_label in report_dict:
                    mlflow.log_metric(f"ML_Precision_C{class_label}", report_dict[class_label]['precision'])
                    mlflow.log_metric(f"ML_Recall_C{class_label}", report_dict[class_label]['recall'])
                    mlflow.log_metric(f"ML_F1_C{class_label}", report_dict[class_label]['f1-score'])

            # --- 3. STATISTICAL ROBUSTNESS TESTS ---
            if permutation_metrics is not None:
                mlflow.log_metrics(permutation_metrics)
            if loo_metrics is not None:
                mlflow.log_metrics({f"LOO_acc_{t}": v for t, v in loo_metrics.items()})
            
            # --- 4. ARTIFACTS (Plots, Models, Reports) ---
            with tempfile.TemporaryDirectory() as tmpdir:
                # Confusion Matrix
                fig_cm, ax_cm = plt.subplots(figsize=(8, 6))
                cm = confusion_matrix(y_true, y_pred, labels=[-1, 0, 1])
                disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=["Sell (-1)", "Neutral (0)", "Buy (1)"])
                disp.plot(ax=ax_cm, cmap='Blues')
                plt.title(f"OOS Walk-Forward Matrix ({len(y_true)} trades)")
                plt.tight_layout()
                cm_path = os.path.join(tmpdir, "confusion_matrix_OOS.png")
                plt.savefig(cm_path)
                mlflow.log_artifact(cm_path)
                plt.close(fig_cm)
                
                # Text Report
                report_txt = classification_report(y_true, y_pred)
                report_path = os.path.join(tmpdir, "classification_report_OOS.txt")
                with open(report_path, "w") as f:
                    f.write(report_txt)
                mlflow.log_artifact(report_path)
                
                if final_model is not None:
                    model_path = os.path.join(tmpdir, "etf_baseline_1.joblib")
                    joblib.dump(final_model, model_path)
                    mlflow.log_artifact(model_path)
                    
                if shap_fig is not None:
                    shap_path = os.path.join(tmpdir, "shap_summary.png")
                    shap_fig.savefig(shap_path)
                    mlflow.log_artifact(shap_path)
                    
                if shap_df is not None:
                    csv_path = os.path.join(tmpdir, "shap_importance.csv")
                    shap_df.to_csv(csv_path, index=False)
                    mlflow.log_artifact(csv_path)
                    
            print(f"Run '{run_name}' successfully committed to MLflow registry.")