import mlflow
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, ConfusionMatrixDisplay
import matplotlib.pyplot as plt
import joblib
import tempfile
import os

class QuantTracker:
    def __init__(self, experiment_name, tracking_uri="http://localhost:5000"):
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)
        
    def log_walk_forward_run(self, run_name, model_params, y_true, y_pred, dias_train, dias_paso, q_high, q_low, umbral_compra, umbral_venta,
                             financial_metrics=None, final_model=None, permutation_metrics=None, 
                             shap_fig=None, shap_df=None, loo_metrics=None):
        with mlflow.start_run(run_name=run_name):
            
            mlflow.log_params(model_params)
            mlflow.log_param("train_window_days", dias_train)
            mlflow.log_param("step_days", dias_paso)
            mlflow.log_param("target_q_high", q_high)
            mlflow.log_param("target_q_low", q_low)
            mlflow.log_param("umbral_compra", umbral_compra)
            mlflow.log_param("umbral_venta", umbral_venta)
            
            # --- 1. MÉTRICAS FINANCIERAS (EL NEGOCIO) ---
            if financial_metrics is not None:
                mlflow.log_metric("FIN_Total_Return_Strat", financial_metrics["Total_Return_Strat"])
                mlflow.log_metric("FIN_Total_Return_Market", financial_metrics["Total_Return_Market"])
                mlflow.log_metric("FIN_Max_Drawdown", financial_metrics["Max_Drawdown"])
                mlflow.log_metric("FIN_Sharpe_Ratio", financial_metrics["Sharpe_Ratio"])
            
            # --- 2. MÉTRICAS DE DIAGNÓSTICO ML (EL MOTOR) ---
            report_dict = classification_report(y_true, y_pred, output_dict=True, zero_division=0)
            
            mlflow.log_metric("ML_Accuracy_Global", report_dict['accuracy'])
            
            # Extraemos Precision, Recall y F1 para cada clase
            # (Aseguramos que los nombres de las clases sean string para el diccionario)
            for clase in ['-1', '0', '1']:
                if clase in report_dict:
                    mlflow.log_metric(f"ML_Precision_C{clase}", report_dict[clase]['precision'])
                    mlflow.log_metric(f"ML_Recall_C{clase}", report_dict[clase]['recall'])
                    mlflow.log_metric(f"ML_F1_C{clase}", report_dict[clase]['f1-score'])

            # --- 3. TESTS DE ROBUSTEZ ESTADÍSTICA ---
            if permutation_metrics is not None:
                mlflow.log_metrics(permutation_metrics)
            if loo_metrics is not None:
                mlflow.log_metrics({f"LOO_acc_{t}": v for t, v in loo_metrics.items()})
            
            # --- 4. ARTEFACTOS (Gráficos, Modelos, Reportes) ---
            with tempfile.TemporaryDirectory() as tmpdir:
                # Matriz de Confusión
                fig_cm, ax_cm = plt.subplots(figsize=(8, 6))
                cm = confusion_matrix(y_true, y_pred, labels=[-1, 0, 1])
                disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=["Venta (-1)", "Neutral (0)", "Compra (1)"])
                disp.plot(ax=ax_cm, cmap='Blues')
                plt.title(f"Matriz OOS Walk-Forward ({len(y_true)} ops)")
                plt.tight_layout()
                cm_path = os.path.join(tmpdir, "confusion_matrix_OOS.png")
                plt.savefig(cm_path)
                mlflow.log_artifact(cm_path)
                plt.close(fig_cm)
                
                # Reporte en TXT
                report_txt = classification_report(y_true, y_pred)
                report_path = os.path.join(tmpdir, "classification_report_OOS.txt")
                with open(report_path, "w") as f:
                    f.write(report_txt)
                mlflow.log_artifact(report_path)
                
                if final_model is not None:
                    model_path = os.path.join(tmpdir, "last_quant_model.pkl")
                    joblib.dump(final_model, model_path)
                    mlflow.log_artifact(model_path)
                    
                if shap_fig is not None:
                    shap_path = os.path.join(tmpdir, "shap_summary_venta.png")
                    shap_fig.savefig(shap_path)
                    mlflow.log_artifact(shap_path)
                    plt.close(shap_fig)
                    
                if shap_df is not None:
                    csv_path = os.path.join(tmpdir, "shap_importance.csv")
                    shap_df.to_csv(csv_path, index=False)
                    mlflow.log_artifact(csv_path)
                    
            print(f"Run '{run_name}' logueado a MLflow (Finanzas + Diagnósticos completos).")