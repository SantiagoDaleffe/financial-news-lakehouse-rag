import numpy as np
from sklearn.metrics import accuracy_score

class TargetShuffler:
    def __init__(self, backtester_instance, iteraciones=3):
        self.backtester = backtester_instance
        self.iteraciones = iteraciones
        
    def run(self, df_panel, lgbm_params, real_accuracy):
        print(f"\n--- INICIANDO TARGET SHUFFLING TEST ({self.iteraciones} Iteraciones) ---")
        accuracies_falsos = []
        
        for i in range(self.iteraciones):
            print(f"Corriendo Permutación {i+1}/{self.iteraciones}...")
            df_res, _ = self.backtester.run(df_panel, lgbm_params, shuffle_target=True)
            accuracies_falsos.append(accuracy_score(df_res['y_true'], df_res['y_pred']))
            
        acc_promedio_falso = np.mean(accuracies_falsos)
        veces_superado = sum(1 for acc in accuracies_falsos if acc >= real_accuracy)
        p_value = veces_superado / self.iteraciones
        
        print(f"Accuracy Real: {real_accuracy:.4f} | Azar: {acc_promedio_falso:.4f} | P-Value: {p_value:.4f}")
        
        return {
            "shuffle_avg_accuracy": acc_promedio_falso,
            "shuffle_p_value": p_value
        }