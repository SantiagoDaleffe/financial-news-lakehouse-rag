import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score
from typing import Dict, Any

class TargetShuffler:
    """
    Implements White's Reality Check via target permutation.
    
    Breaks the dependency between features (X) and targets (y) by randomly shuffling y. 
    If the model performs as well on shuffled data as real data, the original 
    predictive edge was likely spurious (overfitting).
    """
    def __init__(self, backtester_instance: Any, iterations: int = 3):
        self.backtester = backtester_instance
        self.iterations = iterations
        
    def run(self, panel_df: pd.DataFrame, lgbm_params: Dict[str, Any], real_accuracy: float) -> Dict[str, float]:
        """
        Executes the permutation test and computes the empirical p-value.
        
        $$p\text{-value} = \frac{\sum_{i=1}^{N} I(\text{Acc}_{dummy, i} \geq \text{Acc}_{real})}{N}$$
        """
        print(f"\n--- INITIATING TARGET SHUFFLING TEST ({self.iterations} Iterations) ---")
        dummy_accuracies = []
        
        for i in range(self.iterations):
            print(f"Running Permutation {i+1}/{self.iterations}...")
            res_df, _ = self.backtester.run(panel_df, lgbm_params, shuffle_target=True)
            dummy_accuracies.append(accuracy_score(res_df['y_true'], res_df['y_pred']))
            
        avg_dummy_acc = np.mean(dummy_accuracies)
        times_beaten = sum(1 for acc in dummy_accuracies if acc >= real_accuracy)
        p_value = times_beaten / self.iterations
        
        print(f"Real Accuracy: {real_accuracy:.4f} | Dummy Avg: {avg_dummy_acc:.4f} | P-Value: {p_value:.4f}")
        
        return {
            "shuffle_avg_accuracy": avg_dummy_acc,
            "shuffle_p_value": p_value
        }