import joblib
import json
import os
from typing import Any, List, Dict
from targets.target_engineer import TargetEngineer

class ProductionExporter:
    """
    Handles the serialization of machine learning models and their accompanying 
    metadata manifests for production deployment.
    """
    
    def __init__(self, export_dir: str = "../models/etf/production"):
        self.export_dir = export_dir
        os.makedirs(self.export_dir, exist_ok=True)
        
    def export(
        self, 
        model: Any, 
        target_engineer: TargetEngineer, 
        buy_threshold: float, 
        sell_threshold: float,
        lgb_params: Dict[str, Any],
        train_days: int = 252,
        step_days: int = 20,
        embargo_days: int = 5
    ) -> None:
        """
        Saves the LightGBM tree and the JSON configuration manifest.
        """
        # 1. Save the brain (LightGBM model)
        model_path = os.path.join(self.export_dir, "etf_baseline_1.joblib")
        joblib.dump(model, model_path)
        
        # 2. Extract absolute boundaries from the fitted TargetEngineer
        bull_limit = target_engineer.thresholds.get('high', 0.0)
        bear_limit = target_engineer.thresholds.get('low', 0.0)
        
        # 3. Build the Production Manifest
        manifest = {
            "buy_threshold": buy_threshold,
            "sell_threshold": sell_threshold,
            "bull_limit": bull_limit,
            "bear_limit": bear_limit,
            "q_high_original": target_engineer.q_high,
            "q_low_original": target_engineer.q_low,
            "req_train_days": train_days,
            "req_step_days": step_days,
            "req_embargo_days": embargo_days,
            "req_features": list(model.feature_name_),
            "lgb_params": lgb_params
        }
        
        manifest_path = os.path.join(self.export_dir, "quant_config.json")
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=4)
            
        print(f"\n--- SUCCESSFUL PRODUCTION EXPORT ---")
        print(f"Model saved at: {model_path}")
        print(f"Manifest saved at: {manifest_path}")