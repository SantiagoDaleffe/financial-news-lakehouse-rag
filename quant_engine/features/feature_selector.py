import pandas as pd
import lightgbm as lgb

class QuantFeatureSelector:
    def __init__(self, n_features=15):
        self.n_features = n_features
        
    def select(self, X_train, y_train):
        """
        Entrena un árbol rápido y extrae las variables con mayor ganancia de información.
        """
        model = lgb.LGBMClassifier(
            n_estimators=100,
            max_depth=4,
            importance_type='gain', # Evaluamos por cuánta entropía reduce
            verbosity=-1,
            random_state=42
        )
        
        model.fit(X_train, y_train)
        
        importancias = pd.Series(model.feature_importances_, index=X_train.columns)
        # Nos quedamos con los N mejores
        top_features = importancias.nlargest(self.n_features).index.tolist()
        
        return top_features