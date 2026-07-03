import pandas as pd
import lightgbm as lgb

class QuantFeatureSelector:
    """
    Dynamic feature selection module designed for Walk-Forward optimization.
    
    Utilizes a lightweight LightGBM tree to evaluate the Information Gain of each feature, 
    selecting the top subset that best partitions the data in the current temporal regime.
    """

    def __init__(self, n_features: int = 15):
        """
        Args:
            n_features (int): The number of top features to retain after evaluating importance.
        """
        self.n_features = n_features
        
    def select(self, X_train: pd.DataFrame, y_train: pd.Series) -> list:
        """
        Fits a tree-based model and extracts features with the highest information gain.
        
        Information Gain measures the reduction in entropy $$H$$ given a feature $$a$$:
        $$\text{IG}(T, a) = H(T) - H(T|a)$$

        Args:
            X_train (pd.DataFrame): Training feature matrix.
            y_train (pd.Series): Training target variable.

        Returns:
            list: A list containing the string names of the top `n_features`.
        """
        model = lgb.LGBMClassifier(
            n_estimators=100,
            max_depth=4,
            importance_type='gain', # Emphasizes features that reduce entropy the most
            verbosity=-1,
            random_state=42
        )
        
        model.fit(X_train, y_train)
        
        feature_importances = pd.Series(model.feature_importances_, index=X_train.columns)
        
        # Isolate the subset of features that hold the most predictive power
        top_features = feature_importances.nlargest(self.n_features).index.tolist()
        
        return top_features