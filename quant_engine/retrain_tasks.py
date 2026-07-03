import os
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
from airflow.exceptions import AirflowSkipException
from features.feature_engineering import TechnicalFeatureEngineer
import json
import lightgbm as lgb
import joblib
import mlflow
import shutil



DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    try:
        from airflow.configuration import conf
        DATABASE_URL = conf.get('database', 'sql_alchemy_conn')
    except ImportError:
        pass

if DATABASE_URL and DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")

def check_drift_and_metrics(logical_date_str: str) -> bool:
    """
    Queries the last 20 active (non-HOLD) reconciled predictions to calculate the 
    Actionable Win Rate of the LightGBM model. Raises AirflowSkipException if 
    performance remains above the alert threshold.
    """
    engine = create_engine(DATABASE_URL)
    target_date = datetime.strptime(logical_date_str, "%Y-%m-%d").date()
    
    query = text("""
        SELECT quant_decision, realized_return, prediction_date
        FROM predictions_history
        WHERE quant_decision != 'HOLD' 
          AND reconciliation_status = 'RECONCILED'
          AND prediction_date <= :target_date
        ORDER BY prediction_date DESC, id DESC
        LIMIT 20
    """)
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"target_date": target_date})
    except Exception as e:
        print(f"Error accessing PostgreSQL: {e}")
        return False

    if len(df) < 20:
        print(f"Insufficient active history ({len(df)}/20 rows). Skipping retrain to preserve stability.")
        raise AirflowSkipException("Not enough historical data to reliably calculate drift yet.")
        
    hits = 0
    for _, row in df.iterrows():
        decision = row['quant_decision']
        realized_ret = row['realized_return']
        
        if decision == 'BUY' and realized_ret > 0:
            hits += 1
        elif decision == 'SELL' and realized_ret < 0:
            hits += 1


    actionable_win_rate = hits / len(df)
    print(f"Analysis window setup completed. Target Date: {target_date}")
    print(f"Evaluated active sample size: {len(df)} trades.")
    print(f"Calculated Model Actionable Win Rate: {actionable_win_rate * 100:.2f}%")


    if actionable_win_rate >= 0.45:
        print("Model performance is within acceptable statistical boundaries. Concept drift not confirmed.")
        raise AirflowSkipException(f"Skipping weekly retrain. Current Win Rate ({actionable_win_rate*100:.1f}%) is healthy.")


    print("CRITICAL: Actionable Win Rate fell below tolerance threshold. Concept Drift confirmed.")
    print("Proceeding to next task: Recent data extraction for leaf weight adjustment.")
    return True


def fetch_and_engineer_recent_data(logical_date_str: str) -> str:
    """
    Extracts 18 months of historical data, engineers features with inference=False 
    to preserve the target variable, and saves the result as a temporary Parquet file 
    to prevent Airflow XCom database bloat.
    """
    engine = create_engine(DATABASE_URL)
    target_date = datetime.strptime(logical_date_str, "%Y-%m-%d").date()
    
    query = text("""
        SELECT * FROM market_data 
        WHERE date >= :target_date - interval '540 days' AND date <= :target_date
    """)
    raw_df = pd.read_sql(query, engine, params={"target_date": target_date})
    
    if raw_df.empty:
        raise ValueError(f"CRITICAL: No market data found for retraining window ending in {target_date}.")
        
    vix_df = raw_df[raw_df['ticker'] == 'VIX'].copy().set_index('date')
    assets_df = raw_df[raw_df['ticker'] != 'VIX'].copy().set_index('date')
    assets_df['ticker'] = assets_df['ticker'].astype('category')
    
    vix_df['vix_return'] = np.log(vix_df['close'] / vix_df['close'].shift(1))
    panel_df = assets_df.merge(vix_df[['vix_return']], left_index=True, right_index=True, how='left')
    
    engineer = TechnicalFeatureEngineer()
    processed_df = engineer.transform(panel_df, is_inference=False)
    print("Columns:", processed_df.columns.tolist())
    
    if processed_df.empty:
        raise ValueError("Feature engineering resulted in an empty dataset. Check null values and target generation.")
    
    temp_dir = "/tmp/quant_engine_temp"
    os.makedirs(temp_dir, exist_ok=True)
    
    file_path = os.path.join(temp_dir, "light_train_data.parquet")
    
    processed_df.reset_index(inplace=True)
    processed_df.to_parquet(file_path, index=False)
    
    print(f"Engineered training data saved successfully to {file_path}")
    print(f"Total training shape: {processed_df.shape}")
    
    return file_path

def train_and_evaluate_challenger(data_path: str) -> str:
    """
    Loads recent market data, trains a Challenger model using fixed hyperparameters,
    and evaluates it against the production Champion over a blind 40-day holdout set.
    Refits and promotes the Challenger only if it achieves a higher Actionable Win Rate.
    """
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Training data not found at {data_path}")
        
    df = pd.read_parquet(data_path)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    model_dir = "/opt/airflow/quant_engine/models/etf/production"
    champion_path = os.path.join(model_dir, "etf_baseline_1.joblib")
    config_path = os.path.join(model_dir, "quant_config.json")

    champion_model = joblib.load(champion_path)
    with open(config_path, "r") as f:
        config = json.load(f)

    req_features = config.get('req_features')
    bull_limit = config.get('bull_limit')
    bear_limit = config.get('bear_limit')
    
    buy_thresh = config.get('buy_threshold')
    sell_thresh = config.get('sell_threshold')
    
    lgb_params = config.get('lgb_params')
    
    if 'class_weight' in lgb_params and lgb_params['class_weight']:
        lgb_params['class_weight'] = {int(k): v for k, v in lgb_params['class_weight'].items()}

    df['target_class'] = np.where(
        df['fwd_log_return'] >= bull_limit, 1,
        np.where(df['fwd_log_return'] <= bear_limit, -1, 0)
    )

    unique_dates = df['date'].unique()
    if len(unique_dates) < 100:
        raise ValueError("Not enough historical data to perform a robust Train/Test split.")

    cutoff_date = unique_dates[-40]
    train_df = df[df['date'] < cutoff_date]
    test_df = df[df['date'] >= cutoff_date]

    X_train = train_df[req_features]
    y_train = train_df['target_class']
    
    X_test = test_df[req_features]
    y_test = test_df['target_class']

    print(f"Training Challenger on {len(X_train)} samples...")
    challenger_model = lgb.LGBMClassifier(**lgb_params)
    challenger_model.fit(X_train, y_train)

    def get_actionable_accuracy(model, X, y_true):
        probs = model.predict_proba(X)
        idx_sell = np.where(model.classes_ == -1)[0][0]
        idx_buy = np.where(model.classes_ == 1)[0][0]

        actionable_preds = []
        actionable_trues = []

        for i in range(len(probs)):
            prob_buy = probs[i][idx_buy]
            prob_sell = probs[i][idx_sell]

            if prob_buy >= buy_thresh:
                actionable_preds.append(1)
                actionable_trues.append(y_true.iloc[i])
            elif prob_sell >= sell_thresh:
                actionable_preds.append(-1)
                actionable_trues.append(y_true.iloc[i])

        if len(actionable_preds) == 0:
            return 0.0
            
        correct = np.sum(np.array(actionable_preds) == np.array(actionable_trues))
        return correct / len(actionable_preds)

    champ_acc = get_actionable_accuracy(champion_model, X_test, y_test)
    chall_acc = get_actionable_accuracy(challenger_model, X_test, y_test)

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    mlflow.set_experiment("Light_Retrain_Validation")
    
    with mlflow.start_run(run_name=f"Retrain_{datetime.now().strftime('%Y-%m-%d')}"):
        mlflow.log_metric("champion_accuracy", champ_acc)
        mlflow.log_metric("challenger_accuracy", chall_acc)
        
        features_summary = df[req_features].describe().to_markdown()
        mlflow.log_text(features_summary, "data_health/training_dataset_distribution.md")
        
        print(f"Champion Actionable Accuracy: {champ_acc:.2%}")
        print(f"Challenger Actionable Accuracy: {chall_acc:.2%}")

        if chall_acc > champ_acc:
            print("Verdict: CHALLENGER WINS. Refitting on 100% of data...")
            
            X_full = df[req_features]
            y_full = df['target_class']
            final_model = lgb.LGBMClassifier(**lgb_params)
            final_model.fit(X_full, y_full)
            joblib.dump(final_model, champion_path)
            mlflow.log_param("promoted", True)
            
            os.remove(data_path)
            
            return f"PROMOTED - New baseline saved at {champion_path}"
        else:
            print("Verdict: CHAMPION WINS or TIES. Challenger discarded.")
            mlflow.log_param("promoted", False)
            os.remove(data_path)
            raise AirflowSkipException("Challenger failed to beat Champion. Old weights retained.")