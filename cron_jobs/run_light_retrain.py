import os
import sys
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
import lightgbm as lgb
import joblib
import mlflow

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.append(PROJECT_ROOT)
sys.path.append(os.path.join(PROJECT_ROOT, "quant_engine"))

MODEL_DIR = os.path.join(PROJECT_ROOT, "quant_engine", "models", "etf", "production")
from features.feature_engineering import TechnicalFeatureEngineer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")


def check_drift_and_metrics(logical_date_str: str) -> bool:
    """Check for concept drift using recent reconciled predictions.

    This function queries the predictions_history table for the most recent
    reconciled, non-HOLD predictions up to the provided logical date. It
    computes an "actionable win rate" where BUY is considered a win if the
    realized return was positive and SELL is considered a win if the
    realized return was negative. If there are fewer than 20 recent
    reconciled predictions the process exits to avoid unstable retraining.

    Args:
        logical_date_str (str): Target date in YYYY-MM-DD format to evaluate
            the recent prediction window ending on this date.

    Returns:
        bool: True if concept drift is detected (actionable win rate below
            threshold and retrain should proceed). If drift is not detected
            the function exits the process with status 0.
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
        logging.error(f"Error accessing PostgreSQL: {e}")
        return False

    if len(df) < 20:
        logging.info(
            f"Insufficient active history ({len(df)}/20 rows). Skipping retrain to preserve stability."
        )
        sys.exit(0)

    hits = 0
    for _, row in df.iterrows():
        decision = row["quant_decision"]
        realized_ret = row["realized_return"]

        if decision == "BUY" and realized_ret > 0:
            hits += 1
        elif decision == "SELL" and realized_ret < 0:
            hits += 1

    actionable_win_rate = hits / len(df)
    logging.info(f"Evaluated active sample size: {len(df)} trades.")
    logging.info(
        f"Calculated Model Actionable Win Rate: {actionable_win_rate * 100:.2f}%"
    )

    if actionable_win_rate >= 0.45:
        logging.info(
            "Model performance is within acceptable statistical boundaries. Concept drift not confirmed."
        )
        logging.info(
            f"Skipping weekly retrain. Current Win Rate ({actionable_win_rate * 100:.1f}%) is healthy."
        )
        sys.exit(0)

    logging.warning(
        "CRITICAL: Actionable Win Rate fell below tolerance threshold. Concept Drift confirmed."
    )
    return True


def fetch_and_engineer_recent_data(logical_date_str: str) -> str:
    """Fetch recent market data and apply feature engineering for retraining.

    This function connects to the configured database, selects market data for
    the window ending at the given logical date (540 days prior through the
    given date), and prepares a panel dataset for model retraining. The VIX
    series is separated, its log returns are computed and merged into the
    asset panel. The combined dataset is then passed through the
    TechnicalFeatureEngineer to produce model-ready features.

    Args:
        logical_date_str (str): Target end date for the retraining window in
            ISO format (YYYY-MM-DD).

    Raises:
        ValueError: If no market data is found for the requested retraining
            window.

    Returns:
        str: Path to or identifier of the processed dataset ready for
            retraining (implementation-specific string returned by downstream
            processing).
    """
    engine = create_engine(DATABASE_URL)
    target_date = datetime.strptime(logical_date_str, "%Y-%m-%d").date()

    query = text("""
        SELECT * FROM market_data 
        WHERE date >= :target_date - interval '540 days' AND date <= :target_date
    """)
    raw_df = pd.read_sql(query, engine, params={"target_date": target_date})

    if raw_df.empty:
        raise ValueError(
            f"CRITICAL: No market data found for retraining window ending in {target_date}."
        )

    vix_df = raw_df[raw_df["ticker"] == "VIX"].copy().set_index("date")
    assets_df = raw_df[raw_df["ticker"] != "VIX"].copy().set_index("date")
    assets_df["ticker"] = assets_df["ticker"].astype("category")

    vix_df["vix_return"] = np.log(vix_df["close"] / vix_df["close"].shift(1))
    panel_df = assets_df.merge(
        vix_df[["vix_return"]], left_index=True, right_index=True, how="left"
    )

    engineer = TechnicalFeatureEngineer()
    processed_df = engineer.transform(panel_df, is_inference=False)

    if processed_df.empty:
        raise ValueError("Feature engineering resulted in an empty dataset.")

    temp_dir = "/tmp/quant_engine_temp"
    os.makedirs(temp_dir, exist_ok=True)
    file_path = os.path.join(temp_dir, "light_train_data.parquet")

    processed_df.reset_index(inplace=True)
    processed_df.to_parquet(file_path, index=False)
    logging.info(f"Engineered training data saved to {file_path}")

    return file_path


def train_and_evaluate_challenger(data_path: str) -> str:
    """Train a challenger LightGBM model and evaluate against the champion.

    This function loads preprocessed training data from `data_path`, loads the
    current champion model and configuration from the model directory, trains
    a challenger LightGBM model using the provided parameters and feature
    requirements, evaluates performance against the champion, and returns the
    path to the trained challenger model artifact.

    Args:
        data_path (str): Path to the parquet file containing engineered
            training data.

    Raises:
        FileNotFoundError: If `data_path` does not exist.
        ValueError: If required configuration or features are missing or if
            training/evaluation fails.

    Returns:
        str: Filesystem path to the saved challenger model artifact.
    """
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Training data not found at {data_path}")

    df = pd.read_parquet(data_path)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    champion_path = os.path.join(MODEL_DIR, "etf_baseline_1.joblib")
    config_path = os.path.join(MODEL_DIR, "quant_config.json")

    champion_model = joblib.load(champion_path)
    with open(config_path, "r") as f:
        config = json.load(f)

    req_features = config.get("req_features")
    bull_limit = config.get("bull_limit")
    bear_limit = config.get("bear_limit")
    buy_thresh = config.get("buy_threshold")
    sell_thresh = config.get("sell_threshold")
    lgb_params = config.get("lgb_params")

    if "class_weight" in lgb_params and lgb_params["class_weight"]:
        lgb_params["class_weight"] = {
            int(k): v for k, v in lgb_params["class_weight"].items()
        }

    df["target_class"] = np.where(
        df["fwd_log_return"] >= bull_limit,
        1,
        np.where(df["fwd_log_return"] <= bear_limit, -1, 0),
    )

    unique_dates = df["date"].unique()
    if len(unique_dates) < 100:
        raise ValueError("Not enough historical data for Train/Test split.")

    cutoff_date = unique_dates[-40]
    train_df = df[df["date"] < cutoff_date]
    test_df = df[df["date"] >= cutoff_date]

    X_train = train_df[req_features]
    y_train = train_df["target_class"]
    X_test = test_df[req_features]
    y_test = test_df["target_class"]

    logging.info(f"Training Challenger on {len(X_train)} samples.")
    challenger_model = lgb.LGBMClassifier(**lgb_params)
    challenger_model.fit(X_train, y_train)

    def get_actionable_accuracy(model, X, y_true):
        """Compute accuracy only on "actionable" predictions.

        An actionable prediction is one where the model predicts a buy (class 1)
        with probability >= buy_thresh or a sell (class -1) with probability
        >= sell_thresh. Predictions that do not meet either threshold are
        considered non-actionable and ignored when computing accuracy.

        Args:
            model: A fitted classifier implementing predict_proba and exposing
                classes_. Expected classes include -1, 0, and 1.
            X: Features for prediction (array-like or DataFrame).
            y_true: True labels (pd.Series or array-like) aligned with X.

        Returns:
            float: Accuracy over actionable predictions (0.0 if none are
                actionable).
        """
        probs = model.predict_proba(X)
        idx_sell = np.where(model.classes_ == -1)[0][0]
        idx_buy = np.where(model.classes_ == 1)[0][0]
        actionable_preds, actionable_trues = [], []

        for i in range(len(probs)):
            if probs[i][idx_buy] >= buy_thresh:
                actionable_preds.append(1)
                actionable_trues.append(y_true.iloc[i])
            elif probs[i][idx_sell] >= sell_thresh:
                actionable_preds.append(-1)
                actionable_trues.append(y_true.iloc[i])

        if len(actionable_preds) == 0:
            return 0.0
        return np.sum(np.array(actionable_preds) == np.array(actionable_trues)) / len(
            actionable_preds
        )

    champ_acc = get_actionable_accuracy(champion_model, X_test, y_test)
    chall_acc = get_actionable_accuracy(challenger_model, X_test, y_test)

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    mlflow.set_experiment("Light_Retrain_Validation")

    with mlflow.start_run(run_name=f"Retrain_{datetime.now().strftime('%Y-%m-%d')}"):
        mlflow.log_metric("champion_accuracy", champ_acc)
        mlflow.log_metric("challenger_accuracy", chall_acc)

        logging.info(f"Champion Actionable Accuracy: {champ_acc:.2%}")
        logging.info(f"Challenger Actionable Accuracy: {chall_acc:.2%}")

        if chall_acc > champ_acc:
            logging.info("Verdict: CHALLENGER WINS. Refitting on 100% of data.")
            X_full, y_full = df[req_features], df["target_class"]
            final_model = lgb.LGBMClassifier(**lgb_params)
            final_model.fit(X_full, y_full)
            joblib.dump(final_model, champion_path)
            mlflow.log_param("promoted", True)
            os.remove(data_path)
            return f"PROMOTED - New baseline saved at {champion_path}"
        else:
            logging.info("Verdict: CHAMPION WINS or TIES. Challenger discarded.")
            mlflow.log_param("promoted", False)
            os.remove(data_path)
            logging.info("Challenger failed to beat Champion. Old weights retained.")
            sys.exit(0)


if __name__ == "__main__":
    execution_date = datetime.utcnow().strftime("%Y-%m-%d")
    logging.info(f"Starting Light Retrain Pipeline for {execution_date}.")
    check_drift_and_metrics(execution_date)
    data_path = fetch_and_engineer_recent_data(execution_date)
    result = train_and_evaluate_challenger(data_path)
    logging.info(result)
