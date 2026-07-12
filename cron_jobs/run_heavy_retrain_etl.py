import os
import sys
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import joblib
import mlflow
import lightgbm as lgb

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.append(PROJECT_ROOT)
sys.path.append(os.path.join(PROJECT_ROOT, "quant_engine"))
MODEL_DIR = os.path.join(PROJECT_ROOT, "quant_engine", "models", "etf", "production")

from core.optuna_tuner import QuantHyperTuner
from targets.target_engineer import TargetEngineer
from core.model_exporter import ProductionExporter
from features.feature_engineering import TechnicalFeatureEngineer
from features.feature_selector import QuantFeatureSelector

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")


def extract_full_history(logical_date_str: str) -> str:
    """Extract market data history, engineer features and save to parquet.

    Args:
        logical_date_str (str): Date string in YYYY-MM-DD format used as the end
            date for the historical data window.

    Raises:
        ValueError: If the database query returns no rows for the requested window.
        ValueError: If the processed dataset contains fewer than 2000 rows after
            feature engineering and filtering.

    Returns:
        str: File path to the saved parquet file containing the processed training data.
    """
    engine = create_engine(DATABASE_URL)
    target_date = datetime.strptime(logical_date_str, "%Y-%m-%d").date()

    query = text("""
        SELECT * FROM market_data 
        WHERE date >= :target_date - interval '1760 days' AND date <= :target_date
    """)
    raw_df = pd.read_sql(query, engine, params={"target_date": target_date})

    if raw_df.empty:
        raise ValueError(f"FATAL: Database empty for window ending {target_date}.")

    vix_df = raw_df[raw_df["ticker"] == "VIX"].copy().set_index("date")
    assets_df = raw_df[raw_df["ticker"] != "VIX"].copy().set_index("date")
    assets_df["ticker"] = assets_df["ticker"].astype("category")

    vix_df["vix_return"] = np.log(vix_df["close"] / vix_df["close"].shift(1))
    panel_df = assets_df.merge(
        vix_df[["vix_return"]], left_index=True, right_index=True, how="left"
    )

    engineer = TechnicalFeatureEngineer()
    processed_df = engineer.transform(panel_df, is_inference=False)

    four_years_ago = pd.Timestamp(target_date - timedelta(days=1460))
    processed_df = processed_df[processed_df.index >= four_years_ago].copy()

    if len(processed_df) < 2000:
        raise ValueError(
            f"FATAL: Insufficient data density. Only {len(processed_df)} rows survived."
        )

    temp_dir = "/tmp/quant_engine_temp"
    os.makedirs(temp_dir, exist_ok=True)
    file_path = os.path.join(temp_dir, "heavy_train_data.parquet")

    processed_df.reset_index(inplace=True)
    processed_df.to_parquet(file_path, index=False)
    return file_path


def select_top_features(data_path: str) -> list:
    """Select the top features for model training from historical data.

    Args:
        data_path (str): Path to the parquet file containing processed training data.

    Returns:
        list: Ordered list of the top feature names selected by the feature selector.
    """
    df = pd.read_parquet(data_path)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    unique_dates = df["date"].unique()
    cutoff_date = unique_dates[-40]
    train_df = df[df["date"] < cutoff_date].copy()

    target_maker = TargetEngineer(q_high=0.66, q_low=0.33)
    train_df = target_maker.fit_transform(train_df)

    drop_cols = ["target", "fwd_log_return", "close", "ticker", "date"]
    X_train = train_df.drop(columns=drop_cols, errors="ignore")
    y_train = train_df["target"]

    selector = QuantFeatureSelector(n_features=15)
    top_features = selector.select(X_train, y_train)
    logging.info(f"Top 15 features selected: {top_features}")
    return top_features


def run_optuna_search(data_path: str) -> dict:
    """Perform hyperparameter optimization using Optuna to find the best model parameters.

    Args:
        data_path (str): Path to the parquet file containing the panel data with date and features.

    Returns:
        dict: Dictionary containing the best hyperparameters found by the Optuna optimizer.
    """
    df = pd.read_parquet(data_path)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    unique_dates = df["date"].unique()
    cutoff_date = unique_dates[-40]
    optuna_df = df[df["date"] < cutoff_date].copy()

    backtester_args = {
        "train_days": 252,
        "step_days": 20,
        "embargo_days": 5,
        "n_features": 15,
    }
    search_space = {
        "q_high": (0.60, 0.75),
        "q_low": (0.25, 0.40),
        "buy_threshold": (0.40, 0.60),
        "sell_threshold": (0.40, 0.60),
        "weight_buy": (1.0, 1.5),
        "weight_sell": (1.5, 2.5),
        "weight_neutral": (0.8, 2.0),
        "n_estimators": (70, 200),
        "max_depth": (3, 7),
        "num_leaves": (10, 50),
        "learning_rate": (0.01, 0.1),
        "min_child_samples": (50, 300),
        "reg_alpha": (1e-3, 10.0),
        "reg_lambda": (1e-3, 10.0),
    }

    tuner = QuantHyperTuner(
        panel_df=optuna_df, backtester_args=backtester_args, search_space=search_space
    )
    best_params = tuner.optimize(n_trials=40, seed=1)
    return best_params


def promote_heavy_challenger(
    data_path: str, selected_features: list, best_params: dict
) -> str:
    """Promueve un challenger pesado usando los mejores parámetros encontrados.

    Args:
        data_path (str): Ruta al archivo Parquet con los datos históricos.
        selected_features (list): Lista de características seleccionadas para el modelo.
        best_params (dict): Diccionario con los mejores hiperparámetros optimizados.

    Returns:
        str: Identificador o ruta del modelo promovido tras el reentrenamiento.
    """
    df = pd.read_parquet(data_path)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    unique_dates = df["date"].unique()
    cutoff_date = unique_dates[-40]

    train_df = df[df["date"] < cutoff_date].copy()
    test_df = df[df["date"] >= cutoff_date].copy()

    q_high, q_low = best_params.pop("q_high"), best_params.pop("q_low")
    buy_thresh, sell_thresh = (
        best_params.pop("buy_threshold"),
        best_params.pop("sell_threshold"),
    )
    w_buy, w_sell, w_neutral = (
        best_params.pop("weight_buy"),
        best_params.pop("weight_sell"),
        best_params.pop("weight_neutral"),
    )

    lgbm_params = {
        "objective": "multiclass",
        "num_class": 3,
        "metric": "multi_logloss",
        "verbosity": -1,
        "device_type": "cpu",
        "random_state": 1,
        "class_weight": {-1: w_sell, 0: w_neutral, 1: w_buy},
        **best_params,
    }

    challenger_target_maker = TargetEngineer(q_high=q_high, q_low=q_low)
    chall_train_df = challenger_target_maker.fit_transform(train_df)
    chall_test_df = challenger_target_maker.transform(test_df)

    X_train_chall, y_train_chall = (
        chall_train_df[selected_features],
        chall_train_df["target"],
    )
    X_test_chall, y_test_chall = (
        chall_test_df[selected_features],
        chall_test_df["target"],
    )

    logging.info("Training Challenger on N-40 historical data...")
    challenger_model = lgb.LGBMClassifier(**lgbm_params)
    challenger_model.fit(X_train_chall, y_train_chall)

    champion_path = os.path.join(MODEL_DIR, "etf_baseline_1.joblib")
    config_path = os.path.join(MODEL_DIR, "quant_config.json")

    champion_model = joblib.load(champion_path)
    with open(config_path, "r") as f:
        champ_config = json.load(f)

    champ_features = champ_config["req_features"]
    champ_target_maker = TargetEngineer(
        q_high=champ_config["q_high_original"], q_low=champ_config["q_low_original"]
    )
    champ_target_maker.thresholds = {
        "high": champ_config["bull_limit"],
        "low": champ_config["bear_limit"],
    }

    champ_test_df = test_df.copy()
    champ_test_df["target"] = np.where(
        champ_test_df["fwd_log_return"] >= champ_config["bull_limit"],
        1,
        np.where(champ_test_df["fwd_log_return"] <= champ_config["bear_limit"], -1, 0),
    )
    X_test_champ, y_test_champ = champ_test_df[champ_features], champ_test_df["target"]

    def get_actionable_accuracy(model, X, y_true, u_buy, u_sell):
        probs = model.predict_proba(X)
        idx_sell, idx_buy = (
            np.where(model.classes_ == -1)[0][0],
            np.where(model.classes_ == 1)[0][0],
        )
        actionable_preds, actionable_trues = [], []

        for i in range(len(probs)):
            if probs[i][idx_buy] >= u_buy:
                actionable_preds.append(1)
                actionable_trues.append(y_true.iloc[i])
            elif probs[i][idx_sell] >= u_sell:
                actionable_preds.append(-1)
                actionable_trues.append(y_true.iloc[i])

        if not actionable_preds:
            return 0.0
        return np.sum(np.array(actionable_preds) == np.array(actionable_trues)) / len(
            actionable_preds
        )

    champ_acc = get_actionable_accuracy(
        champion_model,
        X_test_champ,
        y_test_champ,
        champ_config["buy_threshold"],
        champ_config["sell_threshold"],
    )
    chall_acc = get_actionable_accuracy(
        challenger_model, X_test_chall, y_test_chall, buy_thresh, sell_thresh
    )

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    mlflow.set_experiment("Heavy_Retrain_Validation")

    with mlflow.start_run(
        run_name=f"Heavy_Optuna_{datetime.now().strftime('%Y-%m-%d')}"
    ) as run:
        mlflow.log_metric("champion_accuracy", champ_acc)
        mlflow.log_metric("challenger_accuracy", chall_acc)
        mlflow.log_params(lgbm_params)

        if chall_acc > champ_acc:
            logging.info("Verdict: OPTUNA CHALLENGER WINS. Refitting on 100% of data.")
            final_target_maker = TargetEngineer(q_high=q_high, q_low=q_low)
            final_df = final_target_maker.fit_transform(df)
            X_full, y_full = final_df[selected_features], final_df["target"]

            final_model = lgb.LGBMClassifier(**lgbm_params)
            final_model.fit(X_full, y_full)

            exporter = ProductionExporter(export_dir=MODEL_DIR)
            exporter.export(
                model=final_model,
                target_engineer=final_target_maker,
                buy_threshold=buy_thresh,
                sell_threshold=sell_thresh,
                lgb_params=lgbm_params,
                train_days=252,
                step_days=20,
                embargo_days=5,
            )

            mlflow.log_artifact(os.path.join(MODEL_DIR, "etf_baseline_1.joblib"))
            mlflow.log_artifact(os.path.join(MODEL_DIR, "quant_config.json"))
            mlflow.log_param("promoted", True)
            os.remove(data_path)
            return f"HEAVY PROMOTION SUCCESSFUL. Backup stored in MinIO Run ID: {run.info.run_id}"
        else:
            logging.info("Verdict: CHAMPION RETAINS THE THRONE. Challenger discarded.")
            mlflow.log_param("promoted", False)
            os.remove(data_path)
            sys.exit(0)


if __name__ == "__main__":
    execution_date = datetime.utcnow().strftime("%Y-%m-%d")
    logging.info(f"Starting Heavy Retrain Pipeline for: {execution_date}")

    data_path = extract_full_history(execution_date)
    features = select_top_features(data_path)
    optuna_params = run_optuna_search(data_path)

    result = promote_heavy_challenger(data_path, features, optuna_params)
    logging.info(result)
