from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys
import logging

# Ensure Airflow can locate the custom quant_engine modules
sys.path.append("/opt/airflow") 
sys.path.append("/opt/airflow/quant_engine")

from quant_engine.neuro_tasks import (
    reconcile_yesterday_predictions,
    build_features,
    run_quant_model,
    fetch_news_context,
    evaluate_signals_and_persist
)   

# Default arguments for the DAG's operational behavior
default_args = {
    "owner": "quant_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="neuro_trader_production_pipeline",
    default_args=default_args,
    description="Operational Neurosymbolic Pipeline with Daily Feedback Loop Reconciliations",
    schedule="0 22 * * 1-5", # Runs Monday-Friday at 22:00 UTC (Post NY market close)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["production", "neurosymbolic", "mlops", "inference"]
)
def neuro_trader_pipeline():
    """
    Orchestrates the daily inference cycle.
    
    The pipeline is divided into two logical parallel streams:
    1. Reconciliation: Evaluates the performance of yesterday's predictions against today's closing prices.
    2. Forward Inference: Extracts today's features, scores them through LightGBM, filters with NLP (FinBERT), 
       evaluates the edge cases via LLM (Gemini), and persists the new targets for tomorrow.
    """

    @task(task_id="reconcile_yesterday_performance")
    def task_reconcile(ds=None) -> str:
        """Triggers the MLOps feedback loop to compute realized returns and drift."""
        logging.info("Initiating dynamic feedback loop reconciliation against today's EOD prices...")
        result = reconcile_yesterday_predictions(ds)
        logging.info(result)
        return result

    @task(task_id="extract_and_engineer_features")
    def task_engineer(ds=None) -> list:
        """Pulls the 300-day rolling window from Postgres and applies cross-sectional feature engineering."""
        logging.info("Calculating technical and macro features from Postgres panel...")
        return build_features(ds)

    @task(task_id="quant_model_inference")
    def task_infer(daily_features: list) -> list:
        """Scores the latest features using the serialized LightGBM tree and extracts SHAP drivers."""
        logging.info(f"Running LightGBM inference and SHAP explainability on {len(daily_features)} active tickers...")
        return run_quant_model(daily_features)

    @task(task_id="semantic_news_retrieval")
    def task_retrieve(actionable_signals: list, ds) -> list:
        """Enhances the Quant signals with 48h macroeconomic context and FinBERT sentiment scores."""
        logging.info(f"Querying ChromaDB for fundamental catalysts on {len(actionable_signals)} candidate signals...")
        return fetch_news_context(actionable_signals, ds)

    @task(task_id="neurosymbolic_evaluation_and_persistence")
    def task_evaluate_and_save(enriched_signals: list, ds=None) -> None:
        """Prompts the LLM with the Neurosymbolic matrix and persists the final veridict to Postgres."""
        logging.info("Executing Gemini 3.5 Flash evaluations and writing actionable signals to operational database...")
        evaluate_signals_and_persist(enriched_signals, ds)

    reconciliation_report = task_reconcile()
    
    features = task_engineer()
    raw_signals = task_infer(features)
    context_signals = task_retrieve(raw_signals)
    
    task_evaluate_and_save(context_signals)

dag_instance = neuro_trader_pipeline()