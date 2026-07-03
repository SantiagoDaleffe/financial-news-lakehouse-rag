from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys
import logging

sys.path.append("/opt/airflow") 
sys.path.append("/opt/airflow/quant_engine")

from quant_engine.retrain_tasks import (
    check_drift_and_metrics,
    fetch_and_engineer_recent_data,
    train_and_evaluate_challenger
)

default_args = {
    'owner': 'quant_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='etf_light_retrain_pipeline',
    default_args=default_args,
    description='Weekly Light Retrain: Drift detection, fixed-param retraining, and Champion vs Challenger validation.',
    schedule_interval='0 2 * * 6',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['quant_engine', 'mlops', 'retrain'],
)
def light_retrain_pipeline():

    @task(task_id="check_drift_status")
    def task_check_drift(**kwargs):
        """
        Analyzes the 'predictions_history' table to compute the Actionable Win Rate.
        Raises an AirflowSkipException if performance remains above the alert threshold,
        gracefully halting the DAG to prevent unnecessary retraining and structural overfitting.
        """
        logical_date_str = kwargs['ds']
        logging.info(f"Analyzing Actionable Win Rate to evaluate concept drift up to {logical_date_str}...")
        return check_drift_and_metrics(logical_date_str)

    @task(task_id="extract_and_prep_recent_data")
    def task_prep_data(**kwargs):
        """
        Retrieves an 18-month rolling window of raw market data up to the logical date.
        Applies the TechnicalFeatureEngineer in training mode (is_inference=False) to preserve 
        the target variable and saves the engineered dataset as a temporary Parquet file.
        """
        logical_date_str = kwargs['ds']
        logging.info(f"Extracting 18 months of historical data up to {logical_date_str}...")
        return fetch_and_engineer_recent_data(logical_date_str)
        
    @task(task_id="arena_champion_vs_challenger")
    def task_arena(data_path: str):
        """
        Trains a Challenger model with fixed hyperparameters and evaluates it against the 
        current production Champion over a blind hold-out test set. Promotes the Challenger 
        by overwriting the production artifact only if it achieves a higher Actionable Win Rate.
        """
        logging.info(f"Initiating validation duel (Champion vs Challenger) with data at: {data_path}")
        return train_and_evaluate_challenger(data_path)

    drift_status = task_check_drift()
    training_data_path = task_prep_data()
    
    drift_status >> training_data_path
    
    promotion_result = task_arena(training_data_path)

dag_instance = light_retrain_pipeline()