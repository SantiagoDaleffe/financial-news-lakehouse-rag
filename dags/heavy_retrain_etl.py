from airflow.decorators import dag, task
from datetime import datetime
import sys
import logging

sys.path.append("/opt/airflow") 
sys.path.append("/opt/airflow/quant_engine")

from quant_engine.heavy_retrain_tasks import (
    extract_full_history,
    select_top_features,
    run_optuna_search,
    promote_heavy_challenger
)

default_args = {
    'owner': 'quant_team',
    'depends_on_past': False,
    'email_on_failure': True, 
    'retries': 0, 
}

@dag(
    dag_id='etf_heavy_retrain_pipeline',
    default_args=default_args,
    description='On-Demand Heavy Retrain: Feature Selection and Hyperparameter Bayesian Optimization.',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['quant_engine', 'mlops', 'optuna'],
)
def heavy_retrain_pipeline():

    @task(task_id="extract_full_history")
    def task_extract_history(**kwargs):
        logical_date_str = kwargs['ds']
        logging.info(f"Extracting 4 years of history up to {logical_date_str}...")
        return extract_full_history(logical_date_str)

    @task(task_id="dynamic_feature_selection")
    def task_feature_selection(data_path: str):
        logging.info("Running automated feature selection (Top 15)...")
        return select_top_features(data_path)

    @task(task_id="run_optuna_search")
    def task_optuna(data_path: str):
        logging.info("Starting Bayesian Optimization (WFO)...")
        return run_optuna_search(data_path)
        
    @task(task_id="arena_heavy_champion_vs_challenger")
    def task_arena(data_path: str, selected_features: list, best_params: dict):
        logging.info("Validating Optuna Challenger against current Champion...")
        return promote_heavy_challenger(data_path, selected_features, best_params)

    historical_data_path = task_extract_history()
    features = task_feature_selection(historical_data_path)
    optuna_params = task_optuna(historical_data_path)
    
    task_arena(historical_data_path, features, optuna_params)

heavy_retrain = heavy_retrain_pipeline()