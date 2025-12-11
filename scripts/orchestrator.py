import logging
import time
from scripts.collect_data_APIs import run_collection as run_api
from scripts.collect_rss import run_rss_collection as run_rss
from scripts.process_bronze import run_bronze_to_silver
from scripts.process_gold import run_silver_to_gold

logging.basicConfig(level=logging.INFO, format='%(asctime)s - ORCHESTRATOR - %(message)s')

def run_pipeline():
    start_time = time.time()
    
    logging.info("STARTING DATA PIPELINE")

    # 1. DATA INGESTION (Bronze)
    logging.info("[STEP 1/3] DATA INGESTION (Bronze)")
    try:
        run_api(days_back=1) 
        run_rss()
    except Exception as e:
        logging.error(f"INGESTION FAILED: {e}")
        # No return, process the data available

    # PASO 2: BIG DATA & SPARK (Silver)
    logging.info("[STEP 2/3] SPARK CLEANING (Silver)")
    try:
        run_bronze_to_silver()
    except Exception as e:
        logging.error(f"SPARK ETL FAILED: {e}")
        return # # if spark fails, cant vectorize

    # PASO 3: AI & Embeddings (Gold)
    logging.info("[STEP 3/3] BUILDING EMBEDDINGS (Gold)")
    try:
        run_silver_to_gold()
    except Exception as e:
        logging.error(f"GOLD LAYER FAILED: {e}")
        return

    duration = round(time.time() - start_time, 2)
    logging.info(f"DATA PIPELINE ENDED ({duration}s)")

if __name__ == "__main__":
    run_pipeline()