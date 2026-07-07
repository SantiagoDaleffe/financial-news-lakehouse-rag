import os
import json
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from pinecone import Pinecone
from sentence_transformers import SentenceTransformer
from google import genai
from google.genai import types
import joblib
import shap
import mlflow
from typing import List, Dict, Any
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

sys.path.append(PROJECT_ROOT)
from quant_engine.features.feature_engineering import TechnicalFeatureEngineer

MODEL_DIR = os.path.join(PROJECT_ROOT, "quant_engine", "models", "etf", "production")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")


def reconcile_yesterday_predictions(logical_date_str: str) -> str:
    """
    Closes the loop on previous predictions by comparing them against actual market data.
    
    Queries the database for 'PENDING' predictions, fetches the actual closing price 
    for the target date, calculates the realized log return, and determines the true 
    class label. Updates the database and logs aggregate performance metrics to MLflow 
    for continuous model monitoring (MLOps).

    Args:
        logical_date_str (str): The current execution date (YYYY-MM-DD).

    Returns:
        str: A summary message indicating the number of reconciled records.
    """
    engine = create_engine(DATABASE_URL)
    target_date = datetime.strptime(logical_date_str, "%Y-%m-%d").date()
    
    pending_query = text("""
        SELECT * FROM predictions_history 
        WHERE prediction_date < :target_date AND reconciliation_status = 'PENDING'
    """)
    df_pending = pd.read_sql(pending_query, engine, params={"target_date": target_date})
    
    if df_pending.empty:
        return f"No pending predictions to reconcile prior to {target_date}."
        
    with open(os.path.join(MODEL_DIR, "quant_config.json"), "r") as f:
        config = json.load(f)
        
    bull_limit = config.get("bull_limit", 0.002)
    bear_limit = config.get("bear_limit", -0.002)
    
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    mlflow.set_experiment("Price_Predict_Monitoring")
    
    reconciled_count = 0
    actionable_trades = 0
    hits = 0
    cumulative_return = 0.0
    
    with mlflow.start_run(run_name=f"Reconciliation_{target_date}"):
        for _, row in df_pending.iterrows():
            ticker = row['ticker']
            pred_id = row['id']
            price_then = row['pred_close_price']
            sig_date = row['signal_date']
            
            price_query = text("""
                SELECT close FROM market_data 
                WHERE ticker = :ticker AND date > :sig_date AND date <= :target_date
                ORDER BY date ASC LIMIT 1
            """)
            with engine.connect() as conn:
                result = conn.execute(price_query, {
                    "ticker": ticker, 
                    "sig_date": sig_date, 
                    "target_date": target_date
                }).fetchone()
                
            if not result:
                continue
                
            price_now = result[0]
            log_return = np.log(price_now / price_then)
            
            if log_return >= bull_limit:
                true_label = "BUY"
            elif log_return <= bear_limit:
                true_label = "SELL"
            else:
                true_label = "HOLD"
                
            llm_verdict = row['llm_verdict']
            
            # Aggregate metrics calculation for MLflow
            if llm_verdict in ["BUY", "SELL"]:
                actionable_trades += 1
                if llm_verdict == true_label:
                    hits += 1
                
                # Directional return calculation
                trade_return = log_return if llm_verdict == "BUY" else -log_return
                cumulative_return += trade_return
            
            with engine.begin() as conn:
                update_cmd = text("""
                    UPDATE predictions_history 
                    SET actual_close_price = :price_now,
                        realized_return = :log_return,
                        reconciliation_status = 'RECONCILED'
                    WHERE id = :pred_id
                """)
                conn.execute(update_cmd, {
                    "price_now": float(price_now), 
                    "log_return": float(log_return), 
                    "pred_id": pred_id
                })
                
            reconciled_count += 1

        # Log daily health metrics to MLflow Dashboard
        if actionable_trades > 0:
            mlflow.log_metric("daily_actionable_win_rate", hits / actionable_trades)
            mlflow.log_metric("daily_avg_realized_return", float(cumulative_return / actionable_trades))
            mlflow.log_metric("daily_actionable_volume", actionable_trades)
            
    return f"Successfully reconciled {reconciled_count} past assets as of {target_date}."


def build_features(logical_date_str: str) -> List[Dict[str, Any]]:
    """
    Extracts the most recent market data block and applies the TechnicalFeatureEngineer 
    pipeline in inference mode.

    Args:
        logical_date_str (str): The current execution date (YYYY-MM-DD).

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the engineered 
        cross-sectional features for the target date.
    """
    engine = create_engine(DATABASE_URL)
    target_date = datetime.strptime(logical_date_str, "%Y-%m-%d").date()
    
    query = text("""
        SELECT * FROM market_data 
        WHERE date >= :target_date - interval '300 days' AND date <= :target_date
    """)
    raw_df = pd.read_sql(query, engine, params={"target_date": target_date})
    
    if raw_df.empty:
        logging.error(f"CRITICAL: No market data found in Postgres up to {target_date}.")
        return []
        
    vix_df = raw_df[raw_df['ticker'] == 'VIX'].copy().set_index('date')
    assets_df = raw_df[raw_df['ticker'] != 'VIX'].copy().set_index('date')
    assets_df['ticker'] = assets_df['ticker'].astype('category')
    
    vix_df['vix_return'] = np.log(vix_df['close'] / vix_df['close'].shift(1))
    panel_df = assets_df.merge(vix_df[['vix_return']], left_index=True, right_index=True, how='left')
    
    engineer = TechnicalFeatureEngineer()
    processed_df = engineer.transform(panel_df, is_inference=True)
    
    latest_date_str = str(processed_df.index.max().date())
    latest_cross_section = processed_df[processed_df.index.astype(str).str.contains(latest_date_str)].copy()
    
    latest_cross_section.reset_index(inplace=True)
    latest_cross_section['date'] = latest_cross_section['date'].astype(str)
    latest_cross_section = latest_cross_section.replace({np.nan: None})
    
    return latest_cross_section.to_dict(orient='records')


def run_quant_model(daily_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Executes the quantitative LightGBM model to generate base predictions.
    
    Calculates probability distributions, defines conviction zones based on dynamic 
    thresholds, and utilizes SHAP (SHapley Additive exPlanations) to extract the 
    top mathematical drivers behind each prediction for LLM interpretability.

    Args:
        daily_data (List[Dict[str, Any]]): Engineered features for the current cross-section.

    Raises:
        ValueError: If essential features contain NaNs.

    Returns:
        List[Dict[str, Any]]: A list of preliminary quantitative signals containing 
        probabilities, conviction zones, and SHAP drivers.
    """
    if not daily_data: 
        return []
    
    df_today = pd.DataFrame(daily_data)
    
    try:
        model = joblib.load(os.path.join(MODEL_DIR, "etf_baseline_1.joblib"))
        with open(os.path.join(MODEL_DIR, "quant_config.json"), "r") as f:
            config = json.load(f)
    except FileNotFoundError as e:
        logging.error(f"Artifacts missing: {e}")
        return []
        
    buy_thresh = config.get('buy_threshold')
    sell_thresh = config.get('sell_threshold')
    required_features = config.get('req_features')
    
    explainer = shap.TreeExplainer(model)
    all_signals = []
    
    idx_sell = np.where(model.classes_ == -1)[0][0]
    idx_buy = np.where(model.classes_ == 1)[0][0]
    
    for _, row in df_today.iterrows():
        ticker = row['ticker']
        X_input = pd.DataFrame([row]).reindex(columns=required_features)
        
        if X_input.isnull().values.any():
            missing_cols = X_input.columns[X_input.isnull().any()].tolist()
            raise ValueError(f"CRITICAL DATA HOLE: Ticker {ticker} has missing or NaN values in features: {missing_cols}")
        
        probs = model.predict_proba(X_input)[0]
        prob_buy = probs[idx_buy]
        prob_sell = probs[idx_sell]
        
        max_prob = max(prob_buy, prob_sell)
        decision = "HOLD"
        conviction_zone = "COLD"
        target_class_idx = None
        leaning_action = "HOLD"
        
        if prob_buy >= buy_thresh: 
            decision, conviction_zone = "BUY", "HOT"
            target_class_idx, leaning_action = idx_buy, "BUY"
        elif prob_sell >= sell_thresh: 
            decision, conviction_zone = "SELL", "HOT"
            target_class_idx, leaning_action = idx_sell, "SELL"
        elif 0.38 <= max_prob < max(buy_thresh, sell_thresh): 
            conviction_zone = "GREY"
            if prob_buy > prob_sell:
                target_class_idx, leaning_action = idx_buy, "BUY"
            else:
                target_class_idx, leaning_action = idx_sell, "SELL"
        
        top_drivers = ["N/A"]
        
        if conviction_zone != "COLD":
            shap_values = explainer.shap_values(X_input)
            if isinstance(shap_values, list):
                target_shap = shap_values[target_class_idx][0]
            elif len(shap_values.shape) == 3:
                target_shap = shap_values[0, :, target_class_idx] if shap_values.shape[2] == len(model.classes_) else shap_values[0, target_class_idx, :]
            else:
                target_shap = shap_values[0]
                
            feature_importances = pd.Series(target_shap, index=required_features)
            top_drivers = feature_importances.abs().sort_values(ascending=False).head(3).index.tolist()
        else:
            top_drivers = ["None"]
            
        signal_dict = {
            "ticker": ticker, 
            "signal_date": row['date'],
            "ml_decision": decision, 
            "leaning_action": leaning_action,
            "probability": float(max_prob),
            "conviction_zone": conviction_zone, 
            "top_drivers": top_drivers, 
            "close_price": float(row['close']),
            "news_context": ""
        }
        
        for feat in required_features:
            signal_dict[f"feat_{feat}"] = float(row[feat])
            
        all_signals.append(signal_dict)
            
    return all_signals


def fetch_news_context(signals: List[Dict[str, Any]], logical_date_str: str) -> List[Dict[str, Any]]:
    """
    Performs similarity search in the Pinecone Vector Store to retrieve relevant 
    macroeconomic context for active signals. 
    
    Skips retrieval for assets in the 'COLD' conviction zone to optimize API usage 
    and reduce context window pollution.

    Args:
        signals (List[Dict[str, Any]]): The preliminary quantitative signals.
        logical_date_str (str): The current execution date (YYYY-MM-DD).

    Returns:
        List[Dict[str, Any]]: Signals enriched with unstructured textual context and FinBERT metadata.
    """
    if not signals: 
        return []
    
    pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
    index = pc.Index(os.getenv("PINECONE_INDEX_NAME"))
    model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
    
    target_date = datetime.strptime(logical_date_str, "%Y-%m-%d")
    target_timestamp = target_date.timestamp()
    window_start = (target_date - timedelta(days=4)).timestamp()
    
    enriched_signals = []
    
    for sig in signals:
        ticker = sig['ticker']
        
        if sig['conviction_zone'] == "COLD":
            sig['news_context'] = "N/A (Skipped - COLD zone)"
            sig['has_news'] = False
            enriched_signals.append(sig)
            continue
            
        try:
            search_query = f"{ticker} financial market news economy"
            embedding = model.encode(search_query).tolist()
            
            results = index.query(
                vector=embedding,
                top_k=15, 
                filter={
                    "published_at": {"$gte": window_start, "$lte": target_timestamp + 86400}
                },
                include_metadata=True,
                namespace="fin_news_v1"
            )
            matches = results.get('matches', [])
        except Exception as e:
            logging.error(f"Error querying Pinecone for {ticker}: {e}")
            matches = []
            
        ticker_news = []
        for match in matches:
            meta = match.get('metadata', {})
            
            is_match = False
            for key in ['ticker', 'tickers', 'ticker_principal', 'matched_tickers']:
                if key in meta and ticker in str(meta[key]):
                    is_match = True
                    break
                    
            if is_match:
                pub_ts = meta.get('published_at', target_timestamp)
                days_ago = max(0, round((target_timestamp - pub_ts) / 86400))
                
                if days_ago == 0:
                    age_str = "TODAY"
                elif days_ago == 1:
                    age_str = "1 DAY AGO"
                else:
                    age_str = f"{days_ago} DAYS AGO"
                    
                sentiment = meta.get('sentiment', 'NEUTRAL')
                score = meta.get('sentiment_score', 0.0)
                doc = meta.get('text', '')
                
                ticker_news.append(f"- [AGE: {age_str}] {doc} [FinBERT: {sentiment} (Score: {score:.2f})]")
        
        if ticker_news:
            sig['news_context'] = "\n".join(ticker_news)
            sig['has_news'] = True
        else:
            sig['news_context'] = "No recent macroeconomic news found for this asset."
            sig['has_news'] = False
            
        enriched_signals.append(sig)
        
    return enriched_signals


def evaluate_signals_and_persist(enriched_signals: List[Dict[str, Any]], logical_date_str: str) -> None:
    """
    Executes the neurosymbolic architecture by passing the Quantitative signals, 
    SHAP drivers, and RAG context to the LLM (Gemini) for final risk assessment.
    
    Implements hard-coded execution rules to prevent hallucinations. Persists 
    the final verdict in PostgreSQL and logs the audit trail (Markdown) to MLflow.

    Args:
        enriched_signals (List[Dict[str, Any]]): Signals enriched with RAG context.
        logical_date_str (str): The current execution date (YYYY-MM-DD).
    """
    if not enriched_signals: 
        return
        
    client = genai.Client(api_key=GEMINI_API_KEY)
    engine = create_engine(DATABASE_URL)
    target_date = datetime.strptime(logical_date_str, "%Y-%m-%d").date()
    
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    mlflow.set_experiment("Production_Inference_Audit")
    
    with mlflow.start_run(run_name=f"Inference_Audit_{target_date}"):
        
        df_audit_features = pd.DataFrame(enriched_signals)
        feat_cols = [c for c in df_audit_features.columns if c.startswith("feat_")]
        
        if not df_audit_features.empty and feat_cols:
            df_snapshot = df_audit_features[["ticker", "signal_date"] + feat_cols].copy()
            df_snapshot.columns = [c.replace("feat_", "") for c in df_snapshot.columns]
            csv_snapshot = df_snapshot.to_csv(index=False)
            mlflow.log_text(csv_snapshot, f"data_health/daily_features_snapshot.csv")
            
        for sig in enriched_signals:
            call_llm = False
            if sig['conviction_zone'] == "HOT":
                call_llm = True
            elif sig['conviction_zone'] == "GREY" and sig.get('has_news', False):
                call_llm = True
                
            if call_llm:
                prompt = f"""
                You are the Lead Risk & Execution Portfolio Manager of a Quantitative Hedge Fund.
                Asset under review: {sig['ticker']}
                
                1. ALGORITHMIC SUGGESTION (QUANT): The LightGBM model is in conviction zone {sig['conviction_zone']}. 
                   It mathematically leans towards {sig['leaning_action']} with a certainty of {sig['probability']*100:.1f}%.
                2. MODEL EXPLAINABILITY (SHAP): The top 3 technical drivers dictating this mathematical leaning are: {', '.join(sig['top_drivers'])}.
                3. MACROECONOMIC CONTEXT (Last 4 days News & NLP Sentiment):
                {sig['news_context']}
                
                EXECUTION RULES:
                - TEMPORAL DECAY (CRITICAL): Pay strict attention to the [AGE] tag on each news item. News from "TODAY" or "1 DAY AGO" carries maximum weight. Heavily discount the relevance of news older than 2 days.
                - If the Quant model is in the "GREY" conviction zone, you must ONLY decree {sig['leaning_action']} if the news reveals a significant, trend-confirming fundamental catalyst. Otherwise, decree HOLD.
                - If the Quant model is in the "HOT" conviction zone, use the news solely as a risk-control auditor. Abort and decree HOLD ONLY IF there is a severe, trend-reversing shock.
                - CRITICAL FALLBACK: If the Quant model is "HOT" and there is NO macroeconomic news found, you MUST implicitly trust the Quant model and decree {sig['leaning_action']}.
                - NLP AUDIT: Explicitly weigh the FinBERT sentiment scores. Discard noise.
                
                Issue your final verdict. Respond EXCLUSIVELY with one of these three words: BUY, SELL, or HOLD. Do not add justifications or any extra text.
                """
                
                try:
                    response = client.models.generate_content(
                        model='gemini-2.5-flash', 
                        contents=prompt,
                        config=types.GenerateContentConfig(temperature=0.0) 
                    )
                    final_decision = response.text.strip().upper()
                    
                    for action in ["BUY", "SELL", "HOLD"]:
                        if action in final_decision:
                            final_decision = action
                            break
                            
                except Exception as e:
                    logging.error(f"LLM failure for {sig['ticker']}: {e}. Defaulting to HOLD.")
                    final_decision = "HOLD"
            else:
                final_decision = "HOLD"
                logging.info(f"[{sig['ticker']}] Bypassing LLM (Zone: {sig['conviction_zone']}, News: {sig.get('has_news', False)}). Verdict: HOLD")

            audit_md = f"""# Neurosymbolic Audit Trail - {sig['ticker']} ({target_date})
            
## 1. Quant Engine Output
* **Base Decision:** {sig['ml_decision']}
* **Leaning Action:** {sig['leaning_action']}
* **Conviction Zone:** {sig['conviction_zone']}
* **Probability:** {sig['probability']*100:.2f}%
* **Top SHAP Drivers:** {', '.join(sig['top_drivers'])}

## 2. LLM Portfolio Manager Output
* **Final Verdict Executed:** **{final_decision}**
* *(Did the LLM evaluate this? {'Yes' if call_llm else 'No, bypassed due to strict execution rules'})*

## 3. NLP Context Provided (FinBERT)
{sig['news_context']}
"""
            mlflow.log_metric(f"{sig['ticker']}_quant_prob", float(sig['probability']))
            mlflow.log_text(audit_md, f"audits/{sig['ticker']}_audit.md")
            
            try:
                with engine.begin() as conn:
                    delete_cmd = text("""
                        DELETE FROM predictions_history 
                        WHERE signal_date = :signal_date AND ticker = :ticker
                    """)
                    conn.execute(delete_cmd, {"signal_date": sig['signal_date'], "ticker": sig['ticker']})
                    
                    insert_cmd = text("""
                        INSERT INTO predictions_history 
                        (prediction_date, signal_date, ticker, quant_decision, quant_probability, conviction_zone, top_drivers, pred_close_price, llm_verdict, reconciliation_status)
                        VALUES (:target_date, :signal_date, :ticker, :quant_decision, :prob, :zone, :drivers, :price, :llm_verdict, 'PENDING')
                    """)
                    conn.execute(insert_cmd, {
                        "target_date": target_date,
                        "signal_date": sig['signal_date'],
                        "ticker": sig['ticker'], 
                        "quant_decision": sig['ml_decision'], 
                        "prob": sig['probability'],
                        "zone": sig['conviction_zone'], 
                        "drivers": ",".join(sig['top_drivers']),
                        "price": sig['close_price'], 
                        "llm_verdict": final_decision
                    })
            except Exception as db_e:
                logging.error(f"DB persistence failed for {sig['ticker']}: {db_e}")


if __name__ == "__main__":
    execution_date = datetime.utcnow().strftime("%Y-%m-%d")
    logging.info(f"Starting Inference Pipeline for date: {execution_date} ---")
    
    logging.info("1. Reconciling yesterday's predictions with actual market data.")
    recon_result = reconcile_yesterday_predictions(execution_date)
    logging.info(recon_result)
    
    logging.info("2. Building features for today's inference.")
    features = build_features(execution_date)
    
    logging.info("3. Running Quant Inference.")
    raw_signals = run_quant_model(features)
    
    logging.info("4. Fetching news context for each signal.")
    context_signals = fetch_news_context(raw_signals, execution_date)
    
    logging.info("5. Evaluating with LLM and saving to Postgres.")
    evaluate_signals_and_persist(context_signals, execution_date)
    
    logging.info("Pipeline completed successfully.")