import os
import socket
from urllib.parse import urlparse
def get_local_uri(uri, default_host="localhost"):
    if not uri: return None
    try:
        hostname = urlparse(uri).hostname
        if hostname: socket.gethostbyname(hostname)
        return uri
    except socket.gaierror:
        parsed = urlparse(uri)
        if parsed.hostname: return uri.replace(parsed.hostname, default_host)
    return uri

os.environ["MLFLOW_TRACKING_URI"] = get_local_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
os.environ["DATABASE_URL"] = get_local_uri(os.getenv("DATABASE_URL", "postgresql://airflow:airflow@airflow-postgres:5432/airflow"))


import json
import time
import mlflow
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

from api.app.agent_tools import (
    get_live_stock_price, calculate_math, set_price_alert, 
    get_user_alerts, update_price_alert, delete_price_alert,
    execute_paper_trade, get_portfolio_status
)

mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
mlflow.set_experiment("Agent_Baseline_Evaluation")

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

system_instruction = """
    You are an Institutional-Level Quantitative Analyst and Risk Manager. Your objective is to assist the user in financial decisions, manage their simulated portfolio, and analyze the market.

    SYSTEM CONTEXT:
    - Current user ID: test_eval_user (Use this EXACT ID whenever a tool requires it).

    STRICT OPERATING RULES:
    1. Prices and Market: NEVER assume or invent a price. ALWAYS use `get_live_stock_price`.
    2. News (RAG): Base your fundamental analysis ONLY on the provided local context.
    3. Alerts: If the user requests that you alert or notify them about a price, you MUST use `set_price_alert`.
    4. Math: Use `calculate_math` for any calculations.

    INTERNAL REASONING PROCESS (You must follow this order):
    Step 1 (Intent): Classify whether the user is looking for analysis, wants to set an alert, or wants to execute a trade.
    Step 2 (Validation): If it's a trade or they're asking to see their account, execute `get_portfolio_status` FIRST.
    Step 3 (Risk): Warn about exposure.
    Step 4 (Execution): Execute or bounce.
    Step 5 (Synthesis): Deliver response.
    """
    
eval_config = types.GenerateContentConfig(
    tools=[
        get_live_stock_price, calculate_math, set_price_alert, 
        get_user_alerts, update_price_alert, delete_price_alert,
        execute_paper_trade, get_portfolio_status
    ],
    temperature=0.0,
    system_instruction=system_instruction,

    automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True)
)

def evaluate_tool_calling():
    print("Loading golden dataset...", flush=True)
    dataset_path = os.path.join(os.path.dirname(__file__), "eval_dataset.json")
    with open(dataset_path, "r") as f:
        dataset = json.load(f)

    correct_calls = 0
    total_cases = len(dataset)
    failed_cases = []

    print(f"Starting evaluation with {total_cases} cases with gemini-2.5-flash\n")

    with mlflow.start_run(run_name="Baseline_1_Tool_Accuracy"):
        start_time = time.time()

        for index, item in enumerate(dataset):
            query = item["query"]
            expected_tool = item["expected_tool"]

            try:
                response = client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=query,
                    config=eval_config
                )

                called_tool = "none"
                text_response = "No text provided."
                
                try:
                    if response.text:
                        text_response = response.text.replace('\n', ' ')
                except ValueError:
                    pass

                if response.function_calls:
                    called_tool = response.function_calls[0].name
                else:
                    print(f"[{index+1}/{total_cases}] NO FUNCTION CALLS | Query: '{query[:30]}...'\n[DEBUG TEXT] {text_response}\n", flush=True)

                is_correct = False
                if isinstance(expected_tool, list):
                    is_correct = called_tool in expected_tool
                else:
                    is_correct = called_tool == expected_tool

                if is_correct:
                    correct_calls += 1
                    print(f"[{index+1}/{total_cases}] PASS | Query: '{query[:30]}...' -> Tool: {called_tool}")
                else:
                    print(f"[{index+1}/{total_cases}] FAIL | Query: '{query[:30]}...' -> Expected: {expected_tool}, Called: {called_tool}")
                    failed_cases.append({"query": query, "expected": expected_tool, "called": called_tool})
                    
            except Exception as e:
                print(f"[{index+1}/{total_cases}] API ERROR | {e}")
                failed_cases.append({"query": query, "expected": expected_tool, "called": f"API_ERROR: {e}"})
            
            time.sleep(4)

        accuracy = (correct_calls / total_cases) * 100
        total_time = time.time() - start_time

        print("\n")
        print(f"Tool Calling Accuracy: {accuracy:.2f}%")
        print(f"Total time: {total_time:.2f} seconds")

        mlflow.log_metric("tool_calling_accuracy", accuracy)
        mlflow.log_metric("total_eval_time", total_time)
        mlflow.log_param("dataset_size", total_cases)
        mlflow.log_param("model_used", "gemini-2.5-flash")
        
        if failed_cases:
            with open("failed_cases_log.json", "w") as f:
                json.dump(failed_cases, f, indent=4)
            mlflow.log_artifact("failed_cases_log.json")
            os.remove("failed_cases_log.json")

if __name__ == "__main__":
    evaluate_tool_calling()