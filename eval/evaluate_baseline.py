import os
import json
import time
import mlflow
from dotenv import load_dotenv
from google import genai
from google.genai import types
import sys
load_dotenv()
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

PROMPT_PATH = os.path.join(os.path.dirname(__file__), "..", "api", "app", "prompts", "agent_skills.md")
with open(PROMPT_PATH, "r", encoding="utf-8") as f:
    base_system_prompt = f.read()
from api.app.agent_tools import (
    get_live_stock_price, calculate_math, set_price_alert, 
    get_user_alerts, update_price_alert, delete_price_alert,
    execute_paper_trade, get_portfolio_status
)

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
mlflow.set_experiment("Agent_Baseline_Evaluation")

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

system_instruction = base_system_prompt.format(user_id="test_eval_user", tenant_id="public_b2c")
    
eval_config = types.GenerateContentConfig(
    tools=[
        get_live_stock_price, calculate_math, set_price_alert, 
        get_user_alerts, update_price_alert, delete_price_alert,
        execute_paper_trade, get_portfolio_status
    ],
    temperature=0.05,
    system_instruction=system_instruction,
    automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True)
)

MODEL_CASCADE = [
    "gemini-3.5-flash",
    "gemini-3-flash-preview",
    "gemini-2.5-flash",
    "gemini-3.1-flash-lite",
    "gemini-1.5-flash"
]


def evaluate_tool_calling():
    """Evaluate the model's ability to call the correct tool for each query.

    Loads a golden dataset of queries and expected tool names, sends each query
    to the configured GenAI model, and checks whether the model's function
    call matches the expected tool. Tracks and logs accuracy, timing, and
    failed cases to MLflow, and writes a JSON artifact for any failures.

    Side effects:
        - Reads `eval_dataset.json` from the same directory.
        - Uses the global `client` and `eval_config` to call the model.
        - Logs metrics and artifacts to MLflow.
        - Prints per-case pass/fail output to stdout.

    Returns:
        None
    """
    print("Loading golden dataset.", flush=True)
    dataset_path = os.path.join(os.path.dirname(__file__), "eval_dataset.json")
    with open(dataset_path, "r") as f:
        dataset = json.load(f)

    correct_calls = 0
    total_cases = len(dataset)
    failed_cases = []

    print(f"Starting evaluation with {total_cases} cases using Model Cascade...\n")

    with mlflow.start_run(run_name="Baseline_1_Tool_Accuracy_Cascade"):
        start_time = time.time()

        for index, item in enumerate(dataset):
            query = item["query"]
            expected_tool = item["expected_tool"]
            response = None
            called_tool = "none"
            successful_model = "failed_all"

            for model_name in MODEL_CASCADE:
                try:
                    response = client.models.generate_content(
                        model=model_name,
                        contents=query,
                        config=eval_config
                    )
                    successful_model = model_name
                    break
                except Exception as e:
                    print(f"[{index+1}/{total_cases}] Fallback from {model_name} due to API Error {e}.")
                    time.sleep(2)
                    continue

            if successful_model == "failed_all":
                print(f"[{index+1}/{total_cases}] API ERROR | ALL MODELS FAILED")
                failed_cases.append({"query": query, "expected": expected_tool, "called": "API_ERROR", "model": "none"})
                time.sleep(5)
                continue

            if response and response.function_calls:
                called_tool = response.function_calls[0].name
            
            is_correct = False
            if isinstance(expected_tool, list):
                is_correct = called_tool in expected_tool
            else:
                is_correct = called_tool == expected_tool

            if is_correct:
                correct_calls += 1
                print(f"[{index+1}/{total_cases}] PASS | Query: '{query[:30]}.' -> Tool: {called_tool} ({successful_model})")
            else:
                print(f"[{index+1}/{total_cases}] FAIL | Query: '{query[:30]}.' -> Expected: {expected_tool}, Called: {called_tool} ({successful_model})")
                failed_cases.append({"query": query, "expected": expected_tool, "called": called_tool, "model": successful_model})
                
            time.sleep(5)

        accuracy = (correct_calls / total_cases) * 100
        total_time = time.time() - start_time

        print("\n")
        print(f"Tool Calling Accuracy: {accuracy:.2f}%")
        print(f"Total time: {total_time:.2f} seconds")

        mlflow.log_metric("tool_calling_accuracy", accuracy)
        mlflow.log_metric("total_eval_time", total_time)
        mlflow.log_param("dataset_size", total_cases)
        mlflow.log_param("cascade_used", str(MODEL_CASCADE))
        
        if failed_cases:
            with open("failed_cases_log.json", "w") as f:
                json.dump(failed_cases, f, indent=4)
            mlflow.log_artifact("failed_cases_log.json")
            os.remove("failed_cases_log.json")

if __name__ == "__main__":
    evaluate_tool_calling()
