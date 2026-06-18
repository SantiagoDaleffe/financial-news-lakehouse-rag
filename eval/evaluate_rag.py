import json
import os
import time
import mlflow
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

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

os.environ["MLFLOW_TRACKING_URI"] = get_local_uri(os.getenv("MLFLOW_TRACKING_URI"))
os.environ["CHROMA_HOST"] = "localhost"
os.environ['CHROMA_PORT'] = "8001"
 
from api.app.routers.agent import run_agent_with_history

mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
mlflow.set_experiment("RAG_Faithfulness_Evaluation")

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

LLM_JUDGE_PROMPT = """
You are an expert impartial AI evaluator. Your task is to evaluate the quality of an AI Assistant's answer based on a given user query and retrieved context documents.

You must score the response on two metrics from 0.0 to 1.0 (where 1.0 is perfect):

1. **Faithfulness (No Hallucinations):** Does the Assistant's Answer ONLY contain information supported by the Context? If the Assistant mentions facts, numbers, or events not present in the Context, score it lower.
2. **Answer Relevance:** Does the Assistant's Answer directly address the User Query? If it goes off-topic or fails to answer the core question, score it lower.

If the Context explicitly says "No recent news found", and the Assistant correctly states that it doesn't have the information, score both as 1.0 (perfect behavior).

Provide your evaluation EXACTLY in the following JSON format without any markdown blocks or extra text:
{
  "faithfulness_score": 0.0,
  "relevance_score": 0.0,
  "reasoning": "brief explanation of why these scores were given"
}

EVALUATION DATA:
---
User Query: {query}
---
Context Retrieved:
{context}
---
Assistant Answer:
{answer}
---
"""

async def evaluate_rag():
    print("Loading RAG dataset...", flush=True)
    dataset_path = os.path.join(os.path.dirname(__file__), "eval_rag_dataset.json")
    with open(dataset_path, "r") as f:
        dataset = json.load(f)

    total_cases = len(dataset)
    results_log = []
    
    total_faithfulness = 0.0
    total_relevance = 0.0

    print(f"Starting RAG evaluation with {total_cases} cases. Judge: gemini-3.1-pro-preview\n")

    with mlflow.start_run(run_name="Baseline_2_RAG_Faithfulness"):
        start_time = time.time()

        for index, item in enumerate(dataset):
            query = item["query"]
            print(f"[{index+1}/{total_cases}] Query: '{query[:50]}...'")

            try:
                ai_response, sources_data, _, model_used = await run_agent_with_history(
                    query=query, 
                    message_history=[], 
                    user_id="eval_user", 
                    model_override="gemini-2.5-flash"
                )

                raw_context = ""
                if not sources_data:
                    raw_context = "No recent news found in the local database."
                else:
                    for source in sources_data:
                        raw_context += f"- {source['text']}\n"

                judge_prompt_filled = LLM_JUDGE_PROMPT.replace("{query}", query).replace("{context}", raw_context).replace("{answer}", ai_response)
                
                judge_response = client.models.generate_content(
                    model='gemini-3.1-pro',
                    contents=judge_prompt_filled,
                    config=types.GenerateContentConfig(temperature=0.0, response_mime_type="application/json")
                )

                eval_metrics = json.loads(judge_response.text)
                f_score = eval_metrics.get("faithfulness_score", 0.0)
                r_score = eval_metrics.get("relevance_score", 0.0)
                reasoning = eval_metrics.get("reasoning", "")
                
                total_faithfulness += f_score
                total_relevance += r_score

                print(f"Faithfulness: {f_score:.2f} | Relevance: {r_score:.2f}")
                print(f"Judge Note: {reasoning}\n")

                results_log.append({
                    "query": query,
                    "context_length": len(sources_data),
                    "answer": ai_response,
                    "faithfulness": f_score,
                    "relevance": r_score,
                    "reasoning": reasoning
                })
                
            except Exception as e:
                print(f"   -> [ERROR]: {e}\n")
                results_log.append({"query": query, "error": str(e)})
            
            time.sleep(5)

        avg_faithfulness = (total_faithfulness / total_cases) * 100
        avg_relevance = (total_relevance / total_cases) * 100
        total_time = time.time() - start_time

        print(f"Avg Faithfulness (No Hallucination): {avg_faithfulness:.2f}%")
        print(f"Avg Answer Relevance: {avg_relevance:.2f}%")
        print(f"Total time: {total_time:.2f} seconds")

        mlflow.log_metric("avg_faithfulness", avg_faithfulness)
        mlflow.log_metric("avg_relevance", avg_relevance)
        mlflow.log_metric("eval_time_seconds", total_time)
        mlflow.log_param("dataset_size", total_cases)
        mlflow.log_param("agent_model", "gemini-2.5-flash")
        mlflow.log_param("judge_model", "gemini-3.1-pro-preview")
        
        with open("rag_eval_results.json", "w") as f:
            json.dump(results_log, f, indent=4)
        mlflow.log_artifact("rag_eval_results.json")
        os.remove("rag_eval_results.json")

if __name__ == "__main__":
    import asyncio
    asyncio.run(evaluate_rag())
