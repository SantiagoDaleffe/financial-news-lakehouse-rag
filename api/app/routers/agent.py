from fastapi import APIRouter
import numpy as np
from pinecone import Pinecone
from sentence_transformers import SentenceTransformer, CrossEncoder
import os
import time
import mlflow
from google import genai
from google.genai import types
from ..agent_tools import (
    get_live_stock_price,
    calculate_math,
    set_price_alert,
    get_user_alerts,
    update_price_alert,
    delete_price_alert,
    execute_paper_trade,
    get_portfolio_status,
)
from ..semantic_cache import SemanticCache

router = APIRouter(tags=["agent"])

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
mlflow.set_experiment("rag_search_experiment")

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

print("Connecting to Pinecone...", flush=True)
pinecone = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
index = pinecone.Index(os.getenv("PINECONE_INDEX_NAME"))

print("loading sentence transformer model...", flush=True)
model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
cache = SemanticCache(pinecone, model)

print("Loading cross-encoder reranker model...", flush=True)
reranker = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")

COMPLEX_PROTOTYPES = [
    "do a comparative analysis",
    "what is the correlation between",
    "explain the financial impact",
    "detailed market summary",
    "what does this mean for my investments",
    "future projection of the company",
    "give me your opinion on the balance sheet",
    "why is the market crashing",
    "assess the credit risk of this portfolio",
    "what are the macroeconomic factors affecting this stock",
]

SIMPLE_PROTOTYPES = [
    "hello",
    "tesla price",
    "delete alert",
    "thank you",
    "create alert",
    "how many shares do I have",
    "cancel my notification",
    "how are you",
    "what is the value of btc",
    "goodbye",
]

complex_embeddings = model.encode(COMPLEX_PROTOTYPES)
simple_embeddings = model.encode(SIMPLE_PROTOTYPES)


def get_routing_complexity(query: str) -> str:
    """Classify a user query as complex or simple using semantic similarity.

    Args:
        query (str): The user query to evaluate.

    Returns:
        str: "complex" if the query is more semantically aligned with the complex prototype set,
             otherwise "simple".
    """
    query_emb = model.encode([query])[0]
    complex_score = np.max(
        np.dot(complex_embeddings, query_emb)
        / (np.linalg.norm(complex_embeddings, axis=1) * np.linalg.norm(query_emb))
    )
    simple_score = np.max(
        np.dot(simple_embeddings, query_emb)
        / (np.linalg.norm(simple_embeddings, axis=1) * np.linalg.norm(query_emb))
    )
    return "complex" if complex_score > simple_score else "simple"


async def run_agent_with_history(
    query: str,
    message_history,
    user_id: str,
    tenant_id: str,
    model_override: str = None,
    has_pii: bool = False
):
    """Run the conversational agent with optional historical context.

    Args:
        query (str): The current user query to process.
        message_history (list|dict): The prior conversation history used to maintain context.
        user_id (str): The identifier for the requesting user.
        tenant_id (str): The identifier for the tenant or workspace.
        model_override (str, optional): Optional model name to use instead of the default. Defaults to None.

    Returns:
        tuple: A tuple containing the agent response or cached response, the updated message history,
            a boolean indicating whether the response was retrieved from cache, and a string
            indicating the response source.
    """
    is_cached = False
    transactional_keywords = [
        "alert",
        "create",
        "delete",
        "notify",
        "my",
        "have",
        "account",
        "buy",
        "sell",
        "trade",
        "portfolio",
        "position",
    ]

    is_transactional = any(word in query.lower() for word in transactional_keywords)

    if not is_transactional and not has_pii:
        cached_response = cache.check(query, threshold=0.15, ttl_seconds=300)
        if cached_response:
            is_cached = True
            return cached_response, [], is_cached, "cache_hit"
    else:
        print("Transactional or PII query detected. Bypassing semantic caching.", flush=True)

    embedding = model.encode(query).tolist()

    global_results = index.query(
        vector=embedding, top_k=10, include_metadata=True, namespace="fin_news_v1"
    )

    tenant_results = index.query(
        vector=embedding,
        top_k=10,
        include_metadata=True,
        namespace=f"tenant_{tenant_id}",
    )

    all_matches = global_results.get("matches", []) + tenant_results.get("matches", [])
    unique_matches = {}
    for m in all_matches:
        if m["id"] not in unique_matches:
            unique_matches[m["id"]] = {
                "id": m["id"],
                "text": m["metadata"]["text"],
                "sentiment": m["metadata"].get("sentiment", "unknown"),
                "sentiment_score": m["metadata"].get("sentiment_score", 0.0),
            }
    matches = list(unique_matches.values())

    if matches:
        rerank_inputs = [(query, m["text"]) for m in matches]
        rerank_scores = reranker.predict(rerank_inputs)

        for idx, m in enumerate(matches):
            m["rerank_score"] = float(rerank_scores[idx])

        matches = sorted(matches, key=lambda x: x["rerank_score"], reverse=True)
        matches = matches[:4]

    if not matches:
        context = "No recent news found in the local database."
        sources_data = []
    else:
        documents = [m["text"] for m in matches]
        context = "\n- ".join(documents)
        sources_data = matches

    enriched_prompt = f"""
    Local Context (News):
    {context}
    
    User Question: {query}
    
    Instructions: If the question can be answered using the Local Context, do so. 
    If it requires current market prices or calculations, use your tools.
    """

    gemini_history = []
    for message in message_history:
        gemini_history.append(
            types.Content(
                role=message.role, parts=[types.Part.from_text(text=message.content)]
            )
        )

    prompt_path = os.path.join(os.path.dirname(__file__), "..", "prompts", "agent_skills.md")
    with open(prompt_path, "r", encoding="utf-8") as f:
        base_system_prompt = f.read()

    system_instruction_text = base_system_prompt.format(user_id=user_id, tenant_id=tenant_id)

    dynamic_config = types.GenerateContentConfig(
        tools=[
            get_live_stock_price,
            calculate_math,
            set_price_alert,
            get_user_alerts,
            update_price_alert,
            delete_price_alert,
            execute_paper_trade,
            get_portfolio_status,
        ],
        temperature=0.05,
        system_instruction=system_instruction_text,
    )

    if model_override:
        complexity = "manual_override"
        model_cascade = [model_override]
    else:
        complexity = get_routing_complexity(query)
        if complexity == "complex":
            model_cascade = [
                "gemini-3.1-pro-preview",
                "gemini-2.5-pro",
                "gemini-3.5-flash",
            ]
        else:
            model_cascade = [
                "gemini-3-flash-preview",
                "gemini-3.1-flash-lite",
                "gemini-2.5-flash-lite",
                "gemini-2.5-flash",
            ]

    print(f"Model chosen: {complexity}. Cascade: {model_cascade}", flush=True)

    start = time.time()
    response_text = None
    model_used = None

    for model_name in model_cascade:
        try:
            print(f"Trying inference with {model_name}...", flush=True)
            chat = client.aio.chats.create(
                model=model_name, config=dynamic_config, history=gemini_history
            )
            response = await chat.send_message(enriched_prompt)
            response_text = response.text
            model_used = model_name
            break

        except Exception as e:
            print(f"[FALLBACK] Error with {model_name}: {e}.", flush=True)
            continue

    latency = time.time() - start

    if not response_text:
        response_text = "Too many requests. Try again later."
        model_used = "failed_all"

    if response_text and model_used != "failed_all" and not is_transactional and not has_pii:
        cache.save(query, response_text)

    with mlflow.start_run(run_name="chat_with_history", nested=True):
        mlflow.log_param("query", query)
        mlflow.log_metric("latency_seconds", latency)
        mlflow.log_param("history_length", len(gemini_history))
        mlflow.log_param("routing_complexity", complexity)
        mlflow.set_tag("model_version", model_used)
        mlflow.set_tag("architecture", "RAG + Agents + Tools + Semantic Routing")

    return response_text, sources_data, is_cached, model_used
