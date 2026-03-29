from fastapi import APIRouter
import numpy as np
import chromadb
from sentence_transformers import SentenceTransformer
import os
import _hashlib
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
    delete_price_alert
)
from ..semantic_cache import SemanticCache

router = APIRouter(tags=["agent"])

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
mlflow.set_experiment("rag_search_experiment")

client = genai.Client(api_key=os.getenv("GENAI_API_KEY"))

agent_config = types.GenerateContentConfig(
    tools=[get_live_stock_price, calculate_math, set_price_alert],
    temperature=0.0,
    system_instruction="You are an Autonomous Financial Assistant. You have access to a local news database (RAG Context) and external tools. If the request requires live market data or precise math calculations, use the available tools.",
)

print("connecting to chromadb...", flush=True)
chroma_client = chromadb.HttpClient(host="chromadb", port=8000)

while True:
    try:
        chroma_client.heartbeat()
        print("connected to chromadb", flush=True)
        break
    except Exception as e:
        print(f"connection failed {e} retrying in 5s", flush=True)
        time.sleep(5)

collection = chroma_client.get_or_create_collection(name="fin_news_v1")

print("loading sentence transformer model...", flush=True)
model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
cache = SemanticCache(chroma_client, model)

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
    "what are the macroeconomic factors affecting this stock"
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
    "goodbye"
]

complex_embeddings = model.encode(COMPLEX_PROTOTYPES)
simple_embeddings = model.encode(SIMPLE_PROTOTYPES)

def get_routing_complexity(query:str) -> str:
    """
    Calculates semantic similarity between the prompt and the prototypes
    for fallback selection.
    """
    query_emb = model.encode([query])[0]
    complex_score = np.max(np.dot(complex_embeddings, query_emb) / (np.linalg.norm(complex_embeddings, axis=1) * np.linalg.norm(query_emb)))
    simple_score = np.max(np.dot(simple_embeddings, query_emb) / (np.linalg.norm(simple_embeddings, axis=1) * np.linalg.norm(query_emb)))
    
    return "complex" if complex_score > simple_score else "simple"    


@router.get("/search")
async def search_news(query: str):
    """
    Queries the vector database for relevant context and passes it
    to the LLM agent to generate a grounded response.
    """
    embedding = model.encode(query).tolist()
    results = collection.query(query_embeddings=[embedding], n_results=5)

    docs = results.get("documents", [[]])[0]
    if not docs:
        context = "No recent news found in the local database."
        sources_data = []
    else:
        documents = results["documents"][0]
        metadatas = results["metadatas"][0]
        context = "\n- ".join(documents)

        sources_data = []
        for doc, meta in zip(documents, metadatas):
            safe_meta = meta or {}
            sources_data.append(
                {
                    "text": doc,
                    "sentiment": safe_meta.get("sentiment", "unknown"),
                    "sentiment_score": safe_meta.get("sentiment_score", 0.0),
                }
            )

    prompt = f"""
    Local Context (News):
    {context}
    
    User Question: {query}
    
    Instructions: If the question can be answered using the Local Context, do so. 
    If it requires current market prices or calculations, use your tools.
    """

    start = time.time()

    chat = client.aio.chats.create(model="gemini-3-flash-preview", config=agent_config)

    response = await chat.send_message(prompt)

    latency = time.time() - start

    with mlflow.start_run(run_name="agent_query"):
        mlflow.log_param("query", query)
        mlflow.log_metric("latency_seconds", latency)
        mlflow.log_param("response", response.text)
        mlflow.set_tag("model_version", "gemini-3-flash-preview")
        mlflow.set_tag("architecture", "RAG + Agents")

    return {"query": query, "response": response.text, "sources": sources_data}


async def run_agent_with_history(query: str, message_history, user_id: str, model_override: str = None):
    """
    Fetches fresh context from ChromaDB, formats the database history for Gemini,
    and generates a response with memory and updated data, injected with user context.
    """
    is_cached = False
    
    transactional_keywords = ['alert', 'create', 'delete', 'notify', 'my', 'have', 'account'] 
    
    is_transactional = any(word in query.lower() for word in transactional_keywords)
    
    if not is_transactional:
        cached_response = cache.check(query, threshold=0.15, ttl_seconds=300)
        if cached_response:
            is_cached = True
            return cached_response, [], is_cached, 'cache_hit'
    
    else:
        print('Transactional query detected. Avoiding semantic caching for security.', flush=True)
    
    embedding = model.encode(query).tolist()
    results = collection.query(query_embeddings=[embedding], n_results=5)

    docs = results.get("documents", [[]])[0]
    if not docs:
        context = "No recent news found in the local database."
        sources_data = []
    else:
        documents = results["documents"][0]
        metadatas = results["metadatas"][0]
        context = "\n- ".join(documents)

        sources_data = []
        for doc, meta in zip(documents, metadatas):
            safe_meta = meta or {}
            sources_data.append(
                {
                    "text": doc,
                    "sentiment": safe_meta.get("sentiment", "unknown"),
                    "sentiment_score": safe_meta.get("sentiment_score", 0.0),
                }
            )

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
        
    dynamic_config = types.GenerateContentConfig(
    tools=[
        get_live_stock_price, 
        calculate_math, 
        set_price_alert, 
        get_user_alerts, 
        update_price_alert, 
        delete_price_alert
    ],
    temperature=0.0,
    system_instruction=f"""You are an Autonomous Financial Assistant. You have access to a local news database (RAG Context) and external tools.
    CRITICAL RULES:
    1. If the user asks for current prices, calculations, or alerts, YOU MUST USE THE TOOLS. Do not rely on local news for live prices.
    2. The current user's ID is '{user_id}'. You must use this EXACT ID when calling any tool that requires a user_id parameter. Never ask the user for their ID."""
)
    if model_override:
        complexity = "manual_override"
        model_cascade = [model_override]
    else:
        complexity = get_routing_complexity(query)
        
        if complexity == 'complex':
            model_cascade = ["gemini-3.1-pro-preview", "gemini-2.5-pro", "gemini-3-flash-preview"]
        else:
            model_cascade = ["gemini-3-flash-preview", "gemini-3.1-flash-lite", "gemini-2.5-flash-lite"]
        
    print(f'Model chosen: {complexity}. Cascade: {model_cascade}', flush=True)

    start = time.time()
    response_text=None
    model_used=None
    
    for model_name in model_cascade:
        try:
            print(f'Trying inference with {model_name}...', flush=True)
            chat= client.aio.chats.create(
                model=model_name,
                config=dynamic_config,
                history=gemini_history
            )
            response = await chat.send_message(enriched_prompt)
            response_text = response.text
            model_used = model_name
            break
        
        except Exception as e:
            print(f'[FALLBACK] Error with {model_name}: {e}.', flush=True)
            continue
    
    
    latency = time.time() - start
    
    if not response_text:
        response_text = "Too many requests. Try again later."
        model_used = "failed_all"
        
    if response_text and model_used != "failed_all" and not is_transactional:
        cache.save(query, response_text)
        
    with mlflow.start_run(run_name="chat_with_history"):
        mlflow.log_param("query", query)
        mlflow.log_metric("latency_seconds", latency)
        mlflow.log_param("history_length", len(gemini_history))
        mlflow.log_param("routing_complexity", complexity)
        mlflow.set_tag("model_version", model_used)
        mlflow.set_tag("architecture", "RAG + Agents + Tools + Semantic Routing")

    return response_text, sources_data, is_cached, model_used
