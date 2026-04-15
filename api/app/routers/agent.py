from fastapi import APIRouter
import numpy as np
import chromadb
from sentence_transformers import SentenceTransformer
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
    get_portfolio_status
)
from ..semantic_cache import SemanticCache

router = APIRouter(tags=["agent"])

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
mlflow.set_experiment("rag_search_experiment")

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
chroma_host = os.getenv("CHROMA_HOST")


print("connecting to chromadb...", flush=True)
chroma_client = chromadb.HttpClient(host=chroma_host, port=8000)

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

async def run_agent_with_history(query: str, message_history, user_id: str, model_override: str = None):
    """
    Fetches fresh context from ChromaDB, formats the database history for Gemini,
    and generates a response with memory and updated data, injected with user context.
    """
    is_cached = False
    
    transactional_keywords = ['alert', 'create', 'delete', 'notify', 'my', 'have', 'account', 'buy', 'sell', 'trade', 'portfolio', 'position'] 
    
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
        delete_price_alert,
        execute_paper_trade,
        get_portfolio_status
    ],
    temperature=0.0,
    system_instruction=f"""
    You are an Institutional-Level Quantitative Analyst and Risk Manager. Your objective is to assist the user in financial decisions, manage their simulated portfolio, and analyze the market.

    SYSTEM CONTEXT:
    - Current user ID: {user_id} (Use this EXACT ID whenever a tool requires it).

    STRICT OPERATING RULES:
    1. Prices and Market: NEVER assume or invent a price. ALWAYS use `get_live_stock_price`.
    2. News (RAG): Base your fundamental analysis ONLY on the provided local context. If there is no relevant news about a ticker, state this explicitly ("I have no recent news about X"). NEVER invent macroeconomic events.
    3. Alerts: If the user requests that you alert or notify them about a price, you MUST use `set_price_alert`.
    4. Math: Use `calculate_math` for any calculations." Percentages, averages, or returns. Don't do mental math.

    INTERNAL REASONING PROCESS (You must follow this order):
    Step 1 (Intent): Classify whether the user is looking for analysis, wants to set an alert, or wants to execute a trade.

    Step 2 (Validation): If it's a trade or they're asking to see their account, execute `get_portfolio_status` FIRST.

    Step 3 (Risk): If a buy order requires more than 50% of their available USD balance, execute the order but clearly warn about the exposure and lack of diversification.

    Step 4 (Execution): If there isn't enough balance for a trade, bounce the order, mathematically detailing the difference, and suggest buying the maximum amount the balance allows.

    Step 5 (Synthesis): Deliver your final response in a structured, direct, and professional manner.

    CRITICAL CONSTRAINT: If a tool returns an error (e.g., (Ticker not found, database error), explain it to the user. NEVER simulate or fabricate that a transaction was successful if the tool failed.
    """
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
