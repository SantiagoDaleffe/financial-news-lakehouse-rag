from fastapi import APIRouter
import chromadb
from sentence_transformers import SentenceTransformer
import os
import time
import mlflow
from google import genai
from google.genai import types

from ..agent_tools import get_live_stock_price, calculate_math, set_price_alert

router = APIRouter(tags=["agent"])

mlflow.set_tracking_uri(os.getenv('MLFLOW_TRACKING_URI'))
mlflow.set_experiment("rag_search_experiment")

client = genai.Client(api_key=os.getenv('GENAI_API_KEY'))

agent_config = types.GenerateContentConfig(
    tools=[get_live_stock_price, calculate_math, set_price_alert],
    temperature=0.0,
    system_instruction="You are an Autonomous Financial Assistant. You have access to a local news database (RAG Context) and external tools. If the request requires live market data or precise math calculations, use the available tools."
)

print("connecting to chromadb...", flush=True)
chroma_client = chromadb.HttpClient(host='chromadb', port=8000)

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
model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')

@router.get('/search')
async def search_news(query: str):
    """
    Queries the vector database for relevant context and passes it 
    to the LLM agent to generate a grounded response.
    """
    embedding = model.encode(query).tolist()
    results = collection.query(
        query_embeddings=[embedding],
        n_results=5
    )
    
    docs = results.get("documents", [[]])[0]
    if not docs:
        context = "No recent news found in the local database."
        sources_data = []
    else:
        documents = results['documents'][0]
        metadatas = results['metadatas'][0]
        context = "\n- ".join(documents)
        
        sources_data = []
        for doc, meta in zip(documents, metadatas):
            safe_meta = meta or {}
            sources_data.append({
                'text': doc,
                'sentiment': safe_meta.get('sentiment', 'unknown'),
                'sentiment_score': safe_meta.get('sentiment_score', 0.0)
            })
    
    prompt = f"""
    Local Context (News):
    {context}
    
    User Question: {query}
    
    Instructions: If the question can be answered using the Local Context, do so. 
    If it requires current market prices or calculations, use your tools.
    """
    
    start = time.time()
    
    chat = client.aio.chats.create(
        model='gemini-3-flash-preview',
        config=agent_config
    )
    
    response = await chat.send_message(prompt)
    
    latency = time.time() - start

    with mlflow.start_run(run_name="agent_query"):
        mlflow.log_param("query", query)
        mlflow.log_metric("latency_seconds", latency)
        mlflow.log_param("response", response.text)
        mlflow.set_tag("model_version", 'gemini-3-flash-preview')
        mlflow.set_tag("architecture", "RAG + Agents")
    
    return {
        "query": query,
        "response": response.text,
        "sources": sources_data
    }