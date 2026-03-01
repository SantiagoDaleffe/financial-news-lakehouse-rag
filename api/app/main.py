from fastapi import FastAPI
from pydantic import BaseModel
import pika
import chromadb
from sentence_transformers import SentenceTransformer
import os
import time
import google.generativeai as genai
import boto3
import json
from datetime import datetime
import hashlib
import mlflow

url = os.getenv('RABBITMQ_URL')
genai.configure(api_key=os.getenv('GEMINI_API_KEY'))

mlflow.set_tracking_uri(os.getenv('MLFLOW_TRACKING_URI'))

mlflow.set_experiment("rag_search_experiment")

llm = genai.GenerativeModel('gemini-3-flash-preview')

s3_client = boto3.client(
    's3',
    endpoint_url=os.getenv('S3_ENDPOINT_URL'),
    aws_access_key_id=os.getenv('MINIO_ROOT_USER'),
    aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'),
    region_name='us-east-1'
)

bucket_name = 'data-lake'

print("Connecting to ChromaDB...", flush=True)
chroma_client = chromadb.HttpClient(host='chromadb', port=8000)

while True:
    try:
        chroma_client.heartbeat()
        print("Connected to ChromaDB!", flush=True)
        break
    except Exception as e:
        print(f"Connection failed: {e}. Retrying in 5 seconds...", flush=True)
        time.sleep(5)

collection = chroma_client.get_or_create_collection(name="fin_news_v1")

print("Loading SentenceTransformer model...", flush=True)
model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')

# parse para extraer el texto
class NewsItem(BaseModel):
    text: str
    
app = FastAPI(title='ingestion-api')

#endpoint de checkeo
@app.get("/health")
def health():
    return {"status": "ok"}

@app.post('/ingest')
def ingest_news(item: NewsItem):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        print(f"Bucket '{bucket_name}' not found. Creating bucket...", flush=True)
        s3_client.create_bucket(Bucket=bucket_name)
        
    today = datetime.now()
    file_hash = hashlib.md5(item.text.encode('utf-8')).hexdigest()[:8]
    
    s3_key = f"raw/news/{today.year}/{today.month:02d}/{today.day:02d}/doc_{file_hash}.json"
    
    payload = {
        "text": item.text,
        'ingested_at':today.isoformat(),
        'source':'api_ingestion'
    }
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(payload),
        ContentType='application/json'
    )
    
    print(f"News item stored in S3 at {s3_key}", flush=True)
    
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='news_queue', durable=True)
    channel.basic_publish(exchange='', routing_key='news_queue', body=item.text)
    connection.close()
    return {'status': 'queued'}

@app.get('/search')
async def search_news(query:str):
    embedding = model.encode(query).tolist()
    results = collection.query(
        query_embeddings=[embedding],
        n_results=5
    )
    
    docs = results["documents"][0]
    if not docs:
        return {"Response": "I don't have enough information in my database."}
    
    documents = results['documents'][0]
    metadatas = results['metadatas'][0]
    
    context = "\n- ".join(documents)
    prompt = f"""
    You are a financial AI assistant.
    Based ONLY on the following news context, answer the user's question.
    If the answer is not in the context, say "I don't have enough information".
    Don't make up answers.
    Context(News):
    - {context}

    Question: {query}
    """
    sources_data = []
    for doc, meta in zip(documents, metadatas):
        safe_meta = meta or {}
        
        sources_data.append({
            'text': doc,
            'sentiment': safe_meta.get('sentiment', 'unknown'),
            'sentiment_score': safe_meta.get('sentiment_score', 0.0)
        })
    
    start = time.time()
    response = await llm.generate_content_async(prompt)
    end = time.time()
        
    latency = end - start
    
    with mlflow.start_run(run_name="search_query"):
        mlflow.log_param("query", query)
        mlflow.log_metric("latency_seconds", latency)
        mlflow.log_param("response", response.text)
        mlflow.set_tag("model_version", 'gemini-3-flash-preview')
    
    return{
        "query": query,
        "response": response.text,
        "sources": sources_data
    }

@app.get('/database-info')
def database_info():
    count = collection.count()
    data = collection.get(limit=5)
    return {"total_items": count, "sample_data": data["documents"][:5]}


