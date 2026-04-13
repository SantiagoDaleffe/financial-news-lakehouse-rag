from fastapi import APIRouter
from pydantic import BaseModel
import pika
import boto3
import json
import os
import hashlib
import time
from datetime import datetime
import chromadb

router = APIRouter(tags=["ingestion"])

s3_client = boto3.client(
    's3',
    endpoint_url=os.getenv('S3_ENDPOINT_URL'),
    aws_access_key_id=os.getenv('MINIO_ROOT_USER'),
    aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'),
    region_name='us-east-1'
)

bucket_name = 'data-lake'
rabbitmq_url = os.getenv('RABBITMQ_URL')

chroma_client = None
news_collection = None

def get_chroma_collection():
    global chroma_client, news_collection
    if news_collection is None:
        chroma_client = chromadb.HttpClient(host="chromadb", port=8000)
        news_collection = chroma_client.get_or_create_collection(name="fin_news_v1")
    return news_collection

class NewsItem(BaseModel):
    text: str
    published_at: float 
    url: str
    
@router.post('/ingest')
def ingest_news(item: NewsItem):
    """
    receives the structured news, saves a backup in S3,
    and enqueues it in RabbitMQ (in JSON format) for asynchronous processing.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        print(f"bucket {bucket_name} not found creating it", flush=True)
        s3_client.create_bucket(Bucket=bucket_name)
        
    today = datetime.now()
    file_hash = hashlib.md5(item.text.encode('utf-8')).hexdigest()[:8]
    
    s3_key = f"raw/news/{today.year}/{today.month:02d}/{today.day:02d}/doc_{file_hash}.json"
    
    payload_s3 = {
            "text": item.text,
            "url": item.url,
            "published_at": item.published_at,
            "ingested_at": today.isoformat(),
            "source": "api_ingestion"
        }
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(payload_s3),
        ContentType='application/json'
    )
    
    print(f"news item saved in s3 {s3_key}", flush=True)
    
    params = pika.URLParameters(rabbitmq_url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='news_queue', durable=True)
    channel.basic_publish(exchange='', routing_key='news_queue', body=item.model_dump_json())
    connection.close()
    
    return {'status': 'queued'}

@router.delete('/prune')
def prune_old_news(days: int = 30):
    """
    Physically remove from ChromaDB all vectors older than 'days'.
    """
    cutoff_timestamp = time.time() - (days * 86400)
    

    col = get_chroma_collection()
    
    col.delete(
        where={"published_at": {"$lt": cutoff_timestamp}}
    )
    
    print(f"Purge completed: News older than {days} days has been removed.", flush=True)
    return {"status": "ok", "deleted_older_than_days": days}

