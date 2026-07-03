from fastapi import APIRouter
from pydantic import BaseModel
import pika
import boto3
import json
import os
import hashlib
import time
from datetime import datetime
from datetime import timezone, timedelta
from pinecone import Pinecone

router = APIRouter(tags=["ingestion"])

s3_client = boto3.client(
    's3',
    endpoint_url=os.getenv('S3_ENDPOINT_URL'),
    aws_access_key_id=os.getenv('MINIO_ROOT_USER'),
    aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'),
    region_name='us-east-1'
)

bucket_name = os.getenv("S3_BUCKET_NAME")
rabbitmq_url = os.getenv('RABBITMQ_URL')

pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
index = pc.Index(os.getenv("PINECONE_INDEX_NAME"))

class NewsItem(BaseModel):
    text: str
    published_at: float 
    url: str
    tickers: list[str] = []
    
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
            "tickers": item.tickers,
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
    cutoff_timestamp = time.time() - (days * 86400)
    
    try:
        index.delete(filter={"published_at": {"$lt": cutoff_timestamp}}, namespace="fin_news_v1")
    except Exception as e:
        print(f"Error pruning Pinecone news: {e}", flush=True)

    try:
        index.delete(filter={"timestamp": {"$lt": cutoff_timestamp}}, namespace="semantic_cache_v1")
    except Exception as e:
        print(f"Error pruning Pinecone cache: {e}", flush=True)

    try:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        paginator = s3_client.get_paginator('list_objects_v2')
        deleted_s3 = 0
        
        for page in paginator.paginate(Bucket=bucket_name, Prefix='raw/news/'):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['LastModified'] < cutoff_date:
                        s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                        deleted_s3 += 1
                        
        print(f"S3 prune completed. Deleted {deleted_s3} raw files.", flush=True)
    except Exception as e:
        print(f"Error during S3 pruning: {str(e)}", flush=True)
        return {"status": "error", "message": str(e)}

    print(f"Purge completed: Data older than {days} days has been removed.", flush=True)
    return {"status": "ok", "deleted_older_than_days": days}

