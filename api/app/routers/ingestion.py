from fastapi import APIRouter
from pydantic import BaseModel
import pika
import boto3
import json
import os
import hashlib
from datetime import datetime

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

class NewsItem(BaseModel):
    text: str

@router.post('/ingest')
def ingest_news(item: NewsItem):
    """
    Receives raw text, stores it as a JSON payload in S3, 
    and publishes a message to RabbitMQ for async processing.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        print(f"bucket {bucket_name} not found creating it", flush=True)
        s3_client.create_bucket(Bucket=bucket_name)
        
    today = datetime.now()
    file_hash = hashlib.md5(item.text.encode('utf-8')).hexdigest()[:8]
    
    s3_key = f"raw/news/{today.year}/{today.month:02d}/{today.day:02d}/doc_{file_hash}.json"
    
    payload = {
        "text": item.text,
        'ingested_at': today.isoformat(),
        'source': 'api_ingestion'
    }
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(payload),
        ContentType='application/json'
    )
    
    print(f"news item saved in s3 {s3_key}", flush=True)
    
    params = pika.URLParameters(rabbitmq_url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='news_queue', durable=True)
    channel.basic_publish(exchange='', routing_key='news_queue', body=item.text)
    connection.close()
    
    return {'status': 'queued'}