import pika
import os
import time
from pinecone import Pinecone
import json
from sentence_transformers import SentenceTransformer
import hashlib
from langchain_text_splitters import RecursiveCharacterTextSplitter
from transformers import pipeline

MAX_RETRIES = 3

print("loading sentence transformer model...", flush=True)
model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')

print("loading finbert sentiment model...", flush=True)
sentiment_model = pipeline('text-classification', model='ProsusAI/finbert')

print("Connecting to Pinecone...", flush=True)
pinecone = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
index = pinecone.Index(name=os.getenv("PINECONE_INDEX_NAME"))

url = os.getenv("RABBITMQ_URL")
params = pika.URLParameters(url)

connection = None
while True:
    try:
        print("connecting to rabbitmq...", flush=True)
        connection = pika.BlockingConnection(params)
        print("connected to rabbitmq ok")
        break
    except pika.exceptions.AMQPConnectionError as e:
        print("rabbitmq connection failed retrying in 5s...", str(e), flush=True)
        time.sleep(5)
        
channel = connection.channel()
channel.queue_declare(queue="news_dlq", durable=True)
retry_args = {
    "x-dead-letter-exchange": "",
    "x-dead-letter-routing-key": "news_queue",
    "x-message-ttl": 10000 
}
channel.queue_declare(queue="news_retry_queue", durable=True, arguments=retry_args)
channel.queue_declare(queue="news_queue", durable=True)

text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=500,
    chunk_overlap=50,
    separators=["\n\n", "\n", ".", " ", ""]
)

def callback(ch, method, properties, body):
    headers = properties.headers or {}
    retry_count = headers.get('retry_count', 0)
    
    try:
        try:
            payload = json.loads(body.decode())
            text = payload.get("text", "")
            published_at = payload.get("published_at", 0.0)
            news_url = payload.get("url", "")
            tickers = payload.get("tickers", [])
        except Exception as e:
            print("Error parsing JSON from RabbitMQ:", str(e), flush=True)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return


        if not tickers:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return


        try:
            sent_result = sentiment_model(text[:512])[0]
            sentiment_label = sent_result['label'].upper()
            sentiment_score = float(sent_result['score'])
        except Exception:
            sentiment_label = 'UNKNOWN'
            sentiment_score = 0.0
            

        chunks = text_splitter.split_text(text)
        batch_vectors = []
        
        for i, chunk in enumerate(chunks):
            embedding = model.encode(chunk).tolist()
            uid = hashlib.md5(f"{chunk}_{i}_{published_at}".encode('utf-8')).hexdigest()
            
            metadata = {
                'text': chunk, 
                'source': 'news_api',
                'url': news_url,
                'published_at': float(published_at),
                'sentiment': sentiment_label,
                'sentiment_score': sentiment_score,
                'ticker_principal': tickers[0],
                'tickers_relacionados': ",".join(tickers)
            }
            batch_vectors.append((uid, embedding, metadata))
            
        if batch_vectors:
            index.upsert(vectors=batch_vectors, namespace="fin_news_v1")
            print(f"Stored {len(batch_vectors)} chunks for {tickers} | Sentiment: {sentiment_label}", flush=True)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"CRITICAL ERROR {str(e)}", flush=True)
        if retry_count < MAX_RETRIES:
            headers['retry_count'] = retry_count + 1
            properties.headers = headers
            ch.basic_publish(exchange='', routing_key='news_retry_queue', body=body, properties=properties)
        else:
            ch.basic_publish(exchange='', routing_key='news_dlq', body=body, properties=properties)
        ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue="news_queue", on_message_callback=callback, auto_ack=False)
channel.start_consuming()