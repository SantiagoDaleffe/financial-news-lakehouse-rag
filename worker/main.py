import pika
import os
import time
import chromadb
import json
from sentence_transformers import SentenceTransformer
import hashlib
from langchain_text_splitters import RecursiveCharacterTextSplitter
from transformers import pipeline

chroma_host = os.getenv("CHROMA_HOST")
print("loading sentence transformer model...", flush=True)
model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')

print("loading finbert sentiment model...", flush=True)
sentiment_model = pipeline('text-classification', model='ProsusAI/finbert')

print("connecting to chromadb...", flush=True)
chroma_client = chromadb.HttpClient(host=chroma_host, port=8000)

while True:
    try:
        chroma_client.heartbeat()
        print("connected to chromadb ok", flush=True)
        break
    except Exception as e:
        print("chromadb connection failed retrying in 5s...", str(e), flush=True)
        time.sleep(5)

collection = chroma_client.get_or_create_collection(name="fin_news_v1")

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
channel.queue_declare(queue="news_queue", durable=True)

text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=500,
    chunk_overlap=50,
    separators=["\n\n", "\n", ".", " ", ""]
)

def callback(ch, method, properties, body):
    try:
        payload = json.loads(body.decode())
        text = payload.get("text", "")
        published_at = payload.get("published_at", 0.0)
        news_url = payload.get("url", "")
    except Exception as e:
        print("Error parsing JSON from RabbitMQ:", str(e), flush=True)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    chunks = text_splitter.split_text(text)
    print("received text split into", len(chunks), "chunks processing...", flush=True)
    
    batch_embeddings = []
    batch_documents = []
    batch_metadatas = []
    batch_ids = []
    
    for i, chunk in enumerate(chunks):
        embedding = model.encode(chunk).tolist()
        
        try:
            sent_result = sentiment_model(chunk[:512])[0]
            sentiment_label = sent_result['label']
            sentiment_score = sent_result['score']
        except Exception as e:
            print("error analyzing sentiment fallback to unknown", str(e), flush=True)
            sentiment_label = 'UNKNOWN'
            sentiment_score = 0.0
            
        uid = hashlib.md5(f"{chunk}_{i}".encode('utf-8')).hexdigest()    
        batch_embeddings.append(embedding)
        batch_documents.append(chunk)
        batch_ids.append(uid)
        
        batch_metadatas.append({
            'source': 'news_api',
            'url': news_url,
            'published_at': float(published_at),
            'sentiment': sentiment_label,
            'sentiment_score': float(sentiment_score)
        })
    
    collection.add(
        embeddings=batch_embeddings,
        documents=batch_documents,
        metadatas=batch_metadatas,
        ids=batch_ids
    )
    
    print("stored", len(batch_ids), "chunks with sentiment data in chromadb", flush=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue="news_queue", on_message_callback=callback, auto_ack=False)

print("worker ready waiting for messages...", flush=True)
channel.start_consuming()