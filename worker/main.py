import pika
import os
import time
import chromadb
from sentence_transformers import SentenceTransformer
import hashlib
from langchain_text_splitters import RecursiveCharacterTextSplitter
from transformers import pipeline

print("Loading SentenceTransformer model...", flush=True)
model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')

print("Loading Sentiment model...", flush=True)
sentiment_model = pipeline('text-classification', model='ProsusAI/finbert')

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

url = os.getenv("RABBITMQ_URL")
params = pika.URLParameters(url)

connection = None
while True:
    try:
        print("Trying to connect to RabbitMQ...", flush=True)
        connection = pika.BlockingConnection(params)
        print("Connected to RabbitMQ!")
        break
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Connection failed: {e}. Retrying in 5 seconds...", flush=True)
        time.sleep(5)
        
channel = connection.channel()
channel.queue_declare(queue="news_queue", durable=True)

text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=500,
    chunk_overlap=50,
    separators=["\n\n", "\n", ".", " ", ""]
)

def callback(ch, method, properties, body):
    text = body.decode()
    chunks = text_splitter.split_text(text)
    print(f"Received text, split into {len(chunks)} chunks. Processing first chunk...", flush=True)
    
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
            print(f'Error analyzing sentiment: {e}')
            sentiment_label = 'UNKNOWN'
            sentiment_score = 0.0
            
        uid = hashlib.md5(f"{chunk}_{i}".encode('utf-8')).hexdigest()    
        batch_embeddings.append(embedding)
        batch_documents.append(chunk)
        batch_ids.append(uid)
        
        batch_metadatas.append({
            'source': 'news_api',
            "sentiment": sentiment_label,
            "sentiment_score": float(sentiment_score)
        })
    
    collection.add(
        embeddings=batch_embeddings,
        documents=batch_documents,
        metadatas=batch_metadatas,
        ids=batch_ids
    )
    
    print(f"Processed and stored {len(batch_ids)} chunks with sentiment analysis", flush=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue="news_queue", on_message_callback=callback, auto_ack=False)

print("Waiting for messages. To exit press CTRL+C", flush=True)
channel.start_consuming()
