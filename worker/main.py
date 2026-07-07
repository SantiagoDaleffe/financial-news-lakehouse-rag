import pika
import os
import time
import json
import hashlib
from pinecone import Pinecone
from sentence_transformers import SentenceTransformer
from langchain_text_splitters import RecursiveCharacterTextSplitter
from transformers import pipeline
from api.app.schemas import NewsItem

MAX_RETRIES = 3

print("Loading Sentence Transformer embedding model.", flush=True)
model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

print("Loading FinBERT sentiment classification model.", flush=True)
sentiment_model = pipeline("text-classification", model="ProsusAI/finbert")

print("Establishing connection to Pinecone Vector Database.", flush=True)
pinecone = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
index = pinecone.Index(name=os.getenv("PINECONE_INDEX_NAME"))

url = os.getenv("RABBITMQ_URL")
params = pika.URLParameters(url)

connection = None
while True:
    try:
        print("Initializing connection to RabbitMQ Message Broker.", flush=True)
        connection = pika.BlockingConnection(params)
        print("Message Broker connection established successfully.")
        break
    except pika.exceptions.AMQPConnectionError as e:
        print(
            "Message Broker connection failed. Retrying in 5 seconds.",
            str(e),
            flush=True,
        )
        time.sleep(5)

channel = connection.channel()
channel.queue_declare(queue="news_dlq", durable=True)

# Configuration for the Retry Mechanism (Dead Letter Exchanges)
retry_args = {
    "x-dead-letter-exchange": "",
    "x-dead-letter-routing-key": "news_queue",
    "x-message-ttl": 10000,
}
channel.queue_declare(queue="news_retry_queue", durable=True, arguments=retry_args)
channel.queue_declare(queue="news_queue", durable=True)

text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=500, chunk_overlap=50, separators=["\n\n", "\n", ".", " ", ""]
)


def callback(ch, method, properties, body):
    """
    Core consumer callback for processing asynchronous news ingestion events.

    1. Deserializes the JSON payload.
    2. Applies FinBERT for financial sentiment classification.
    3. Fragments the document via LangChain's RecursiveCharacterTextSplitter.
    4. Generates dense vector embeddings using Sentence Transformers.
    5. Upserts the vectorized chunks and metadata to Pinecone.

    Implements a resilient retry architecture: unrecoverable logical errors (e.g.,
    empty payloads) are routed immediately to the Dead Letter Queue (DLQ).
    Infrastructure errors trigger a delayed retry loop up to MAX_RETRIES.
    """
    headers = properties.headers or {}
    retry_count = headers.get("retry_count", 0)

    try:
        try:
            news_data = NewsItem.model_validate_json(body)
        except Exception as e:
            print(
                "CRITICAL: Pydantic validation failure. Routing to DLQ.",
                str(e),
                flush=True,
            )
            ch.basic_publish(
                exchange="", routing_key="news_dlq", body=body, properties=properties
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        text = news_data.text
        published_at = news_data.published_at
        news_url = news_data.url
        tickers = news_data.tickers

        if not text or not tickers:
            print(
                f"WARNING: Received empty text or missing tickers for {news_url}. Routing to DLQ.",
                flush=True,
            )
            ch.basic_publish(
                exchange="", routing_key="news_dlq", body=body, properties=properties
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            sent_result = sentiment_model(text[:512])[0]
            sentiment_label = sent_result["label"].upper()
            sentiment_score = float(sent_result["score"])
        except Exception:
            sentiment_label = "UNKNOWN"
            sentiment_score = 0.0

        chunks = text_splitter.split_text(text)
        batch_vectors = []

        for i, chunk in enumerate(chunks):
            embedding = model.encode(chunk).tolist()
            uid = hashlib.md5(f"{chunk}_{i}_{published_at}".encode("utf-8")).hexdigest()

            metadata = {
                "text": chunk,
                "source": "news_api",
                "url": news_url,
                "published_at": float(published_at),
                "sentiment": sentiment_label,
                "sentiment_score": sentiment_score,
                "ticker_principal": tickers[0],
                "tickers_relacionados": ",".join(tickers),
            }
            batch_vectors.append((uid, embedding, metadata))

        if batch_vectors:
            index.upsert(vectors=batch_vectors, namespace="fin_news_v1")
            print(
                f"Persisted {len(batch_vectors)} vector embeddings for {tickers} | Sentiment: {sentiment_label}",
                flush=True,
            )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"INFRASTRUCTURE EXCEPTION: {str(e)}", flush=True)
        # INFRASTRUCTURE RETRY: For network timeouts or DB lockups, we retry.
        if retry_count < MAX_RETRIES:
            headers["retry_count"] = retry_count + 1
            properties.headers = headers
            print(
                f"Re-queuing message. Attempt {retry_count + 1}/{MAX_RETRIES}",
                flush=True,
            )
            ch.basic_publish(
                exchange="",
                routing_key="news_retry_queue",
                body=body,
                properties=properties,
            )
        else:
            print("MAX RETRIES EXCEEDED. Routing payload to DLQ.", flush=True)
            ch.basic_publish(
                exchange="", routing_key="news_dlq", body=body, properties=properties
            )
        ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue="news_queue", on_message_callback=callback, auto_ack=False)
channel.start_consuming()
