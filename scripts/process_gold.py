import os
import logging
import chromadb
from chromadb.utils import embedding_functions
import sys
from deltalake import DeltaTable
from transformers import pipeline
import pandas as pd

# utils for bring up Spark (deltatable instead of fallback to Spark for reading delta)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scripts.spark_utils import get_spark_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - GOLD LAYER - %(message)s')

MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password123")
CHROMA_HOST = os.getenv("CHROMA_HOST", "chromadb")
CHROMA_PORT = os.getenv("CHROMA_PORT", "8000")

def get_chroma_client():
    return chromadb.HttpClient(host=CHROMA_HOST, port=int(CHROMA_PORT))

def run_silver_to_gold():
    logging.info("Starting Gold Layer Process")
    
    try:
        logging.info("loading FinBERT")
        sentiment_pipeline = pipeline("text-classification", model="ProsusAI/finbert")
    except Exception as e:
        logging.error(f"error: {e}")
        return
    
    # reading deltalake from raw python (faster for inference than spark)
    silver_path = "s3://silver/articles_delta/"
    
    storage_options = {
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
        "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        # fix http endpoint
        "AWS_ALLOW_HTTP": "true" 
    }

    try:
        logging.info(f"Delta Table: {silver_path}")
        # reading with deltalake instead of spark
        dt = DeltaTable(silver_path, storage_options=storage_options)
        df = dt.to_pandas()
        
        if df.empty:
            logging.warning("there is no data in Silver Layer. Exiting Gold Layer process.")
            return

        logging.info(f"Data: {len(df)} articles.")

    except Exception as e:
        logging.error(f"error reading silver: {e}")
        return

    # 2. emmbedding + index ChromaDb
    try:
        # title + description as text_blob
        df['text_blob'] = df['title'].fillna('') + ". " + df['description'].fillna('')
        df = df[df['text_blob'].str.len() > 10].copy()

        logging.info("sentiment analysis")
        titles = df['title'].fillna("").astype(str).tolist()
        results = sentiment_pipeline(titles, truncation=True, max_length=512)

        df['sentiment'] = [r['label'] for r in results]
        df['sentiment_score'] = [r['score'] for r in results]
        
        logging.info(f"sentiment calculated. EXAMPLE: {df[['title', 'sentiment']].head(1).values}")
        
        client = get_chroma_client()
        # baseline all-MiniLM-L6-v2
        ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
        
        # get_or_create avoiding failure if collection exists
        collection = client.get_or_create_collection(
            name="financial_news", 
            embedding_function=ef, 
            metadata={"hnsw:space": "cosine"}
        )
        
        logging.info("creating embeddings and indexing in ChromaDB")
        
        batch_size = 100
        total_docs = len(df)
        
        for i in range(0, total_docs, batch_size):
            batch = df.iloc[i : i + batch_size]
            ids = batch['article_id'].tolist()
            documents = batch['text_blob'].tolist()

            metadatas = []
            for _, row in batch.iterrows():
                metadatas.append({
                    "source": row['source'],
                    "url": row['url'],
                    "title": row['title'],
                    "published_at": str(row['published_at']),
                    "sentiment": row['sentiment'], # positive, negative, neutral
                    "sentiment_score": float(row['sentiment_score'])
                })
                
            collection.upsert(ids=ids, documents=documents, metadatas=metadatas)
            logging.info(f"indexed: {i + len(batch)} / {total_docs}")

        logging.info(f"Gold layer completd {total_docs} embeddings in ChromaDB.")

    except Exception as e:
        logging.error(f"error with vectorization: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_silver_to_gold()