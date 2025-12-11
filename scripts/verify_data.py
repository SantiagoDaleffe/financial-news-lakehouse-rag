import os
import chromadb
import pandas as pd
from deltalake import DeltaTable

CHROMA_HOST = os.getenv("CHROMA_HOST", "chromadb")
CHROMA_PORT = os.getenv("CHROMA_PORT", "8000")
MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password123")

def verify_architecture():
    print("STARTING DATA VERIFICATION\n")

    # 1. CHECKING SILVER (Delta Lake on MinIO)
    print("CHEKING SILVER LAYER (Delta Lake on MinIO)")
    try:
        # s3:// so deltalake-python understands its S3-compatible
        silver_path = "s3://silver/articles_delta/"
        
        storage_options = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
            "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true" # HTTP without SSL
        }
        
        # deltatable
        dt = DeltaTable(silver_path, storage_options=storage_options)
        df = dt.to_pandas()
        
        print(f"Delta Table on MinIO")
        print(f"rows in silver: {len(df)}")
        if not df.empty:
            print(f"last article: {df['published_at'].max()}")
            print(f"source: {df['source'].unique()}")
        else:
            print("table exists but is empty")
            
    except Exception as e:
        print(f"error reading silver (Delta): {e}")

    print("\n" + "="*30 + "\n")

    # 2. checking gold (ChromaDB)
    print("CHECKING GOLD LAYER (ChromaDB)")
    try:
        client = chromadb.HttpClient(host=CHROMA_HOST, port=int(CHROMA_PORT))
        
        # list collections
        collections = client.list_collections()
        col_names = [c.name for c in collections]
        print(f"collections: {col_names}")

        if "financial_news" in col_names:
            coll = client.get_collection("financial_news")
            count = coll.count()
            print(f"Collection 'financial_news' exists.")
            print(f"embeddings: {count}")
            
            # example peek
            if count > 0:
                peek = coll.peek(limit=1)
                print("\ndata embedding example:")
                if peek['ids']:
                    print(f"   ID: {peek['ids'][0]}")
                if peek['metadatas']:
                    print(f"   Metadata: {peek['metadatas'][0]}")
                if peek['documents']:
                    print(f"   Text (extract): {peek['documents'][0][:100]}...")
        else:
            print("didnt found collection 'financial_news'.")
            print("Posible cause: process gold failed silently or didnt run.")

    except Exception as e:
        print(f"error conecting to ChromaDB: {e}")

if __name__ == "__main__":
    verify_architecture()