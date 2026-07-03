from fastapi import APIRouter
import os
from pinecone import Pinecone
from sentence_transformers import SentenceTransformer

router = APIRouter(tags=["research"])

pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
index = pc.Index(os.getenv("PINECONE_INDEX_NAME"))

print("loading sentence transformer for research feed...", flush=True)
model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

@router.get("/latest")
def get_latest_news(limit: int = 10):
    """
    Fetches relevant market news from Pinecone and sorts them by date 
    to populate the frontend feed.
    """
    embedding = model.encode("financial market news economy").tolist()
    
    results = index.query(
        vector=embedding,
        top_k=limit,
        include_metadata=True,
        namespace="fin_news_v1"
    )

    feed = []
    for match in results.get("matches", []):
        meta = match.get("metadata", {})
        feed.append({
            "text": meta.get("text", ""),
            "sentiment": meta.get("sentiment", "unknown"),
            "score": meta.get("sentiment_score", 0.0),
            "ticker_principal": meta.get("ticker_principal", "none"),
            "tickers_relacionados": meta.get("tickers_relacionados", "none"),
            "published_at": meta.get("published_at", 0.0),
            "source": meta.get("source", "unknown"),
            "url": meta.get("url", "")
        })
        
    feed = sorted(feed, key=lambda x: x["published_at"], reverse=True)

    return {"feed": feed}