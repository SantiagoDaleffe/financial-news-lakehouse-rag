from fastapi import APIRouter
from .agent import collection

router = APIRouter(tags=["research"])


@router.get("/latest")
def get_latest_news(limit: int = 10):
    """
    fetches the most recent news and their sentiment from the vector db
    for the frontend feed.
    """
    results = collection.get(limit=limit)

    feed = []
    if results and results.get("documents"):
        for doc, meta in zip(results["documents"], results["metadatas"]):
            safe_meta = meta or {}
            feed.append(
                {
                    "text": doc,
                    "sentiment": safe_meta.get("sentiment", "unknown"),
                    "score": safe_meta.get("sentiment_score", 0.0),
                    "source": safe_meta.get("source", "unknown"),
                }
            )

    return {"feed": feed}
