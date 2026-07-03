from fastapi import APIRouter
import os
from pinecone import Pinecone

router = APIRouter(tags=["system"])

pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
index = pc.Index(os.getenv("PINECONE_INDEX_NAME"))

@router.get("/health")
def health():
    """Returns the API status."""
    return {"status": "ok"}

@router.get('/database-info')
def database_info():
    """Returns exact vector counts per namespace directly from Pinecone."""
    stats = index.describe_index_stats()
    return {"stats": stats.to_dict()}