from fastapi import APIRouter
from .agent import collection

router = APIRouter(tags=["system"])

@router.get("/health")
def health():
    """Returns the API status."""
    return {"status": "ok"}

@router.get('/database-info')
def database_info():
    """Returns basic stats from the vector database."""
    count = collection.count()
    data = collection.get(limit=5)
    return {"total_items": count, "sample_data": data["documents"][:5]}