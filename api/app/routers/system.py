from fastapi import APIRouter
from .agent import collection

router = APIRouter(tags=["system"])

@router.get("/health")
def health():
    """Returns the API status."""
    return {"status": "ok"}

@router.get('/database-info')
def database_info():
    """Returns basic stats from the vector database including all metadata."""
    count = collection.count()
    data = collection.get(limit=5)
    
    sample = []
    if data and data.get("documents"):
        for doc, meta, doc_id in zip(data["documents"], data["metadatas"], data["ids"]):
            sample.append({
                "id": doc_id,
                "document": doc[:150] + "...",
                "metadata": meta
            })
            
    return {"total_items": count, "sample_data": sample}