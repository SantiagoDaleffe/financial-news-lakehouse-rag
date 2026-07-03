import hashlib
import time
import os
from pinecone import Pinecone

class SemanticCache:
    def __init__(self, pc_client, embedding_model, namespace="semantic_cache_v1"):
        self.model = embedding_model
        self.index = pc_client.Index(os.getenv("PINECONE_INDEX_NAME"))
        self.namespace = namespace

    def check(self, query: str, threshold: float = 0.15, ttl_seconds: int=100):
        """
        Calculates the vector distance. If it is less than the threshold, it is a Cache Hit.
        """
        embedding = self.model.encode(query).tolist()
        results = self.index.query(
            vector=embedding,
            top_k=1,
            include_metadata=True,
            namespace=self.namespace
        )

        if not results.get('matches'):
            return None
        
        match = results['matches'][0]
        
        distance = 1.0 - match['score']

        
        if distance < threshold:
            metadata = match['metadata']
            timestamp_saved = metadata.get('timestamp', 0)
            actual_time = time.time()
            
            if (actual_time - timestamp_saved) > ttl_seconds:
                print(f'CACHE EXPIRED. More than {ttl_seconds}s have passed. Discarding...')
                return None
            
            print(f'CACHE HIT. Distance: {distance:.4f}. Saving model call.', flush=True)
            return metadata['response']
        
        print(f"CACHE MISS. Distance to nearest: {distance:.4f}. Processing prompt...", flush=True)
        return None

    def save(self, query: str, response: str):
        """
        Saves the prompt and the processed response for future identical queries.
        """
        embedding = self.model.encode(query).tolist()
        doc_id = hashlib.md5(query.encode('utf-8')).hexdigest()
        
        metadata = {
            "response": response, 
            "timestamp": time.time(),
            "query_text": query
        }
        
        self.index.upsert(
            vectors=[(doc_id, embedding, metadata)], 
            namespace=self.namespace
        )
        print("New answer saved in semantic cache.", flush=True)