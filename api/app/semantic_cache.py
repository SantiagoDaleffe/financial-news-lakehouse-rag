import hashlib
import time

class SemanticCache:
    def __init__(self, chroma_client, embedding_model, collection_name="semantic_cache_v1"):
        """
        Initializes the cache by connecting to the dedicated collection in ChromaDB.
        """
        self.model = embedding_model
        self.collection = chroma_client.get_or_create_collection(name=collection_name)

    def check(self, query: str, threshold: float = 0.15, ttl_seconds: int=100):
        """
        Calculates the vector distance. If it is less than the threshold, it is a Cache Hit.
        """
        embedding = self.model.encode(query).tolist()
        results = self.collection.query(
            query_embeddings=[embedding],
            n_results=1
        )

        if not results['documents'] or not results['documents'][0]:
            return None

        distance = results['distances'][0][0]
        
        if distance < threshold:
            metadata = results['metadatas'][0][0]
            timestamp_saved = metadata.get('timestamp',0)
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
        
        self.collection.upsert(
            ids=[doc_id],
            embeddings=[embedding],
            documents=[query],
            metadatas=[{"response": response, "timestamp": time.time()}]
        )
        print("New answer saved in semantic cache.", flush=True)