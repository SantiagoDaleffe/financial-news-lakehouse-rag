import chromadb
import json
from datetime import datetime

# Conectamos directo al puerto expuesto en el docker-compose
client = chromadb.HttpClient(host="localhost", port=8001)

try:
    collection = client.get_collection(name="fin_news_v1")
    total_docs = collection.count()
    print(f"=== ESTADO DE CHROMADB ===")
    print(f"Total de vectores (chunks) almacenados: {total_docs}\n")

    if total_docs == 0:
        print("La base de datos está vacía.")
        exit()

    # --- QUERY 1: Buscar por Ticker Específico (Como un WHERE ticker = 'SPY') ---
    ticker_a_buscar = "Bitcoin" # Cambiá esto por SPY, QQQ, etc para testear
    
    print(f"--- Buscando noticias etiquetadas con: {ticker_a_buscar} ---")
    resultados = collection.get(
        where={"ticker_principal": {"$eq": ticker_a_buscar}},
        limit=5 # Traemos solo 5 para no saturar la consola
    )

    if not resultados['documents']:
        print(f"No hay vectores para {ticker_a_buscar}.")
    else:
        for i in range(len(resultados['documents'])):
            doc = resultados['documents'][i]    
            meta = resultados['metadatas'][i]
            
            # Formateamos la fecha para que sea legible
            fecha_legible = "Desconocida"
            if 'published_at' in meta:
                fecha_legible = datetime.fromtimestamp(meta['published_at']).strftime('%Y-%m-%d %H:%M:%S')

            print(f"\n[ID: {resultados['ids'][i]}]")
            print(f"Fecha:  {fecha_legible}")
            print(f"Ticker: {meta.get('ticker_principal')} (Relacionados: {meta.get('tickers_relacionados')})")
            print(f"Sentimiento: {meta.get('sentiment')} (Score: {meta.get('sentiment_score'):.2f})")
            print(f"Texto:  {doc[:150]}...") # Truncamos a 150 caracteres para leer fácil

except Exception as e:
    print(f"Error conectando a ChromaDB: {e}")