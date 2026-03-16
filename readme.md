# Financial AI Agent - Enterprise RAG Pipeline

An end-to-end Asynchronous Retrieval-Augmented Generation (RAG) system designed for the financial sector. This platform autonomously ingests daily financial news, processes sentiment analysis using traditional NLP (FinBERT), and provides a conversational interface powered by Google's Gemini to query market intelligence without hallucinations.

## Architecture

This project implements a decoupled microservices architecture prioritizing scalability, observability, and idempotency:

1. **Orchestration:** **Apache Airflow** (w/ PostgreSQL backend) schedules daily ETL jobs to fetch the latest market news.
2. **Ingestion API:** A **FastAPI** service receives the raw data, acts as a gateway, and ensures raw payloads are persisted in a Data Lake (**MinIO / S3**).
3. **Message Broker:** The API publishes ingestion tasks to **RabbitMQ** to decouple the fast API responses from heavy ML processing.
4. **Processing Worker:** Asynchronous workers consume the queue, perform text chunking, calculate embeddings (MiniLM), and score financial sentiment (**FinBERT**).
5. **Vector Database:** Embeddings and metadata are stored in **ChromaDB**, utilizing cryptographic hashing to ensure idempotency and prevent duplicate chunks.
6. **LLM & Observability:** The search endpoint queries ChromaDB, builds the prompt context, and streams the response via **Gemini 1.5 Flash**. Every prompt and latency metric is tracked via **MLflow**.
7. **Agent Tools (Function Calling):** The LLM leverages **Gemini 1.5 Flash** as the advanced reasoning engine, which executes functions in real-time. Beyond text generation, it performs dynamic queries to external financial APIs (**Yahoo Finance**), executes mathematical computations, and retrieves live market data to provide contextually accurate intelligence.
8. **Alert Worker & Telegram Integration:** An asynchronous time-oriented worker crosses alert conditions from **PostgreSQL** with real-time prices, triggering push notifications via **Telegram** to subscribers whenever trading signals are matched.
9. **Frontend (Multi-repo Architecture):** The backend exposes a **REST API** with **CORS** enabled at `http://localhost:8000`, ready for consumption by external clients. The frontend is decoupled as a separate repository (e.g., **Next.js**), enabling independent scaling and deployment of the client-side application.

## Tech Stack

* **Languages:** Python 3.11
* **Machine Learning:** Hugging Face (FinBERT, SentenceTransformers), Gemini API
* **Data Engineering:** Airflow, RabbitMQ, MinIO (S3)
* **Databases:** ChromaDB, PostgreSQL
* **MLOps:** MLflow, Docker Compose, GitHub Actions (CI)

## How to Run Locally

### Prerequisites
* Docker & Docker Compose
* NewsAPI Key
* Google Gemini API Key

### Setup
1. Clone the repository.
2. Create a `.env` file in the root directory based on the `.env.example`.
3. Give permissions to the local volumes:
   ```bash
   sudo chmod -R 777 dags logs plugins minio_data chroma_data mlflow_data
   ```
4. Build and spin up the cluster:
    ```bash
    docker-compose up -d --build
    ```
5. Trigger the initial data load by navigating to Airflow (http://localhost:8080), logging in (admin/admin), and triggering the news_etl_pipeline DAG.
6. The REST API will be available at http://localhost:8000/docs, where you can view the complete OpenAPI specification and interact with all available endpoints.

## Future Optimizations

* **Change Data Capture (CDC)**: Implement a state-tracking database table prior to the vectorization queue to avoid re-inferencing previously processed news, optimizing compute costs.
