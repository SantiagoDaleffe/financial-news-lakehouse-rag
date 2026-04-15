# Financial AI Agent - Enterprise RAG Pipeline

An end-to-end Asynchronous Retrieval-Augmented Generation (RAG) system designed for the financial sector. This platform autonomously ingests daily financial news, processes sentiment analysis using traditional NLP (FinBERT), and provides a conversational interface powered by Google's Gemini to query market intelligence, manage simulated portfolios, and set real-time price alerts.

## Key Features

*   **Multi-Model Semantic Routing:** Automatically detects query complexity (Simple vs. Complex) using prototype embeddings and routes requests to the most efficient Gemini model (Flash vs. Pro).
*   **Model Cascading & Fallback:** Resilient inference system that attempts a cascade of models (e.g., Gemini 1.5 Pro -> Flash) to ensure high availability.
*   **Intelligent Semantic Cache:** Uses ChromaDB to cache non-transactional LLM responses, significantly reducing latency and API costs. Transactional queries (trading, alerts) bypass the cache for data integrity.
*   **Paper Trading Engine:** Integrated tools for executing simulated trades and monitoring portfolio status in real-time.
*   **Advanced RAG with Sentiment:** Ingested news is chunked, vectorized (MiniLM), and enriched with FinBERT sentiment scores for deeper financial context.
*   **Real-time Alerts:** Telegram integration for push notifications when price targets are hit.
*   **Automated Evaluation Framework:** Built-in "Golden Dataset" testing to measure Tool Calling Accuracy and RAG performance, tracked via MLflow.
*   **Data Lifecycle Management:** Automated ETL pipelines in Airflow and a dedicated `/prune` endpoint to maintain vector database health.

## Architecture

This project implements a decoupled microservices architecture prioritizing scalability, observability, and idempotency:

1.  **Orchestration:** **Apache Airflow** schedules daily ETL jobs for news, daily prices, and historical backfills.
2.  **Ingestion Gateway:** A **FastAPI** service that validates payloads, persists raw data in **MinIO (S3)**, and enqueues tasks in **RabbitMQ**.
3.  **Processing Worker:** Asynchronous workers perform text chunking, embedding generation (SentenceTransformers), and sentiment analysis (FinBERT).
4.  **Vector Database:** **ChromaDB** stores embeddings and metadata (sentiment, timestamps).
5.  **Agent Logic:** Advanced reasoning engine using **Gemini 1.5/2.0/3.0** with function calling for live market data (Yahoo Finance), math, and alert management.
6.  **Observability:** Full tracking of prompts, latency, and evaluation metrics via **MLflow**. System state monitoring via **pgAdmin**.

## Tech Stack

*   **Core:** Python 3.11, FastAPI, Pydantic
*   **LLM/AI:** Google Gemini API, Hugging Face (FinBERT, SentenceTransformers)
*   **Data:** Airflow, RabbitMQ, MinIO (S3), PostgreSQL
*   **Vector DB:** ChromaDB
*   **MLOps:** MLflow, Docker Compose, GitHub Actions (CI)

## API Endpoints

The system exposes a modular API under `/api/v1/`:

*   **`/agent`**: Conversational interface, semantic routing, and tool execution.
*   **`/alerts`**: CRUD operations for price alerts and Telegram notifications.
*   **`/ingestion`**: Data entry point and vector database pruning (`/prune`).
*   **`/chats`**: Session management and history persistence.
*   **`/research`**: Deep analysis endpoints for specialized financial reports.
*   **`/system`**: Health checks and service status.
*   **`/auth`**: JWT-based security layer.

## Evaluation & Metrics

To ensure reliability, the project includes an evaluation module in `/eval`:
1.  **Golden Dataset:** A curated set of financial queries and expected tool calls.
2.  **Benchmark Script:** `evaluate_baseline.py` runs the dataset against models and logs "Tool Calling Accuracy" to MLflow.
3.  **Monitoring:** All production LLM calls are logged in MLflow with latency, model version, and routing complexity tags.

## How to Run Locally

### Prerequisites
*   Docker & Docker Compose
*   NewsAPI Key
*   Google Gemini API Key
*   Telegram Bot Token (Optional for alerts)

### Setup
1.  Clone the repository.
2.  Create a `.env` file based on `.env.example`.
3.  Prepare local volumes:
    ```bash
    sudo chmod -R 777 dags logs plugins minio_data chroma_data mlflow_data
    ```
4.  Launch the stack:
    ```bash
    docker-compose up -d --build
    ```

### Access Points
* **API Docs:** [http://localhost:8000/docs](http://localhost:8000/docs)
* **Airflow UI:** [http://localhost:8080](http://localhost:8080) *(Use the credentials defined in your .env)*
* **MLflow Tracking:** [http://localhost:5000](http://localhost:5000)
* **MinIO Console:** [http://localhost:9001](http://localhost:9001) *(Use the credentials defined in your .env)*
* **pgAdmin:** [http://localhost:5050](http://localhost:5050) *(Use the credentials defined in your .env)*

## 🔮 Future Roadmap
*   **Change Data Capture (CDC):** Implement state tracking to avoid redundant vectorization.
*   **Multi-Agent Swarm:** Specialized agents for Risk, Fundamental, and Technical analysis.
*   **Dynamic Re-ranking:** Implement Cross-Encoders for improved retrieval precision.
