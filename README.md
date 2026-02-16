# 💰 FinTech Lakehouse: Real-time Financial RAG & Sentiment Analysis Platform

![Python](https://img.shields.io/badge/Python-3.10-blue) ![Spark](https://img.shields.io/badge/Apache%20Spark-3.3-orange) ![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.3-blue) ![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED) ![Llama 3](https://img.shields.io/badge/GenAI-Llama%203-purple)

An end-to-end **Data Engineering and Generative AI platform** designed to ingest, process, and synthesize financial news streams in real-time. 

This project implements a local **Data Lakehouse** using a **Medallion Architecture** (Bronze/Silver/Gold) to power a **RAG (Retrieval-Augmented Generation)** system. It allows financial analysts to "chat" with the market, obtaining grounded explanations for asset volatility using **Llama 3** and **Vector Search**.

## 🏗 Architecture & Engineering Decisions

The system is built on a decoupled, microservices-oriented architecture fully containerized with Docker.

### The Medallion Architecture
* **🥉 Bronze Layer (Raw Ingestion):** Async Python orchestrators ingest unstructured data (JSON) from multiple RSS feeds/APIs into **MinIO** (S3-compatible Object Storage).
* **🥈 Silver Layer (Processing & ACID Transactions):** * **Challenge:** Standard Parquet writes on S3 suffer from eventual consistency issues and lack atomic transactions, leading to `FileAlreadyExists` exceptions during concurrent writes.
    * **Solution:** Implemented **Delta Lake** on top of Apache Spark. This guarantees **ACID transactions**, schema enforcement, and "Time Travel" (data versioning) capabilities essential for audit trails in Fintech.
* **🥇 Gold Layer (Semantic Intelligence):** Processed news is embedded using `all-MiniLM-L6-v2` and indexed in **ChromaDB** for vector retrieval.

### Hybrid AI Pipeline (Discriminative + Generative)
To optimize latency and cost, a two-stage inference pipeline was designed:
1.  **Pre-Filtering (Discriminative):** **FinBERT** classifies news sentiment metadata to filter out noise.
2.  **RAG Agent (Generative):** **Llama 3** (via LangChain) receives the filtered context to generate summarized insights, reducing hallucinations by grounding answers in retrieved documents.

## 🛠 Tech Stack

* **Ingestion:** Python AsyncIO, Feedparser, MinIO (S3).
* **Processing (ETL):** Apache Spark 3.3 (PySpark), Delta Lake 2.3.
* **Vector Database:** ChromaDB.
* **LLM Orchestration:** LangChain, Ollama (Llama 3), FinBERT (Hugging Face).
* **Infrastructure:** Docker, Docker Compose.

## 🚀 How to Run (Production Simulation)

**Prerequisites:** Docker & Docker Compose.

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/YourUsername/financial-news-lakehouse-rag.git](https://github.com/YourUsername/financial-news-lakehouse-rag.git)
    cd financial-news-lakehouse-rag
    ```

2.  **Start Infrastructure (Spark, MinIO, ChromaDB):**
    ```bash
    docker-compose up -d --build
    ```

3.  **Run the ETL Orchestrator:**
    Executes the full pipeline: Ingestion -> Delta Conversion -> Embedding Generation.
    ```bash
    docker exec -it etl_runner python scripts/orchestrator.py
    ```

4.  **Query the RAG Agent:**
    ```bash
    curl -X POST http://localhost:8000/analyze -H "Content-Type: application/json" -d '{"query": "Why is Bitcoin dropping today?"}'
    ```

## 📂 Project Structure

```bash
├── scripts/
│   ├── orchestrator.py    # Master DAG controller
│   ├── process_silver.py  # Spark + Delta Lake transformation logic
│   ├── process_gold.py    # Vector embedding generation
│   └── spark_utils.py     # SparkSession config with Hadoop-AWS JAR injection
├── docker/                # Custom Dockerfiles for Spark/Hadoop compatibility
├── notebooks/             # Prototyping and EDA
└── docker-compose.yml     # Infrastructure definition
```

Developed by Santiago Daleffe - Open for collaboration on Fintech & GenAI Engineering.
