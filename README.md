# Financial News Intelligence Platform

**End-to-End Data Engineering and MLOps Platform for Financial News and Crypto Analysis.**

This project implements a local **Lakehouse architecture** using Docker containers, simulating a production Big Data environment. It ingests news from multiple sources, processes them with Spark (Delta Lake), generates vector embeddings, and makes them available for semantic analysis.

## Architecture

The system follows a **Medallion Architecture** optimized for AI:

- **Bronze Layer:** Raw data ingestion from APIs/RSS into MinIO (JSON).
- **Silver Layer:** Data cleaning and conversion to Delta Lake format using Apache Spark.
- **Gold Layer:** Generation of embeddings and storage in ChromaDB for semantic search.

## Tech Stack

- **Ingestion:** Python (Requests, Feedparser)
- **Storage (Data Lake):** MinIO (S3 Compatible)
- **Processing:** Apache Spark 3.3 (PySpark) + Delta Lake 2.3
- **Vector Database:** ChromaDB (Gold Layer/Serving)
- **Orchestration:** Modular Docker scripts
- **MLOps:** MLflow (Tracking) + Docker Compose

## How to Run

### Prerequisites

- Docker & Docker Compose installed.

### Steps

- **Clone the repository.**
- Configure Environment:  
    Create a .env file with the necessary credentials (check .env.example or use defaults).
- Launch Infrastructure:  
    Build and start the containers.  
    docker-compose up -d --build  

- Run the Data Pipeline (ETL + AI):  
    Execute the master orchestrator script inside the runner container.  
    docker exec -it etl_runner python3 scripts/orchestrator.py  

- Verify Data Integrity:  
    Check that data has been correctly processed and stored.  
    docker exec -it etl_runner python3 scripts/verify_data.py  

## Project Structure

- scripts/: Source code for pipelines.
  - collect_\*.py: Data ingestion (Bronze Layer).
  - process_bronze.py: Cleaning and conversion to Delta Lake (Silver Layer).
  - process_gold.py: Embedding generation and loading into ChromaDB (Gold Layer).
  - orchestrator.py: Master script to run the entire flow.
  - spark_utils.py: Centralized Spark and S3 configuration.
- data/: Docker persistent volumes (excluded from git).

## Technical Decisions

- **MinIO vs. MongoDB:** An **Object Storage (S3)** based Data Lake was chosen to align with modern industry standards.
- **Delta Lake:** The Delta format is used in the Silver layer to guarantee **ACID transactions** and avoid consistency errors in S3 (e.g., _FileAlreadyExists Exception_), enabling concurrent reads and writes.
- **Dockerization:** An isolated Spark environment with manually injected Hadoop-AWS JARs to ensure full compatibility with MinIO.
- **ChromaDB:** Used as the Gold layer to enable **semantic search** and RAG (Retrieval Augmented Generation) in future phases.