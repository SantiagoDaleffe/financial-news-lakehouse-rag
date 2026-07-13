# Enterprise Financial Agent & Quant Trading System

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688?logo=fastapi)
![GenAI](https://img.shields.io/badge/Google_Gemini-Agentic_RAG-orange)
![MLOps](https://img.shields.io/badge/MLflow-Tracking-blue?logo=mlflow)
![DataEng](https://img.shields.io/badge/Airflow_%7C_RabbitMQ-Event_Driven-red)

An end-to-end B2B financial platform that bridges rigorous Quantitative Data Science with modern Generative AI. This system features a highly optimized predictive trading engine (LightGBM) audited by a Neurosymbolic LLM, combined with a multi-tenant conversational agent capable of semantic routing, real-time portfolio management, and news-based RAG.

## High-Level Cloud & Tech Stack
* **Core API & Agents:** FastAPI, Python 3.11, LangChain (Custom Routers), Pydantic
* **Generative AI:** Google Gemini, SentenceTransformers, FinBERT
* **Quantitative Engine:** LightGBM, SHAP, Optuna, Pandas/NumPy
* **Data Engineering (Event-Driven):** RabbitMQ, MinIO (S3), GitHub Actions (Cron Orchestration)
* **Databases:** PostgreSQL (Supabase) for Relational State, Pinecone Serverless for Vector/Semantic Cache
* **MLOps & CI/CD:** MLflow (DagsHub), Docker, GitHub Container Registry, Pytest

---

## 1. The Quantitative Engine (From Lab to MLOps)

The predictive trading logic is decoupled into two environments: a rigorous laboratory for experimentation (`quant_engine/`) and an automated deployment pipeline for production.

### Data Science & Experimentation (The Lab)
The quantitative research environment was built from scratch to prevent common time-series modeling pitfalls, such as look-ahead bias and data leakage. The entire pipeline is modular:

* **Data Ingestion (`download_history.py` & `external_features.py`):** 
  Automated retrieval of historical OHLCV data for a specific ETF universe (SPY, QQQ, DIA, GLD, TLT, IWM). Macroeconomic context is injected by computing the momentum of market fear (Log Returns of the CBOE VIX index).

* **Feature Engineering (`feature_engineering.py`):** 
  Transforms raw pricing data into stationary, normalized technical features suitable for gradient boosting. Features include relative strength against the S&P 500 ($\beta$), volatility shocks, normalized moving average distances, and momentum oscillators (RSI, MACD). A strict masking system drops forbidden columns (absolute prices, forward returns) before training to prevent data leakage.

* **Dynamic Asymmetric Labeling (`target_engineer.py`):**
  Instead of using fixed percentage thresholds for labels (which fail during volatility regime shifts), targets are generated dynamically based on forward return quantiles (e.g., $Q_{high}$ for BUY, $Q_{low}$ for SELL).

* **Time-Series Validation (`backtest_engine.py`):** 
  Implements strict **Walk-Forward Optimization (WFO)**. The model trains on a rolling window (e.g., 500 days) and predicts on an out-of-sample block (e.g., 20 days), separated by a strict embargo period (5 days) to eliminate autocorrelation leakage between the train and test sets.

* **Dynamic Feature Selection (`feature_selector.py`):**
  A lightweight LightGBM tree evaluates the Information Gain of each engineered feature, selecting the optimal subset that best partitions the data for the current temporal regime during the Walk-Forward process.

* **Hyperparameter Tuning (`optuna_tuner.py`):** 
  Bayesian optimization via Optuna (TPE Sampler). The objective function is highly customized for finance: it maximizes the Annualized Sharpe Ratio and Total Return, while heavily penalizing extreme drawdowns or low market exposure.

* **Production Exporter (`model_exporter.py`):**
  Once the optimal model is found, this module serializes the LightGBM tree (`.joblib`) alongside a strict `quant_config.json` manifest detailing probability thresholds, required features, and rolling windows for the production inference pipeline.

**Baseline Out-of-Sample Performance (Eq-Weight Portfolio):**
* Strategy Return: 29.72%
* Maximum Drawdown: -12.13%
* Annualized Sharpe Ratio: 0.81

## 2. Production Inference & Continuous Training (MLOps)

The static laboratory models are operationalized into dynamic, self-maintaining systems orchestrated by **GitHub Actions**. The production pipeline runs daily, executing a neurosymbolic flow that merges gradient boosting predictions with LLM risk management.

### The Daily Inference Pipeline (`run_inference.py`)
Triggered automatically every trading day at `01:30 UTC`, the inference script executes a strict 5-step lifecycle:

1. **Continuous Reconciliation (Audit Loop):** Before making new predictions, the system fetches yesterday's `PENDING` signals, queries the actual market closing price, and calculates the realized log return. It labels the true outcome and logs daily actionable win rates and realized returns to **MLflow/DagsHub**.
2. **Feature Hydration:** Queries the last 300 days of market data from **PostgreSQL** and processes the cross-section through the `TechnicalFeatureEngineer`.
3. **Quantitative Signal Generation:** The LightGBM model calculates probability distributions. Crucially, it uses **TreeExplainer (SHAP)** to extract the top three mathematical drivers behind every "HOT" signal, making the black-box model interpretable.
4. **Context Retrieval (RAG):** For assets with strong quantitative signals, the pipeline queries the **Pinecone Vector Database** (via SentenceTransformers) to retrieve macroeconomic news from the last 4 days, pulling pre-computed **FinBERT** sentiment scores.
5. **Neurosymbolic LLM Audit:** The quantitative probabilities, SHAP drivers, and RAG context are fed into **Gemini 2.5 Flash** (acting as a Risk Manager). Enforcing strict guardrails (e.g., temporal decay of news relevance, fallback trust in the quant model), the LLM issues a final `BUY`, `SELL`, or `HOLD` verdict. The full reasoning trace is saved as a Markdown artifact in MLflow.

### Continuous Training Pipelines (Concept Drift Management)
Financial markets are non-stationary. To combat model degradation, the system implements an automated challenger-champion paradigm:

* **Light Retrain (`run_light_retrain.py`):** Runs weekly (`02:00 UTC` on Saturdays). It first checks for **Concept Drift** by calculating the real-world win rate of the last 20 actionable trades. If the win rate falls below 45%, it triggers a retrain using the last 540 days of data. The new "Challenger" model must empirically beat the "Champion" model's actionable accuracy to be promoted.
* **Heavy Retrain (`run_heavy_retrain.py`):** Triggered manually (`workflow_dispatch`) during major macro regime shifts. It processes 4+ years of data, triggers dynamic feature selection (`QuantFeatureSelector`), and spins up an **Optuna** study (Bayesian optimization) to completely reconstruct the model's hyperparameters and probability thresholds.

## 3. The Financial GenAI Agent & Core API

The user-facing layer is an asynchronous **FastAPI** microservice. It exposes a conversational interface powered by a highly constrained Gemini agent, equipped with a suite of Python tools to execute simulated trades, manage alerts, and perform real-time RAG.

### Semantic Routing & Cost Optimization
LLM API calls are expensive. To optimize costs without sacrificing intelligence, the API implements a custom **Semantic Router**:
* **Complexity Classification:** User queries are embedded using SentenceTransformers and compared against pre-computed `COMPLEX` and `SIMPLE` prototype embeddings via Cosine Similarity.
* **Model Cascade:** 
  * If the query is *Simple* (e.g., "delete my alert", "what is my balance"), it routes to the faster, cheaper tier (`gemini-3-flash` -> `flash-lite`).
  * If the query is *Complex* (e.g., "compare the credit risk of AAPL vs MSFT"), it routes to the heavy reasoning tier (`gemini-3.1-pro` -> `gemini-2.5-pro`).
* **Resilience:** If the primary model faces a rate limit (`429`) or server error, the cascade automatically falls back to the next available model in the tier, guaranteeing zero-downtime inference.

### Semantic Caching
To further slash API costs and latency, the system implements a **Semantic Cache** powered by Pinecone.
* When a non-transactional query arrives (e.g., "Why did the S&P 500 crash?"), it is embedded and queried against the cache namespace.
* If a similar query (Cosine Distance < `0.15`) was asked within the Time-To-Live (TTL) window (5 minutes), the API returns the cached response instantly, bypassing the LLM entirely.
* **Security Bypass:** Transactional keywords (`buy`, `sell`, `portfolio`) automatically bypass the cache to ensure real-time data integrity.

### Advanced RAG with Cross-Encoder Reranking
When retrieving macroeconomic news for a query, standard cosine similarity often retrieves contextually irrelevant documents. 
* The API fetches the top 20 candidate news articles from Pinecone.
* It then uses a **Cross-Encoder Reranker** (`ms-marco-MiniLM-L-6-v2`) to re-score the actual textual relationship between the user's query and the documents.
* Only the top 4 highly correlated, re-ranked documents are injected into the LLM prompt.

### Deterministic Tool Calling & Zero-Trust Security
The agent operates under a strict, hard-coded prompt constitution (`agent_skills.md`) that enforces a rigid internal reasoning process, paired with zero-trust backend tools:
* **Pre-Trade Validation:** The agent is forced to call `get_portfolio_status` before confirming any `execute_paper_trade` command to prevent phantom transactions.
* **AST Mathematical Sandbox:** To prevent Remote Code Execution (RCE) and Exponential DoS attacks via malicious math prompts, the `calculate_math` tool strictly bypasses Python's `eval()`. Instead, it parses the string into an **Abstract Syntax Tree (AST)**, mapping exclusively to safe `operator` functions (add, sub, mul, truediv) and actively rejecting exponentiation and alphabet characters.
* **Real-Time Data:** It leverages `get_live_stock_price` (via `yfinance`) to ensure zero hallucinations in pricing.
* **PII Redaction:** Before the prompt ever hits Google's servers, a Regex-based masking engine (`pii_masking.py`) intercepts and scrubs Credit Cards, Emails, Phone Numbers, and National IDs (`[EMAIL_REDACTED]`), ensuring absolute B2B data compliance.

## 4. Event-Driven Data Engineering & Ingestion

To feed both the Quantitative Engine and the GenAI Agent, the platform requires a massive, continuous influx of clean data. This is managed through a decoupled, event-driven architecture designed to handle high throughput without blocking the main API.

### The Macroeconomic News ETL Pipeline
News ingestion relies on a robust Producer-Consumer architecture:

* **The Producer (`run_news_etl.py`):** A daily cronjob queries premium financial domains (WSJ, Bloomberg, Reuters) via NewsAPI. It parses and filters articles one by one, dropping any empty or irrelevant payloads before pushing them to the API.
* **The Ingestion Gateway (`ingestion.py`):** An asynchronous FastAPI endpoint. 
  * *Idempotency & Audit:* Immediately persists the raw JSON payload into an S3-compatible **MinIO Data Lake**.
  * *Decoupling:* Pushes the validated payload into a **RabbitMQ** `news_queue` and immediately returns a `202 Accepted` response.
* **The NLP Worker (`worker/main.py`):** An isolated daemon consuming messages from RabbitMQ. 
  1. Validates the JSON schema via Pydantic.
  2. Applies **FinBERT** (`ProsusAI/finbert`) to classify financial sentiment.
  3. Chunks the text using LangChain's `RecursiveCharacterTextSplitter`.
  4. Embeds the chunks using SentenceTransformers (`MiniLM-L12-v2`).
  5. Upserts the vectors into **Pinecone**.
* **Resilience (Retry & DLQ):** The worker features a dual-queue fault tolerance system. 
  * Unrecoverable logical errors (e.g., empty texts or Pydantic validation failures) are sent directly to the **Dead Letter Queue (news_dlq)**. 
  * Infrastructure errors (e.g., Pinecone network timeouts) are sent to a `news_retry_queue` equipped with an `x-message-ttl` of 10,000ms (10 seconds). After the TTL expires, the message is automatically re-queued into the main queue for another attempt, up to a strict `MAX_RETRIES` limit.

### Market Data Ingestion (`run_market_data.py`)
To feed the Quantitative Engine, a dedicated cronjob runs at `01:00 UTC` (post-market close). It fetches the latest pricing data via `yfinance`, handles timezone normalization (NYC to UTC), and executes safe `UPSERT` operations into the PostgreSQL `market_data` table to prevent duplicates.

## 5. Asynchronous Price Alerts System

The platform features a real-time price monitoring system decoupled from the main API thread. Users can provision alerts via the Chat Agent (using Tool Calling) or directly via the `alerts` router.

* **Alert Worker Daemon (`alert_worker.py`):** A standalone Python process that acts as an infinite polling daemon. 
  * Every 60 seconds, it queries PostgreSQL for all `active` alerts across all users and tenants.
  * It deduplicates the requested tickers and polls `yfinance` for live market data.
  * If a target threshold is crossed (`above` or `below`), it executes an HTTP POST to the **Telegram Bot API**, delivering an instant push notification to the user's device, and transitions the alert state to `triggered`.
* **Automated Database Maintenance:** To prevent the `price_alerts` table from bloating and degrading the daemon's polling speed, an asynchronous thread (`prune_old_alerts_async`) fires every hour to permanently hard-delete `triggered` or `cancelled` alerts.

## 6. API Security, Rate Limiting & Multi-Tenancy

The FastAPI layer (`app/main.py`) is engineered to support a B2B SaaS operational model, ensuring strict data isolation between corporate clients (Tenants).

* **JWT Multi-Tenancy (`security.py`):** Authentication is handled via **Supabase**. The API intercepts the Bearer token, decodes the JWT, and extracts both the `user_id` (from the `sub` claim) and the `tenant_id` (from the `app_metadata` claim). Every single database query and Pinecone namespace search is strictly filtered by these two IDs, guaranteeing absolute data isolation between clients.
* **Defense-in-Depth Gateway:** 
  * **Payload Limiting:** A custom ASGI middleware inspects the `content-length` header and rejects any request larger than 2MB (`HTTP 413`) to prevent memory-exhaustion attacks.
  * **Rate Limiting (SlowAPI):** The `/chat` endpoint is constrained to 5 requests per minute per authenticated user (or IP fallback). 
* **PII Masking Engine (`utils/pii_masking.py`):** To comply with financial data privacy laws, all user prompts pass through a Regex interception layer *before* being processed or cached. Credit cards, emails, phone numbers, and National IDs are scrubbed and replaced with placeholders (e.g., `[EMAIL_REDACTED]`), ensuring zero PII leakages to third-party LLM providers.
* **Token Economy:** A simulated credit system (`chats.py`) deducts dynamic costs based on the exact Gemini model utilized during the semantic routing cascade, blocking access (`HTTP 402`) when credits are exhausted.

## 7. LLM Evaluation (MLOps) & CI/CD Pipelines

To guarantee the reliability of the Generative AI agent in a high-stakes financial environment, the system implements an automated evaluation suite based on the **LLM-as-a-Judge** paradigm. Metrics are logged continuously to **DagsHub / MLflow**.

### Evaluation Framework (`eval/`)
The testing suite does not rely on subjective human review. Instead, it evaluates the agent against a curated "Golden Dataset" (`eval_dataset.json`) using strict empirical metrics:

* **Tool Calling Accuracy (93.33%):** Measured by `evaluate_baseline.py`. The script evaluates if the agent selects the exact correct tool (or sequence of tools) for a given query without hallucinating. It features its own `MODEL_CASCADE` logic, ensuring tests complete successfully even if the primary Gemini API is experiencing high demand.
* **Faithfulness & Relevance (RAGAS):** Measured by `evaluate_rag.py`. A secondary "Judge" LLM (`gemini-2.5-flash`) inspects the agent's responses. 
  * *Faithfulness:* Evaluates if the agent's answer is strictly grounded in the retrieved Pinecone context (0 hallucination tolerance).
  * *Relevance:* Evaluates if the agent actually answered the user's prompt rather than going off-topic.

### Continuous Integration & Delivery (CI/CD)
The repository is governed by strict GitHub Actions workflows (`ci_cd_pipeline.yml`) to ensure production stability:

1. **Continuous Integration (CI):** On every push or PR to `main`, the pipeline spins up an isolated Ubuntu runner, installs dependencies, and executes the `pytest` suite. This suite includes mocked tests for the Agent Router (verifying Pinecone reranking and cache hits), Alert validation paths, and Data Engineering cutoff logic, all powered by an in-memory SQLite database.
2. **Continuous Delivery (CD):** Once the test suite passes, the pipeline authenticates with the GitHub Container Registry (`ghcr.io`), builds the multi-container architecture (API and RabbitMQ Worker), and pushes the `latest` Docker images, making them immediately available for Cloud deployment.

## 8. System Architecture & Local Setup

The system is designed as a decoupled, microservices-based infrastructure utilizing Docker Compose. This ensures identical behavior across local testing and cloud production environments.

### Core Components (`compose.yml`)
* **`api`:** The FastAPI application serving the GenAI agent, ingestion endpoints, and auth.
* **`worker`:** The async RabbitMQ consumer handling FinBERT sentiment analysis and Pinecone embeddings.
* **`alert-worker`:** The background daemon polling PostgreSQL and triggering Telegram notifications.
* **`rabbitmq`:** The central message broker handling the `news_queue`, `news_retry_queue`, and `news_dlq`.
* **`minio`:** An S3-compatible Data Lake storing raw JSON payloads for auditing.

### Prerequisites
* Docker & Docker Compose
* External Accounts: Pinecone, Supabase, Google AI Studio (Gemini), NewsAPI.
* *(Optional)* Telegram Bot Token for push alerts.

### Quickstart

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/YourUsername/genai_core_pipeline.git](https://github.com/YourUsername/genai_core_pipeline.git)
   cd genai_core_pipeline
   ```
2. **Configure Environment Variables:**
    * Copy the example environment file and fill in your API keys and credentials.
    ```bash
    cp .env.example .env
    ```
    *Note: For the GitHub Actions Cronjobs to work, you must also inject these exact variables into your GitHub Repository Secrets.*
3. **Launch the Infraestructure:**
    ```bash
    docker compose up --build
    ```
4. **Verify Services:**
    * API Docs (Swagger): http://localhost:8000/docs

    *  RabbitMQ Console: http://localhost:15672

    *  MinIO Console: http://localhost:9001

    *  MLflow / DagsHub: Track experiments remotely using your configured tracking URI.
    

