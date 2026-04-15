from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
from dateutil import parser

API_KEY = os.getenv("NEWS_API_KEY")
INGESTION_URL = os.getenv("INGESTION_API_URL")

TICKERS_QUERY = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "Bitcoin", "Ethereum"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def parse_articles(articles_list):
    parsed_articles = []
    for article in articles_list:
        if not article.get("title") or article.get("title") == "[Removed]":
            continue

        parse = {
            "title": article.get("title", "").strip(),
            "description": article.get("description", "").strip(),
            "url": article.get("url"),
            "publishedAt": article.get("publishedAt"),
        }

        parsed_articles.append(parse)
    return parsed_articles

def extract_and_ingest(limit=100):
    if not API_KEY:
        raise Exception("missing NEWS_API_KEY in env")

    yesterday = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    domains = "wsj.com,bloomberg.com,reuters.com,cnbc.com,finance.yahoo.com"
    query = " OR ".join(TICKERS_QUERY)
    url = f"https://newsapi.org/v2/everything?q={query}&domains={domains}&from={yesterday}&sortBy=publishedAt&pageSize={limit}&apiKey={API_KEY}"

    print(f"Fetching news from API with query: {query}", flush=True)
    response = requests.get(url)
    
    if response.status_code == 200:
        raw_articles = response.json().get("articles", [])
        clean_articles = parse_articles(raw_articles)
        print("Fetched", len(clean_articles), "articles. Sending to ingestion API...", flush=True)

        count = 0
        for article in clean_articles:
            embedd = f"TITLE: {article['title']}. DESCRIPTION: {article['description']}. DATE: {article['publishedAt']}. URL: {article['url']}"

            dt = parser.parse(article['publishedAt'])
            timestamp = dt.timestamp()
            
            payload = {
                "text": embedd,
                "published_at": timestamp,
                "url": article['url']
            }

            try:
                r = requests.post(INGESTION_URL, json=payload)
                if r.status_code == 200:
                    count += 1
            except Exception as e:
                print("error posting article to api", str(e), flush=True)

        print(f"Ingested {count} articles ok. Initiating database prune...", flush=True)
        try:
            prune_url = INGESTION_URL.replace("/ingest", "/prune")
            prune_req = requests.delete(f"{prune_url}?days=30")
            print(f"Prune result: {prune_req.json()}", flush=True)
        except Exception as e:
            print("Error calling prune endpoint", str(e), flush=True)
            
        return f"Pipeline finished. Ingested: {count}"
    else:
        raise Exception(f"failed fetching news status {response.status_code} {response.text}")

with DAG(
    dag_id="daily_financial_news_etl",
    default_args=default_args,
    description="Daily ETL pulling financial news and pruning old vectors",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "news", "rag"],
) as dag:
    ingest_task = PythonOperator(
        task_id="news_ingest_and_prune",
        python_callable=extract_and_ingest,
        op_kwargs={"limit": 50},
    )