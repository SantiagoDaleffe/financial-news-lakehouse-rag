from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import json
import os

API_KEY = os.getenv("NEWS_API_KEY")
INGESTION_URL = os.getenv("INGESTION_API_URL", "http://api:8000/ingest")

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


def extract_and_ingest(limit=50):
    if not API_KEY:
        raise Exception("NEWS_API_KEY environment variable is not set.")

    yesterday = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    url = f"https://newsapi.org/v2/everything?q=Bitcoin&from={yesterday}&sortBy=publishedAt&pageSize={limit}&apiKey={API_KEY}"

    print(f"Fetching news from: {url.replace(API_KEY, '***')}")
    response = requests.get(url)
    if response.status_code == 200:
        raw_articles = response.json().get("articles", [])
        clean_articles = parse_articles(raw_articles)
        print(f"Fetched {len(clean_articles)} articles, ingesting to API...")

        count = 0
        for article in clean_articles:
            embedd = f"TITLE: {article['title']}. DESCRIPTION: {article['description']}. DATE: {article['publishedAt']}. URL: {article['url']}"

            payload = {"text": embedd}

            try:
                r = requests.post(INGESTION_URL, json=payload)
                if r.status_code == 200:
                    count += 1
            except Exception as e:
                print(f"Error ingesting article: {e}")

        return f"Ingested {count} articles successfully."
    else:
        raise Exception(
            f"Failed to fetch news: {response.status_code} - {response.text}"
        )


with DAG(
    dag_id="news_etl_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline for news articles",
    schedule="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "crypto"],
) as dag:
    ingest_task = PythonOperator(
        task_id="News_ingest",
        python_callable=extract_and_ingest,
        op_kwargs={"limit": 20},
    )
