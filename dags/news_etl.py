from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
from dateutil import parser
import urllib.parse

API_KEY = os.getenv("NEWS_API_KEY")
INGESTION_URL = os.getenv("INGESTION_API_URL")

TICKER_MAPPINGS = {
    "AAPL": "Apple OR AAPL",
    "MSFT": "Microsoft OR MSFT",
    "GOOGL": "Google OR Alphabet OR GOOGL",
    "AMZN": "Amazon OR AMZN",
    "NVDA": "Nvidia OR NVDA",
    "META": "Meta OR Facebook OR META",
    "TSLA": "Tesla OR TSLA",
    "Bitcoin": "Bitcoin OR BTC",
    "Ethereum": "Ethereum OR ETH",
    "SPY": "S&P 500 OR SPY",
    "QQQ": "Nasdaq OR QQQ",
    "DIA": "Dow Jones OR DIA",
    "IWM": "Russell 2000 OR IWM",
    "GLD": "Gold OR GLD",
    "TLT": "Treasury Bonds OR TLT"
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def parse_articles(articles_list, ticker):
    parsed_articles = []
    for article in articles_list:
        if not article.get("title") or article.get("title") == "[Removed]":
            continue

        title = article.get("title", "").strip()
        desc = article.get("description", "").strip()

        parse = {
            "title": title,
            "description": desc,
            "url": article.get("url"),
            "publishedAt": article.get("publishedAt"),
            "matched_tickers": [ticker]
        }
        parsed_articles.append(parse)
    return parsed_articles

def extract_and_ingest(limit_per_ticker=10):
    if not API_KEY:
        raise Exception("missing NEWS_API_KEY in env")

    yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    domains = "wsj.com,bloomberg.com,reuters.com,cnbc.com,finance.yahoo.com"
    
    total_ingested = 0
    print(f"Fetching {len(TICKER_MAPPINGS)} tickers", flush=True)

    for ticker, search_query in TICKER_MAPPINGS.items():
        encoded_query = urllib.parse.quote(search_query)
        url = f"https://newsapi.org/v2/everything?q={encoded_query}&domains={domains}&from={yesterday}&sortBy=publishedAt&pageSize={limit_per_ticker}&apiKey={API_KEY}" 
        print(f'Fetching data for {ticker} (Query: {search_query})', flush=True)
        try:
            response = requests.get(url)
            if response.status_code == 200:
                raw_articles = response.json().get("articles", [])
                clean_articles = parse_articles(raw_articles, ticker)
                print(f"Fetched {len(clean_articles)} articles for {ticker}.", flush=True)
                
                for article in clean_articles:
                    embedd = f"TITLE: {article['title']}. DESCRIPTION: {article['description']}. DATE: {article['publishedAt']}. URL: {article['url']}"
                    dt = parser.parse(article['publishedAt'])
                    
                    payload = {
                        "text": embedd,
                        "published_at": dt.timestamp(),
                        "url": article['url'],
                        "tickers": article['matched_tickers']
                    }
                    
                    try:
                        r = requests.post(INGESTION_URL, json=payload)
                        if r.status_code == 200:
                            total_ingested += 1
                    except Exception as e:
                        print(f"Failed to ingest article for{ticker}: {str(e)}", flush=True)
            else:
                print(f"Response error for {ticker}: {response.status_code}", flush=True)
        except Exception as e:
            print(f"Error fetching data for{ticker}: {str(e)}", flush=True)

    print(f"Ingestion Process completed. Total items ingested: {total_ingested}", flush=True)
    try:
        prune_url = INGESTION_URL.replace("/ingest", "/prune")
        prune_req = requests.delete(f"{prune_url}?days=30")
        print(f"ChromaDB pruned: {prune_req.json()}", flush=True)
    except Exception as e:
        print(f"Couldnt complete prune process: {str(e)}", flush=True)
        
    return f"Pipeline finalizado con éxito. Items cargados: {total_ingested}"

with DAG(
    dag_id="daily_financial_news_etl",
    default_args=default_args,
    description="Daily ETL pulling financial news individually per ticker and pruning old vectors",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "news", "rag"],
) as dag:
    
    ingest_task = PythonOperator(
        task_id="news_ingest_and_prune",
        python_callable=extract_and_ingest,
        op_kwargs={"limit_per_ticker": 10},
    )