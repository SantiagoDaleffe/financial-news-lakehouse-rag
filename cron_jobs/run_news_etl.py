import requests
import os
from dateutil import parser
import urllib.parse
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

def parse_articles(articles_list, ticker):
    parsed_articles = []
    for article in articles_list:
        if not article.get("title") or article.get("title") == "[Removed]":
            continue

        title = (article.get("title") or "").strip()
        desc = (article.get("description") or "").strip()

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
    if not INGESTION_URL:
        raise Exception("missing INGESTION_API_URL in env")

    yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    domains = "wsj.com,bloomberg.com,reuters.com,cnbc.com,finance.yahoo.com"
    
    total_ingested = 0
    logging.info(f"Fetching {len(TICKER_MAPPINGS)} tickers")

    for ticker, search_query in TICKER_MAPPINGS.items():
        encoded_query = urllib.parse.quote(search_query)
        url = f"https://newsapi.org/v2/everything?q={encoded_query}&domains={domains}&from={yesterday}&sortBy=publishedAt&pageSize={limit_per_ticker}&apiKey={API_KEY}" 
        logging.info(f'Fetching data for {ticker} (Query: {search_query})')
        try:
            response = requests.get(url)
            if response.status_code == 200:
                raw_articles = response.json().get("articles", [])
                clean_articles = parse_articles(raw_articles, ticker)
                logging.info(f"Fetched {len(clean_articles)} articles for {ticker}.")
                
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
                        logging.error(f"Failed to ingest article for {ticker}: {str(e)}")
            else:
                logging.error(f"Response error for {ticker}: {response.status_code}")
        except Exception as e:
            logging.error(f"Error fetching data for {ticker}: {str(e)}")

    logging.info(f"Ingestion Process completed. Total items ingested: {total_ingested}")
    
    try:
        prune_url = INGESTION_URL.replace("/ingest", "/prune")
        prune_req = requests.delete(f"{prune_url}?days=30")
        logging.info(f"Database pruned: {prune_req.json()}")
    except Exception as e:
        logging.error(f"Couldnt complete prune process: {str(e)}")
        
    return f"Loaded items: {total_ingested}"

if __name__ == "__main__":
    extract_and_ingest(limit_per_ticker=10)