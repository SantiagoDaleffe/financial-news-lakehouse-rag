import requests
import os
from dateutil import parser
import urllib.parse
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any
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

def parse_articles(articles_list: List[Dict[str, Any]], ticker: str) -> List[Dict[str, Any]]:
    """
    Parses and filters raw articles fetched from the NewsAPI.
    
    Validates the presence of meaningful content. Articles marked as '[Removed]'
    or lacking both title and description are explicitly discarded to prevent 
    downstream pollution in the Vector Store.

    Args:
        articles_list (List[Dict[str, Any]]): The raw JSON payload from NewsAPI.
        ticker (str): The financial ticker associated with the search query.

    Returns:
        List[Dict[str, Any]]: A list of cleaned, validated, and structured article dictionaries.
    """
    parsed_articles = []
    for article in articles_list:
        title = (article.get("title") or "").strip()
        desc = (article.get("description") or "").strip()
        
        if not title or title == "[Removed]":
            continue
            
        if not title or not desc:
            logging.warning(f"Discarded empty article for {ticker}: {article.get('url')}")
            continue

        parse = {
            "title": title,
            "description": desc,
            "url": article.get("url"),
            "publishedAt": article.get("publishedAt"),
            "matched_tickers": [ticker]
        }
        parsed_articles.append(parse)
    return parsed_articles


def extract_and_ingest(limit_per_ticker: int = 10) -> str:
    """
    Executes the Daily ETL pipeline for financial news ingestion.

    Iterates through the defined universe of tickers, queries the NewsAPI for 
    the last 48 hours of macroeconomic data across premium financial domains, 
    and POSTs the validated payloads to the central Ingestion API.

    Args:
        limit_per_ticker (int): Maximum number of articles to retrieve per ticker. Defaults to 10.

    Raises:
        EnvironmentError: If required API keys or URLs are missing from the environment.

    Returns:
        str: A summary string containing the total number of successfully ingested items.
    """
    if not API_KEY:
        raise EnvironmentError("CRITICAL: NEWS_API_KEY is missing from environment variables.")
    if not INGESTION_URL:
        raise EnvironmentError("CRITICAL: INGESTION_API_URL is missing from environment variables.")

    yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    domains = "wsj.com,bloomberg.com,reuters.com,cnbc.com,finance.yahoo.com"
    
    total_ingested = 0
    logging.info(f"Initiating extraction for {len(TICKER_MAPPINGS)} target assets.")

    for ticker, search_query in TICKER_MAPPINGS.items():
        encoded_query = urllib.parse.quote(search_query)
        url = f"https://newsapi.org/v2/everything?q={encoded_query}&domains={domains}&from={yesterday}&sortBy=publishedAt&pageSize={limit_per_ticker}&apiKey={API_KEY}" 
        logging.info(f"Fetching data for {ticker} (Query: {search_query})")
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                raw_articles = response.json().get("articles", [])
                clean_articles = parse_articles(raw_articles, ticker)
                logging.info(f"Extracted {len(clean_articles)} clean articles for {ticker}.")
                
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
                        else:
                            logging.warning(f"API rejected payload for {ticker}. Status: {r.status_code}")
                    except Exception as e:
                        logging.error(f"Network failure pushing {ticker} to Ingestion API: {str(e)}")
            else:
                logging.error(f"NewsAPI error for {ticker}. HTTP Status: {response.status_code}")
        except Exception as e:
            logging.error(f"Execution failed for {ticker}: {str(e)}")

    logging.info(f"ETL Cycle Completed. Total documents committed: {total_ingested}")
    
    # Prune outdated vectors to maintain semantic relevance (Context Window management)
    try:
        prune_url = INGESTION_URL.replace("/ingest", "/prune")
        prune_req = requests.delete(f"{prune_url}?days=30")
        logging.info(f"Vector Database Pruning: {prune_req.json()}")
    except Exception as e:
        logging.error(f"Pruning process failed: {str(e)}")
        
    return f"Loaded items: {total_ingested}"

if __name__ == "__main__":
    extract_and_ingest(limit_per_ticker=10)