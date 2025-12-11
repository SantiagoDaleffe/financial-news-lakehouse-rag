import feedparser
import hashlib
from datetime import datetime
from scripts.utils import save_to_bronze
import logging

CHROME_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"

RSS_FEEDS = {
    "cointelegraph": "https://cointelegraph.com/rss/tag/bitcoin",
    "yahoo_finance": "https://finance.yahoo.com/news/rssindex",
    "decrypt": "https://decrypt.co/feed"
}

def generate_id(text):
    return hashlib.md5(text.encode()).hexdigest()

def parse_rss_entry(entry, source):
    """parse rss entry"""
    return {
        "_id": generate_id(f"{source}::{entry.title}::{entry.link}"),
        "title": entry.title,
        "description": entry.get("summary", "") or entry.get("description", ""),
        "content": "",
        "url": entry.link,
        "source": source,
        "published_at": entry.get("published", str(datetime.now())),
        "collected_at": datetime.now(),
        "extra": {
            "tags": [t.term for t in entry.get("tags", [])],
            "guid": entry.get("id", "")
        }
    }

def run_rss_collection():
    logging.info("collecting rss")
    all_items = []

    for source, url in RSS_FEEDS.items():
        try:
            logging.info(f"feed: {source}")
            feed = feedparser.parse(url, agent=CHROME_USER_AGENT)
            
            items = []
            for entry in feed.entries:
                text_blob = (entry.title + " " + entry.get("summary", "")).lower()
                if "bitcoin" in text_blob or "btc" in text_blob or "crypto" in text_blob:
                    parsed = parse_rss_entry(entry, source)
                    items.append(parsed)
            
            logging.info(f"found {len(items)} articles in {source}")
            all_items.extend(items)
            
        except Exception as e:
            logging.error(f"error reading {source}: {e}")

    # MinIO
    if all_items:
        filename = f"rss_feed_{int(datetime.now().timestamp())}.json"
        save_to_bronze(all_items, filename, source_type="rss_feeds")

if __name__ == "__main__":
    run_rss_collection()