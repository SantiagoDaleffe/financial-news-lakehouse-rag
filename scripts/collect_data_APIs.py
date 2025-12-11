import time
import logging
import re
from datetime import datetime, timedelta
from scripts.parser_APIs import parse_cryptocompare
import requests
from scripts.utils import save_to_bronze

# Filter bitcoin (regex)
BTC_RE = re.compile(r"\b(bitcoin|btc|satoshi)\b", flags=re.IGNORECASE)

def _get_keywords(item):
    """keywords/tags"""
    for k in ("KEYWORDS", "keywords", "TAGS", "tags"):
        val = item.get(k)
        if not val:
            continue
        if isinstance(val, str):
            return [t.strip().lower() for t in val.split(",") if t.strip()]
        if isinstance(val, list):
            return [str(t).strip().lower() for t in val]
    return []

def _get_text_blob(item):
    """join title + body/content"""
    title = (item.get("TITLE") or item.get("title") or "").strip().lower()
    body = (item.get("BODY") or item.get("CONTENT") or item.get("content") or "").strip().lower()
    return f"{title} {body}"

def fetch_cryptocompare_day(date=None, per_day=50):
    if date is None:
        date = datetime.utcnow()
    
    # Timestamp (end of day)
    to_ts = int(datetime(date.year, date.month, date.day, 23, 59, 59).timestamp())

    url = "https://data-api.coindesk.com/news/v1/article/list"
    params = {"lang": "EN", "limit": per_day, "to_ts": to_ts, "tag": "bitcoin", "source_key": "coindesk"}
    headers = {"Content-type": "application/json; charset=UTF-8"}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("Data", []) or []
    except Exception as e:
        logging.error(f"Error API: {e}")
        return []

    filtered = []
    for it in data:
        text_blob = _get_text_blob(it)
        keywords = _get_keywords(it)
        
        if any("bitcoin" in k or "btc" in k for k in keywords) or BTC_RE.search(text_blob):
            filtered.append(it)

    return filtered

def run_collection(days_back=5):
    """process days and save to MinIO"""
    logging.info(f"collecting coindesk (last {days_back} days)")
    
    all_articles = []
    seen_ids = set()
    today = datetime.utcnow()

    for i in range(days_back):
        day = today - timedelta(days=i)
        logging.info(f"processing {day.date()}")
        
        raw_data = fetch_cryptocompare_day(date=day, per_day=20)
        parsed_data = [parse_cryptocompare(a) for a in raw_data]

        # deduplicate in memory
        new_articles = []
        for p in parsed_data:
            if p["_id"] not in seen_ids:
                new_articles.append(p)
                seen_ids.add(p["_id"])
        
        all_articles.extend(new_articles)
        time.sleep(0.5)

    # save on MinIO
    if all_articles:
        filename = f"coindesk_{int(time.time())}.json"
        save_to_bronze(all_articles, filename, source_type="coindesk_api")
    else:
        logging.warning("no new articles found")

if __name__ == "__main__":
    # test
    run_collection(days_back=10)