import time
import os
import requests
import socket
import requests.packages.urllib3.util.connection as urllib3_cn
import yfinance as yf
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import PriceAlert

def allowed_gai_family():
    return socket.AF_INET
urllib3_cn.allowed_gai_family = allowed_gai_family

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_message(message: str):
    """sends push notification to user via telegram"""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    
    if not token or not chat_id:
        print("missing telegram credentials in env. console fallback:", message)
        return
        
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    try:
        res = requests.post(url, json=payload, timeout=10, verify=False)
        res.raise_for_status()
    except Exception as e:
        print("error sending telegram message", str(e))

def process_alerts():
    db = SessionLocal()
    try:
        alerts = db.query(PriceAlert).filter(PriceAlert.status == "active").all()
        
        if not alerts:
            print("no active alerts to check", flush=True)
            return

        print("checking", len(alerts), "active alerts", flush=True)

        unique_tickers = set([a.ticker for a in alerts])
        current_prices = {}

        for ticker in unique_tickers:
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                current_prices[ticker] = info.get('currentPrice') or info.get('regularMarketPrice')
            except Exception as e:
                print("error fetching yahoo finance data for", ticker, str(e), flush=True)

        alerts_triggered = 0
        for alert in alerts:
            price = current_prices.get(alert.ticker)
            if not price:
                continue

            trigger = False
            if alert.condition == 'above' and price >= alert.target_price:
                trigger = True
            elif alert.condition == 'below' and price <= alert.target_price:
                trigger = True

            if trigger:
                msg = f"*MARKET ALERT*\n\nasset *{alert.ticker}* reached target\ncondition: `{alert.condition} {alert.target_price}`\ncurrent price: *{price} USD*"
                print("alert triggered", alert.ticker, "at", price, flush=True)
                
                send_telegram_message(msg)
                
                alert.status = "triggered"
                alerts_triggered += 1

        if alerts_triggered > 0:
            db.commit()

    except Exception as e:
        print("general error in worker process", str(e), flush=True)
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    print("starting alert worker. waiting for market data...", flush=True)
    while True:
        process_alerts()
        print("sleeping for 60s...", flush=True)
        time.sleep(60)