import time
import os
import requests
import socket
import requests.packages.urllib3.util.connection as urllib3_cn
import yfinance as yf
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import PriceAlert
import logging
import threading

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

def allowed_gai_family():
    return socket.AF_INET


urllib3_cn.allowed_gai_family = allowed_gai_family

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def send_telegram_message(message: str) -> None:
    """
    Sends a message via Telegram bot.

    Args:
        message (str): The message to send.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    if not token or not chat_id:
        logging.warning(
            f"Telegram credentials missing. Fallback console output: {message}"
        )
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}

    try:
        res = requests.post(url, json=payload, timeout=10, verify=False)
        res.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to transmit Telegram payload: {str(e)}")


def process_alerts() -> None:
    """
    Evaluates all active price alerts and triggers notifications when conditions are met.

    This function:
    1. Queries the database for all alerts with 'active' status
    2. Fetches current stock prices from Yahoo Finance for each unique ticker
    3. Evaluates each alert's trigger condition (above/below target price)
    4. Sends Telegram notifications for triggered alerts
    5. Updates triggered alerts to 'triggered' status in the database

    The overall pipeline works as follows:
    - Main daemon loop runs every 60 seconds
    - Each cycle calls process_alerts() to check market conditions
    - Every 60 cycles (after 1 hour), prune_old_alerts_async() removes stale data
    - Telegram messages are sent when price conditions are met
    - Database is updated with alert status changes
    - Errors are caught and logged without stopping the daemon
    """
    db = SessionLocal()
    try:
        alerts = db.query(PriceAlert).filter(PriceAlert.status == "active").all()

        if not alerts:
            logging.info("No active alerts to check. Waiting.")
            return
        
        logging.info(f"Checking {len(alerts)} active alerts...")

        unique_tickers = set([a.ticker for a in alerts])
        current_prices = {}

        for ticker in unique_tickers:
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                current_prices[ticker] = info.get("currentPrice") or info.get(
                    "regularMarketPrice"
                )
            except Exception as e:
                logging.error(f"Yahoo Finance telemetry failed for {ticker}: {str(e)}")

        alerts_triggered = 0
        for alert in alerts:
            price = current_prices.get(alert.ticker)
            if not price:
                continue

            trigger = False
            if alert.condition == "above" and price >= alert.target_price:
                trigger = True
            elif alert.condition == "below" and price <= alert.target_price:
                trigger = True

            if trigger:
                msg = f"*MARKET ALERT*\n\nAsset: *{alert.ticker}*\nCondition Met: `{alert.condition} {alert.target_price}`\nCurrent Price: *{price} USD*"
                logging.info(
                    f"Trigger condition met for {alert.ticker} at {price} USD."
                )

                send_telegram_message(msg)

                alert.status = "triggered"
                alerts_triggered += 1

        if alerts_triggered > 0:
            db.commit()

    except Exception as e:
        logging.error(f"Critical failure in evaluation loop: {str(e)}")
        db.rollback()
    finally:
        db.close()


def prune_old_alerts() -> None:
    """
    Hard-deletes alerts that have already been 'triggered' or 'cancelled'.
    Maintains table performance and prevents dead data buildup.
    """
    db = SessionLocal()
    try:
        deleted_count = (
            db.query(PriceAlert)
            .filter(PriceAlert.status.in_(["triggered", "cancelled"]))
            .delete(synchronize_session=False)
        )
        db.commit()
        if deleted_count > 0:
            logging.info(f"Routine Prune: Cleared {deleted_count} inactive alerts.")
    except Exception as e:
        logging.error(f"Pruning failure: {str(e)}")
        db.rollback()
    finally:
        db.close()


def prune_old_alerts_async() -> None:
    """Trigger the pruning of old alerts in a separate thread to avoid blocking the main loop."""
    thread = threading.Thread(target=prune_old_alerts)
    thread.start()


if __name__ == "__main__":
    logging.info("Initializing Alert Polling Daemon. Establishing market connection...")

    cycles = 0

    while True:
        try:
            process_alerts()

            cycles += 1
            if cycles >= 60:
                prune_old_alerts_async()
                cycles = 0

        except Exception as crash_e:
            logging.error(f"Daemon recovered from fatal crash: {crash_e}")

        time.sleep(60)
