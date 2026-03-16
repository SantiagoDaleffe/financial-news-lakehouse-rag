import yfinance as yf
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import PriceAlert

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@airflow-postgres:5432/airflow")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_live_stock_price(ticker: str, period: str="1d") -> dict:
    """
    fetches current price and currency for a given stock or crypto
    args:
        ticker: market symbol (eg 'AAPL', 'BTC-USD')
        period: time window (eg '1d', '5d')
    """
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        price = info.get('currentPrice') or info.get('regularMarketPrice')
        currency = info.get('currency')
        return {"ticker": ticker, "price": price, "period": period, 'currency': currency}
    except Exception as e:
        return {"error": str(e), "ticker": ticker, "period": period}

def calculate_math(expression: str) -> dict:
    """
    evaluates exact math calculations and financial operations
    use this whenever you need to calculate percentages, sums, multiplications etc
    args:
        expression: valid python math expression (eg '15.5 * 185 * 1.12')
    """
    try:
        result = eval(expression, {"__builtins__": None}, {})
        return {"result": str(result), "expression": expression}
    except Exception as e:
        return {"error": f"math error: {str(e)}"}
    
def set_price_alert(ticker: str, target_price: float, condition: str, user_id: str = "default_user", tenant_id: str = "public_b2c") -> dict:
    """
    saves a price alert in the database to notify the user later
    use this whenever the user asks to be notified or alerted about a price
    args:
        ticker: market symbol (eg 'AAPL', 'BTC-USD')
        target_price: target number
        condition: 'above' if waiting for price to go up, 'below' if waiting for drop
    """
    db = SessionLocal()
    try:
        new_alert = PriceAlert(
            tenant_id=tenant_id, 
            user_id=user_id, 
            ticker=ticker.upper(), 
            target_price=float(target_price), 
            condition=condition
        )
        db.add(new_alert)
        db.commit()
        return {"message": f"alert saved. system will notify when {ticker} goes {condition} {target_price} usd"}
    except Exception as e:
        db.rollback()
        return {"error": f"db error saving alert: {str(e)}"}
    finally:
        db.close()