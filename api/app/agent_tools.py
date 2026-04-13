import yfinance as yf
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import PriceAlert, PortfolioAccount, PortfolioPosition, PortfolioTransaction

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
        
def get_user_alerts(user_id: str) -> dict:
    """
    Fetches all active price alerts for the current user.
    Use this to see what alerts the user has before modifying or deleting them.
    args:
        user_id: the ID of the current user
    """
    db = SessionLocal()
    try:
        alerts = db.query(PriceAlert).filter(
            PriceAlert.user_id == user_id,
            PriceAlert.status == "active"
        ).all()
        
        if not alerts:
            return {"message": "No active alerts found."}
            
        return {
            "alerts": [
                {"id": a.id, "ticker": a.ticker, "target_price": a.target_price, "condition": a.condition} 
                for a in alerts
            ]
        }
    except Exception as e:
        return {"error": f"Database error: {str(e)}"}
    finally:
        db.close()

def update_price_alert(alert_id: int, new_target_price: float, user_id: str) -> dict:
    """
    Updates the target price of an existing alert.
    args:
        alert_id: the ID of the alert to modify
        new_target_price: the new numeric price target
        user_id: the ID of the current user
    """
    db = SessionLocal()
    try:
        alert = db.query(PriceAlert).filter(
            PriceAlert.id == alert_id,
            PriceAlert.user_id == user_id,
            PriceAlert.status == "active"
        ).first()
        
        if not alert:
            return {"error": f"Alert {alert_id} not found or doesn't belong to you."}
            
        alert.target_price = float(new_target_price)
        db.commit()
        return {"message": f"Alert {alert_id} updated. New target price: {new_target_price}"}
    except Exception as e:
        db.rollback()
        return {"error": f"Database error: {str(e)}"}
    finally:
        db.close()

def delete_price_alert(alert_id: int, user_id: str) -> dict:
    """
    Cancels/deletes an active price alert.
    args:
        alert_id: the ID of the alert to delete
        user_id: the ID of the current user
    """
    db = SessionLocal()
    try:
        alert = db.query(PriceAlert).filter(
            PriceAlert.id == alert_id,
            PriceAlert.user_id == user_id
        ).first()
        
        if not alert:
            return {"error": f"Alert {alert_id} not found or doesn't belong to you."}
            
        alert.status = "cancelled"
        db.commit()
        return {"message": f"Alert {alert_id} cancelled successfully."}
    except Exception as e:
        db.rollback()
        return {"error": f"Database error: {str(e)}"}
    finally:
        db.close()
        
def get_portfolio_status(user_id: str) -> dict:
    """
    Fetches the user's current paper trading portfolio, including cash balance and active stock positions.
    Use this whenever the user asks about their balance, what they own, or how their investments are doing.
    args:
        user_id: the ID of the current user
    """
    db = SessionLocal()
    try:

        account = db.query(PortfolioAccount).filter(PortfolioAccount.user_id == user_id).first()
        if not account:
            account = PortfolioAccount(user_id=user_id, cash_balance=10000.0)
            db.add(account)
            db.commit()
            

        positions = db.query(PortfolioPosition).filter(
            PortfolioPosition.user_id == user_id, 
            PortfolioPosition.quantity > 0
        ).all()
        
        pos_list = [
            {
                "ticker": p.ticker, 
                "quantity": p.quantity, 
                "average_buy_price": p.average_buy_price
            } for p in positions
        ]
            
        return {
            "cash_balance_usd": account.cash_balance,
            "active_positions": pos_list
        }
    except Exception as e:
        return {"error": f"Database error: {str(e)}"}
    finally:
        db.close()

def execute_paper_trade(ticker: str, action: str, quantity: float, user_id: str) -> dict:
    """
    Executes a simulated buy or sell order in the paper trading environment.
    Use this strictly when the user explicitly asks to buy or sell an asset.
    args:
        ticker: market symbol (eg 'AAPL', 'BTC-USD')
        action: strictly 'BUY' or 'SELL'
        quantity: numeric amount of shares/coins to trade
        user_id: the ID of the current user
    """
    action = action.upper()
    if action not in ["BUY", "SELL"]:
        return {"error": "action must be exactly BUY or SELL"}
    if float(quantity) <= 0:
        return {"error": "quantity must be greater than 0"}

    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        current_price = info.get('currentPrice') or info.get('regularMarketPrice')
        if not current_price:
            return {"error": f"could not fetch live market price for {ticker}. market might be closed."}
    except Exception as e:
        return {"error": f"error fetching live price: {str(e)}"}

    total_amount = float(quantity) * float(current_price)

    db = SessionLocal()
    try:
        account = db.query(PortfolioAccount).filter(PortfolioAccount.user_id == user_id).first()
        if not account:
            account = PortfolioAccount(user_id=user_id, cash_balance=10000.0)
            db.add(account)
            
        position = db.query(PortfolioPosition).filter(
            PortfolioPosition.user_id == user_id,
            PortfolioPosition.ticker == ticker.upper()
        ).first()

        if action == "BUY":
            if account.cash_balance < total_amount:
                return {"error": f"insufficient funds. need {total_amount:.2f} USD, have {account.cash_balance:.2f} USD."}
            
            account.cash_balance -= total_amount
            
            if position:

                total_cost_before = position.quantity * position.average_buy_price
                total_cost_new = total_cost_before + total_amount
                new_quantity = position.quantity + float(quantity)
                
                position.average_buy_price = total_cost_new / new_quantity
                position.quantity = new_quantity
            else:
                new_pos = PortfolioPosition(
                    user_id=user_id,
                    ticker=ticker.upper(),
                    quantity=float(quantity),
                    average_buy_price=current_price
                )
                db.add(new_pos)

        elif action == "SELL":
            if not position or position.quantity < float(quantity):
                current_qty = position.quantity if position else 0
                return {"error": f"insufficient shares. trying to sell {quantity}, but you only own {current_qty}."}
            

            account.cash_balance += total_amount

            position.quantity -= float(quantity)
            

            if position.quantity == 0:
                db.delete(position)

        transaction = PortfolioTransaction(
            user_id=user_id,
            ticker=ticker.upper(),
            transaction_type=action,
            quantity=float(quantity),
            price_per_unit=current_price,
            total_amount=total_amount
        )
        db.add(transaction)

        db.commit()
        return {
            "status": "success",
            "message": f"Successfully executed {action} for {quantity} shares of {ticker} at {current_price:.2f} USD per unit.",
            "total_transaction_value": total_amount,
            "remaining_cash_balance": account.cash_balance
        }

    except Exception as e:
        db.rollback()
        return {"error": f"database error executing trade: {str(e)}"}
    finally:
        db.close()