import yfinance as yf
import ast
import operator
import re
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import (
    PriceAlert,
    PortfolioAccount,
    PortfolioPosition,
    PortfolioTransaction,
)

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_live_stock_price(ticker: str, period: str = "1d") -> dict:
    """
    Fetches real-time pricing and currency data for a specified financial asset.

    Args:
        ticker (str): The standard market symbol (e.g., 'AAPL', 'BTC-USD').
        period (str, optional): The temporal window for the query. Defaults to '1d'.

    Returns:
        dict: A dictionary containing the live price, currency, and ticker status.
    """
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        price = info.get("currentPrice") or info.get("regularMarketPrice")
        currency = info.get("currency")
        return {
            "ticker": ticker,
            "price": price,
            "period": period,
            "currency": currency,
        }
    except Exception as e:
        return {"error": str(e), "ticker": ticker, "period": period}


def calculate_math(expression: str) -> dict:
    """Securely evaluates mathematical expressions with safety checks.

    Parses and evaluates mathematical expressions containing only numeric literals
    and allowed operators (+, -, *, /, unary +/-). Results are validated and rounded
    to 4 decimal places for floating-point numbers.

    Args:
        expression (str): A mathematical expression string to evaluate (e.g., '2+2', '10/5').

    Raises:
        ValueError: If the expression contains unsupported constant types.
        ZeroDivisionError: If division by zero is attempted.
        TypeError: If unsupported operators or AST nodes are encountered.

    Returns:
        dict: A dictionary containing either:
            - "result": The computed value as a string, and "expression": the input expression
            - "error": An error message if evaluation fails
    """

    clean_expr = expression.replace(" ", "")

    if not re.match(r"^[\d\+\-\*\/\.\(\)]+$", clean_expr):
        return {
            "error": "Security Block: Invalid characters. Only pure math operations are allowed."
        }

    allowed_operators = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.USub: operator.neg,
        ast.UAdd: operator.pos,
    }

    def evaluate_node(node):
        """Recursively evaluate an AST node representing a numeric expression.

        Args:
            node (ast.AST): The AST node to evaluate. Supported node types are
                ast.Constant for numbers, ast.BinOp for binary operations, and
                ast.UnaryOp for unary operations.

        Raises:
            ValueError: If a constant node contains a non-numeric value.
            ZeroDivisionError: If a division by zero is attempted.
            TypeError: If an unsupported AST node or operator is encountered.

        Returns:
            int | float: The numeric result of evaluating the node.
        """
        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)):
                return node.value
            raise ValueError("Unsupported constant type")

        elif isinstance(node, ast.BinOp):
            left = evaluate_node(node.left)
            right = evaluate_node(node.right)

            if isinstance(node.op, ast.Div) and right == 0:
                raise ZeroDivisionError("Division by zero")

            op_func = allowed_operators.get(type(node.op))
            if not op_func:
                raise TypeError(
                    f"Unsupported binary operator: {type(node.op).__name__}"
                )
            return op_func(left, right)

        elif isinstance(node, ast.UnaryOp):
            operand = evaluate_node(node.operand)
            op_func = allowed_operators.get(type(node.op))
            if not op_func:
                raise TypeError(f"Unsupported unary operator: {type(node.op).__name__}")
            return op_func(operand)

        else:
            raise TypeError(f"Unsupported AST node: {type(node).__name__}")

    try:
        tree = ast.parse(clean_expr, mode="eval")
        result = evaluate_node(tree.body)

        if isinstance(result, float):
            result = round(result, 4)

        return {"result": str(result), "expression": expression}
    except Exception as e:
        return {"error": f"Arithmetic parsing failure: {str(e)}"}


def set_price_alert(
    ticker: str,
    target_price: float,
    condition: str,
    user_id: str = "default_user",
    tenant_id: str = "public_b2c",
) -> dict:
    """
    Provisions a background price monitoring alert for the user.
    Invoke this tool explicitly when the user requests to be notified about an asset reaching a specific price.

    Args:
        ticker (str): The target market symbol (e.g., 'AAPL').
        target_price (float): The numeric price threshold in USD.
        condition (str): Must be strictly 'above' (bullish trigger) or 'below' (bearish trigger).
        user_id (str): The unique identifier of the requesting user.
        tenant_id (str): The multi-tenant identifier.

    Returns:
        dict: Status message confirming the successful persistence of the alert.
    """
    db = SessionLocal()
    try:
        new_alert = PriceAlert(
            tenant_id=tenant_id,
            user_id=user_id,
            ticker=ticker.upper(),
            target_price=float(target_price),
            condition=condition.lower(),
        )
        db.add(new_alert)
        db.commit()
        return {
            "message": f"Alert persisted. System will monitor {ticker.upper()} for condition: {condition} {target_price} USD."
        }
    except Exception as e:
        db.rollback()
        return {"error": f"Database failure during alert creation: {str(e)}"}
    finally:
        db.close()


def get_user_alerts(user_id: str, tenant_id: str) -> dict:
    """
    Fetches all active price alerts for the current user.
    Use this to see what alerts the user has before modifying or deleting them.
    args:
        user_id: the ID of the current user
        tenant_id: the multi-tenant identifier

    Returns:
        dict: A list of active alerts with their details or a message if none exist.
    """
    db = SessionLocal()
    try:
        alerts = (
            db.query(PriceAlert)
            .filter(
                PriceAlert.user_id == user_id,
                PriceAlert.status == "active",
                PriceAlert.tenant_id == tenant_id,
            )
            .all()
        )

        if not alerts:
            return {"message": "No active alerts found."}

        return {
            "alerts": [
                {
                    "id": a.id,
                    "ticker": a.ticker,
                    "target_price": a.target_price,
                    "condition": a.condition,
                }
                for a in alerts
            ]
        }
    except Exception as e:
        return {"error": f"Database error: {str(e)}"}
    finally:
        db.close()


def update_price_alert(
    alert_id: int, new_target_price: float, user_id: str, tenant_id: str
) -> dict:
    """
    Updates the target price of an existing alert.
    args:
        alert_id: the ID of the alert to modify
        new_target_price: the new numeric price target
        user_id: the ID of the current user
        tenant_id: the multi-tenant identifier

    Returns:
        dict: Status message confirming the successful update or an error if the alert doesn't exist.
    """
    db = SessionLocal()
    try:
        alert = (
            db.query(PriceAlert)
            .filter(
                PriceAlert.id == alert_id,
                PriceAlert.user_id == user_id,
                PriceAlert.tenant_id == tenant_id,
                PriceAlert.status == "active",
            )
            .first()
        )

        if not alert:
            return {"error": f"Alert {alert_id} not found or doesn't belong to you."}

        alert.target_price = float(new_target_price)
        db.commit()
        return {
            "message": f"Alert {alert_id} updated. New target price: {new_target_price}"
        }
    except Exception as e:
        db.rollback()
        return {"error": f"Database error: {str(e)}"}
    finally:
        db.close()


def delete_price_alert(alert_id: int, user_id: str, tenant_id: str) -> dict:
    """
    Cancels/deletes an active price alert.
    args:
        alert_id: the ID of the alert to delete
        user_id: the ID of the current user
        tenant_id: the multi-tenant identifier

    Returns:
        dict: Status message confirming the successful cancellation or an error if the alert doesn't exist.
    """
    db = SessionLocal()
    try:
        alert = (
            db.query(PriceAlert)
            .filter(
                PriceAlert.id == alert_id,
                PriceAlert.user_id == user_id,
                PriceAlert.tenant_id == tenant_id,
            )
            .first()
        )

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


def get_portfolio_status(user_id: str, tenant_id: str) -> dict:
    """
    Fetches the user's current paper trading portfolio, including cash balance and active stock positions.
    Use this whenever the user asks about their balance, what they own, or how their investments are doing.
    args:
        user_id: the ID of the current user
        tenant_id: the multi-tenant identifier

    returns:
        dict: A summary of the user's cash balance and a list of active positions with their details
    """
    db = SessionLocal()
    try:
        account = (
            db.query(PortfolioAccount)
            .filter(
                PortfolioAccount.user_id == user_id,
                PortfolioAccount.tenant_id == tenant_id,
            )
            .first()
        )

        if not account:
            account = PortfolioAccount(
                user_id=user_id, tenant_id=tenant_id, cash_balance=100000.0
            )
            db.add(account)
            db.commit()

        positions = (
            db.query(PortfolioPosition)
            .filter(
                PortfolioPosition.user_id == user_id,
                PortfolioPosition.tenant_id == tenant_id,
                PortfolioPosition.quantity > 0,
            )
            .all()
        )

        pos_list = [
            {
                "ticker": p.ticker,
                "quantity": p.quantity,
                "average_buy_price": p.average_buy_price,
            }
            for p in positions
        ]

        return {"cash_balance_usd": account.cash_balance, "active_positions": pos_list}
    except Exception as e:
        return {"error": f"Database error: {str(e)}"}
    finally:
        db.close()


def execute_paper_trade(
    ticker: str, action: str, quantity: float, user_id: str, tenant_id: str
) -> dict:
    """
    Executes a simulated transaction (Buy/Sell) within the user's paper trading portfolio.
    Invoke this tool ONLY when the user explicitly issues a trade execution command.

    Args:
        ticker (str): The target market symbol (e.g., 'TSLA').
        action (str): The execution directive, strictly 'BUY' or 'SELL'.
        quantity (float): The exact numeric amount of shares/coins to transact.
        user_id (str): The unique identifier of the executing user.
        tenant_id (str): The multi-tenant identifier.

    Returns:
        dict: A comprehensive trade receipt detailing execution price, total value, and remaining balance.
    """
    action = action.upper()
    if action not in ["BUY", "SELL"]:
        return {"error": "Invalid directive. Action must be exactly 'BUY' or 'SELL'."}
    if float(quantity) <= 0:
        return {"error": "Invalid volume. Quantity must be strictly greater than 0."}

    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        current_price = info.get("currentPrice") or info.get("regularMarketPrice")
        if not current_price:
            return {
                "error": f"Market data unavailable for {ticker}. The exchange might be closed."
            }
    except Exception as e:
        return {"error": f"Live telemetry failure: {str(e)}"}

    total_amount = float(quantity) * float(current_price)
    db = SessionLocal()

    try:
        account = (
            db.query(PortfolioAccount)
            .filter(
                PortfolioAccount.user_id == user_id,
                PortfolioAccount.tenant_id == tenant_id,
            )
            .first()
        )

        if not account:
            account = PortfolioAccount(
                user_id=user_id, tenant_id=tenant_id, cash_balance=100000.0
            )
            db.add(account)

        position = (
            db.query(PortfolioPosition)
            .filter(
                PortfolioPosition.user_id == user_id,
                PortfolioPosition.tenant_id == tenant_id,
                PortfolioPosition.ticker == ticker.upper(),
            )
            .first()
        )

        if action == "BUY":
            if account.cash_balance < total_amount:
                return {
                    "error": f"Insufficient liquidity. Required: {total_amount:.2f} USD, Available: {account.cash_balance:.2f} USD."
                }

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
                    tenant_id=tenant_id,
                    ticker=ticker.upper(),
                    quantity=float(quantity),
                    average_buy_price=current_price,
                )
                db.add(new_pos)

        elif action == "SELL":
            if not position or position.quantity < float(quantity):
                current_qty = position.quantity if position else 0
                return {
                    "error": f"Insufficient position size. Attempted to sell {quantity}, but holdings are {current_qty}."
                }

            account.cash_balance += total_amount
            position.quantity -= float(quantity)

            if position.quantity == 0:
                db.delete(position)

        transaction = PortfolioTransaction(
            user_id=user_id,
            tenant_id=tenant_id,
            ticker=ticker.upper(),
            transaction_type=action,
            quantity=float(quantity),
            price_per_unit=current_price,
            total_amount=total_amount,
        )
        db.add(transaction)
        db.commit()

        return {
            "status": "success",
            "message": f"Order Executed: {action} {quantity} units of {ticker} at {current_price:.2f} USD.",
            "total_transaction_value": total_amount,
            "remaining_cash_balance": account.cash_balance,
        }

    except Exception as e:
        db.rollback()
        return {"error": f"Database transaction aborted: {str(e)}"}
    finally:
        db.close()
