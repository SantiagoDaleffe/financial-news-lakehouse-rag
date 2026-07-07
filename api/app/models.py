from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    ForeignKey,
    Date,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

Base = declarative_base()

class Tenant(Base):
    """
    Core architecture for B2B2C. Represents an organization, fund, or the default public platform.
    """
    __tablename__ = "tenants"

    id = Column(String, primary_key=True, index=True) # e.g., 'public_b2c', 'alpha_fund'
    name = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, index=True)
    tenant_id = Column(String, ForeignKey("tenants.id"), index=True, default="public_b2c")
    email = Column(String, unique=True, index=True)
    credits = Column(Float, default=100.0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class PriceAlert(Base):
    __tablename__ = "price_alerts"

    id = Column(Integer, primary_key=True, index=True)
    tenant_id = Column(String, ForeignKey("tenants.id"), index=True, default="public_b2c")
    user_id = Column(String, ForeignKey("users.id"), index=True)
    ticker = Column(String, index=True)
    target_price = Column(Float)
    condition = Column(String)
    status = Column(String, default="active")
    created_at = Column(DateTime, default=datetime.utcnow)


class Conversation(Base):
    __tablename__ = "conversation"

    id = Column(Integer, primary_key=True, index=True)
    tenant_id = Column(String, ForeignKey("tenants.id"), index=True, default="public_b2c")
    user_id = Column(String, ForeignKey("users.id"), index=True)
    title = Column(String, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    messages = relationship("Message", back_populates="conversation")


class Message(Base):
    __tablename__ = "message"

    id = Column(Integer, primary_key=True, index=True)
    tenant_id = Column(String, ForeignKey("tenants.id"), index=True, default="public_b2c")
    conversation_id = Column(Integer, ForeignKey("conversation.id"), index=True)
    user_id = Column(String, ForeignKey("users.id"), index=True)
    role = Column(String)
    content = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    conversation = relationship("Conversation", back_populates="messages")


class MarketData(Base):
    """Save the daily closing price (EOD) for the universe of tickers. This is GLOBAL data."""
    __tablename__ = "market_data"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, index=True, nullable=False)
    date = Column(DateTime, index=True, nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    adj_close = Column(Float)


class PortfolioAccount(Base):
    """Experimental simulated portfolio isolated by tenant and user"""
    __tablename__ = "portfolio_accounts"

    user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    tenant_id = Column(String, ForeignKey("tenants.id"), index=True, default="public_b2c")
    cash_balance = Column(Float, default=100000.0)


class PortfolioPosition(Base):
    """Experimental simulated portfolio position for PnL and risk modeling, and backtesting"""
    __tablename__ = "portfolio_positions"

    id = Column(Integer, primary_key=True, index=True)
    tenant_id = Column(String, ForeignKey("tenants.id"), index=True, default="public_b2c")
    user_id = Column(String, ForeignKey("users.id"))
    ticker = Column(String, index=True)
    quantity = Column(Float, default=0.0)
    average_buy_price = Column(Float, default=0.0)


class PortfolioTransaction(Base):
    """Audit log of all transactions in the simulated portfolio"""
    __tablename__ = "portfolio_transactions"

    id = Column(Integer, primary_key=True, index=True)
    tenant_id = Column(String, ForeignKey("tenants.id"), index=True, default="public_b2c")
    user_id = Column(String, ForeignKey("users.id"))
    ticker = Column(String)
    transaction_type = Column(String)  # buy or sell
    quantity = Column(Float)
    price_per_unit = Column(Float)  # price at which the transaction was executed
    total_amount = Column(Float)  # quantity * price_per_unit
    timestamp = Column(DateTime, default=datetime.utcnow)


class PredictionsHistory(Base):
    """Global Quant Engine tracking. This remains global."""
    __tablename__ = "predictions_history"

    id = Column(Integer, primary_key=True, index=True)
    prediction_date = Column(Date, nullable=False)
    signal_date = Column(Date, nullable=False)
    ticker = Column(String(10), nullable=False)
    quant_decision = Column(String(10), nullable=False)
    quant_probability = Column(Float, nullable=False)
    conviction_zone = Column(String(10), nullable=False)
    top_drivers = Column(String(100), nullable=False)
    pred_close_price = Column(Float, nullable=False)
    llm_verdict = Column(String(10), nullable=False)
    actual_close_price = Column(Float, nullable=True)
    realized_return = Column(Float, nullable=True)
    reconciliation_status = Column(String(20), default="PENDING")

    __table_args__ = (
        UniqueConstraint("prediction_date", "ticker", name="unique_ticker_date"),
    )