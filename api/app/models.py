from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

Base = declarative_base()


class PriceAlert(Base):
    __tablename__ = "price_alerts"

    id = Column(Integer, primary_key=True, index=True)
    tenant_id = Column(String, index=True, default="public_b2c")
    user_id = Column(String, index=True)
    ticker = Column(String, index=True)
    target_price = Column(Float)
    condition = Column(String)
    status = Column(String, default="active")
    created_at = Column(DateTime, default=datetime.utcnow)


class Conversation(Base):
    __tablename__ = "conversation"

    id = Column(Integer, primary_key=True, index=True)
    # tenant_id = Column(String, index=True, default="public_b2c")
    user_id = Column(String, index=True)
    title = Column(String, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    messages = relationship("Message", back_populates="conversation")


class Message(Base):
    __tablename__ = "message"

    id = Column(Integer, primary_key=True, index=True)
    conversation_id = Column(Integer, ForeignKey("conversation.id"), index=True)
    user_id = Column(String, index=True)
    role = Column(String)
    content = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    conversation = relationship("Conversation", back_populates="messages")


class ChatRequest(BaseModel):
    message: str
    conversation_id: Optional[int] = None
