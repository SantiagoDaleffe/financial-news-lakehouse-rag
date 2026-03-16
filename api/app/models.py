from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base
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