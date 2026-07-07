from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import List
from ..schemas import AlertCreate, AlertResponse, MessageResponse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
import os
from ..models import PriceAlert, User
from ..security import get_current_user_and_tenant

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """
    Dependency generator for isolated database sessions per request.
    Ensures safe resource teardown after HTTP response.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

router = APIRouter(tags=["alerts"])

@router.get("/", response_model=List[AlertResponse])
def get_alerts(auth_data: dict = Depends(get_current_user_and_tenant), db: Session = Depends(get_db)):
    """
    Retrieves all active price alerts for the authenticated user.
    Used to populate the frontend dashboard.
    """
    user_id = auth_data["user_id"]
    tenant_id = auth_data["tenant_id"]
    
    alerts = db.query(PriceAlert).filter(
        PriceAlert.user_id == user_id,
        PriceAlert.tenant_id == tenant_id,
        PriceAlert.status == "active"
    ).all()
    
    return alerts


@router.post("/", response_model=MessageResponse)
def create_alert(alert: AlertCreate, auth_data: dict = Depends(get_current_user_and_tenant), db: Session = Depends(get_db)):
    """
    Manually provisions a new price alert, bypassing the LLM agent.
    Validates input via Pydantic to ensure database integrity.
    """
    user_id = auth_data["user_id"]
    tenant_id = auth_data["tenant_id"]
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            user = User(id=user_id, tenant_id=tenant_id, email=f"{user_id}@auth.local", credits=100.0)
            db.add(user)
            db.commit()
            
        new_alert = PriceAlert(
            tenant_id=tenant_id, 
            user_id=user_id, 
            ticker=alert.ticker.upper(), 
            target_price=alert.target_price, 
            condition=alert.condition
        )
        db.add(new_alert)
        db.commit()
        return {"message": "Alert created successfully.", "ticker": alert.ticker.upper()}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database transaction failed: {str(e)}")


@router.delete("/{alert_id}")
def delete_alert(alert_id: int, auth_data: dict = Depends(get_current_user_and_tenant), db: Session = Depends(get_db)):
    """
    Soft-deletes an active alert by updating its lifecycle status to 'cancelled'.
    """
    user_id = auth_data["user_id"]
    tenant_id = auth_data["tenant_id"]
    
    alert = db.query(PriceAlert).filter(
        PriceAlert.id == alert_id,
        PriceAlert.user_id == user_id,
        PriceAlert.tenant_id == tenant_id
    ).first()
    
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found or access denied.")
        
    alert.status = "cancelled"
    db.commit()
    
    return {"message": f"Alert {alert_id} cancelled successfully"}