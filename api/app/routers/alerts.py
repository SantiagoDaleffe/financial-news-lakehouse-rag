from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
import os
from ..models import PriceAlert
from ..security import get_current_user

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@airflow-postgres:5432/airflow")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """
    dependency to manage database sessions per request
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

router = APIRouter(tags=["alerts"])

@router.get("/")
def get_alerts(user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    """
    fetches all active alerts to populate the frontend dashboard
    """
    alerts = db.query(PriceAlert).filter(
        PriceAlert.user_id == user_id,
        PriceAlert.status == "active"
    ).all()
    
    return alerts

@router.delete("/{alert_id}")
def delete_alert(alert_id: int, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    """
    soft deletes an alert by changing its status to cancelled
    """
    alert = db.query(PriceAlert).filter(
        PriceAlert.id == alert_id,
        PriceAlert.user_id == user_id
    ).first()
    
    if not alert:
        raise HTTPException(status_code=404, detail="alert not found")
        
    alert.status = "cancelled"
    db.commit()
    
    print(f"alert {alert_id} cancelled by user", flush=True)
    return {"message": f"alert {alert_id} cancelled successfully"}