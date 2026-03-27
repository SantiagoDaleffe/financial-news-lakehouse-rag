from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from .models import Base
from .routers import ingestion, agent, system, alerts, research, chats
from .security import router as auth_router 

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@airflow-postgres:5432/airflow")
engine = create_engine(DATABASE_URL)
Base.metadata.create_all(bind=engine)

app = FastAPI(title='agent-api')

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(system.router)
app.include_router(ingestion.router, prefix="/api/v1")
app.include_router(agent.router, prefix="/api/v1")
app.include_router(alerts.router, prefix="/api/v1/alerts")
app.include_router(research.router, prefix="/api/v1/research")
app.include_router(chats.router, prefix="/api/v1/chats")
app.include_router(auth_router, prefix="/api/v1/auth")