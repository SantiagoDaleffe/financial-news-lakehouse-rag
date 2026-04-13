from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from .models import Base
from .routers import ingestion, agent, system, alerts, research, chats
from .security import router as auth_router
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://airflow:airflow@airflow-postgres:5432/airflow"
)
engine = create_engine(DATABASE_URL)
Base.metadata.create_all(bind=engine)

limiter = Limiter(key_func=get_remote_address)

app = FastAPI(title="agent-api")

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": True, "message": exc.detail, "code": exc.status_code},
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={
            "error": True,
            "message": "Data validation error. Please check the input data and try again.",
            "details": exc.errors(),
            "code": 422,
        },
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    print(f"Critical error: {str(exc)}", flush=True)

    return JSONResponse(
        status_code=500,
        content={
            "error": True,
            "message": "Internal server error. Please try again later.",
            "code": 500,
        },
    )


app.include_router(system.router)
app.include_router(ingestion.router, prefix="/api/v1")
app.include_router(agent.router, prefix="/api/v1")
app.include_router(alerts.router, prefix="/api/v1/alerts")
app.include_router(research.router, prefix="/api/v1/research")
app.include_router(chats.router, prefix="/api/v1/chats")
app.include_router(auth_router, prefix="/api/v1/auth")
