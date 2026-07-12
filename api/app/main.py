from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import os
from dotenv import load_dotenv
from .models import Base, Tenant
from .routers import ingestion, agent, system, alerts, research, chats, auth
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from fastapi.encoders import jsonable_encoder
from .limiter import limiter

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Base.metadata.create_all(bind=engine)


def seed_tenants():
    """Seed default tenants into the database if they do not already exist."""
    with Session(engine) as db:
        try:
            if not db.query(Tenant).filter(Tenant.id == "public_b2c").first():
                db.add(Tenant(id="public_b2c", name="Public B2C Tenant"))
            if not db.query(Tenant).filter(Tenant.id == "admin").first():
                db.add(Tenant(id="admin", name="Admin_tenant"))
            db.commit()
            print("Tenants initialized.", flush=True)
        except Exception as e:
            db.rollback()
            print(f"Error initializing tenants: {e}", flush=True)


seed_tenants()

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


@app.middleware("http")
async def limit_payload_size(request: Request, call_next):
    """Limit incoming request payload size to protect the application from oversized uploads.

    Args:
        request (Request): The incoming HTTP request.
        call_next (Callable): The next middleware or route handler.

    Returns:
        Response: The HTTP response from the next handler or a 413 error response.
    """
    MAX_PAYLOAD_SIZE = 2 * 1024 * 1024

    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_PAYLOAD_SIZE:
        return JSONResponse(
            status_code=413,
            content={
                "error": True,
                "message": "Payload too large. Maximum size is 2MB.",
            },
        )

    return await call_next(request)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle HTTP exceptions and format them into a JSON response."""
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": True, "message": exc.detail, "code": exc.status_code},
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    """Return a structured JSON response for request validation failures."""
    return JSONResponse(
        status_code=422,
        content={
            "error": True,
            "message": "Data validation error. Please check the input data and try again.",
            "details": jsonable_encoder(exc.errors()),
        },
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions and return a generic internal server error response."""
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
app.include_router(auth.router, prefix="/api/v1/auth")
app.include_router(ingestion.router, prefix="/api/v1")
app.include_router(agent.router, prefix="/api/v1")
app.include_router(alerts.router, prefix="/api/v1/alerts")
app.include_router(research.router, prefix="/api/v1/research")
app.include_router(chats.router, prefix="/api/v1/chats")
