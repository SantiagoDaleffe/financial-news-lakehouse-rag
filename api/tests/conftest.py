import pytest
from fastapi.testclient import TestClient
from pathlib import Path
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from app.main import app
from app.routers.alerts import get_db
from app.security import get_current_user_and_tenant
from app.models import Base, Tenant, User, PriceAlert, Conversation, Message

SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}, poolclass=StaticPool
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    """
    Override the database dependency for testing purposes.

    Provides an in-memory SQLite database session for test isolation.

    Yields:
        Session: A SQLAlchemy database session connected to the test database.
    """
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


def override_get_current_user_and_tenant():
    """Override the authentication dependency for testing purposes."""
    return {"user_id": "test_user_uuid_123", "tenant_id": "test_tenant_alpha"}


app.dependency_overrides[get_db] = override_get_db
app.dependency_overrides[get_current_user_and_tenant] = (
    override_get_current_user_and_tenant
)


@pytest.fixture(scope="function", autouse=True)
def setup_database():
    """Se ejecuta ANTES de cada test individual"""
    Base.metadata.create_all(bind=engine)
    
    # Inyectamos el tenant obligatorio para que no salten errores de clave foránea
    db = TestingSessionLocal()
    db.add(Tenant(id="test_tenant_alpha", name="Fondo de Pruebas"))
    db.commit()
    db.close()
    
    yield # Acá el test hace su trabajo
    
    # Se ejecuta DESPUÉS de cada test para limpiar la RAM
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="module")
def client():
    """Fixture to create a test client for the FastAPI app."""
    with TestClient(app) as c:
        yield c
