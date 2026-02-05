"""Shared pytest fixtures for control plane tests."""

import pytest
import pytest_asyncio
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from fastapi.testclient import TestClient

from control_plane.app.core.database import Base, get_db
from control_plane.app.main import app


# Use SQLite file for testing (sync engine for direct fixture access)
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Async engine/session used by FastAPI dependency overrides to match production code
ASYNC_SQLALCHEMY_DATABASE_URL = "sqlite+aiosqlite:///./test.db"
async_engine = create_async_engine(
    ASYNC_SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
)
AsyncTestingSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


@pytest.fixture(scope="function")
def test_db():
    """Create test database and tables."""
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def db_session(test_db):
    """Get test database session."""
    db = TestingSessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@pytest_asyncio.fixture(scope="function")
async def async_db_session(test_db):
    """
    Async database session fixture backed by the same SQLite file.

    Used by async services (e.g., HotspotService) that expect an AsyncSession.
    """
    async with AsyncTestingSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
@pytest.fixture(scope="function")
def client(db_session):
    """Get test client with database override."""

    async def override_get_db():
        """
        Async override for get_db that uses an async SQLite session.

        This matches the production dependency (which yields AsyncSession),
        while still sharing the same underlying SQLite database file as the
        synchronous TestingSessionLocal used in other fixtures.
        """
        async with AsyncTestingSessionLocal() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()
