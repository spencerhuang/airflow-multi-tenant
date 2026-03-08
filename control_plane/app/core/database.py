"""Database configuration and session management."""

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from typing import AsyncGenerator

from shared_models.tables import metadata as shared_metadata
from control_plane.app.core.config import settings

# DATABASE_URL is already configured with mysql+aiomysql:// in config.py
async_database_url = settings.DATABASE_URL

# Create async database engine
engine = create_async_engine(
    async_database_url,
    echo=False,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

# Create async session factory
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# Create base class for models using shared metadata so Alembic sees all tables
Base = declarative_base(metadata=shared_metadata)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency function that yields async database sessions.

    Yields:
        AsyncSession: Async database session

    Example:
        @app.get("/items")
        async def get_items(db: AsyncSession = Depends(get_db)):
            result = await db.execute(select(Item))
            return result.scalars().all()
    """
    async with AsyncSessionLocal() as session:
        yield session
