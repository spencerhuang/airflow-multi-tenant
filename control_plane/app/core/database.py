"""Database configuration and session management."""

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from typing import AsyncGenerator

from control_plane.app.core.config import settings

# Convert DATABASE_URL to use async driver
# Replace mysql+pymysql:// with mysql+aiomysql:// for async support
async_database_url = settings.DATABASE_URL.replace('mysql+pymysql://', 'mysql+aiomysql://')

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

# Create base class for models
Base = declarative_base()


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
