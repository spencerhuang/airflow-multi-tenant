"""Health check endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from control_plane.app.core.database import get_db
from control_plane.app.core.config import settings

router = APIRouter()


@router.get("/health")
async def health_check():
    """
    Health check endpoint.

    Returns:
        Status and version information
    """
    return {
        "status": "healthy",
        "service": settings.PROJECT_NAME,
        "version": settings.VERSION,
    }


@router.get("/health/db")
async def database_health_check(db: AsyncSession = Depends(get_db)):
    """
    Database health check endpoint.

    Args:
        db: Async database session

    Returns:
        Database connection status
    """
    try:
        # Execute a simple query to check database connectivity
        await db.execute(text("SELECT 1"))
        return {
            "status": "healthy",
            "database": "connected",
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
        }
