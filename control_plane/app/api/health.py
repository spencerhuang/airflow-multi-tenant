"""Health check endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from control_plane.app.core.database import get_db
from control_plane.app.core.config import settings

router = APIRouter()


@router.get("/health")
def health_check():
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
def database_health_check(db: Session = Depends(get_db)):
    """
    Database health check endpoint.

    Args:
        db: Database session

    Returns:
        Database connection status
    """
    try:
        # Execute a simple query to check database connectivity
        db.execute("SELECT 1")
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
