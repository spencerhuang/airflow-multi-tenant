"""Main FastAPI application for the control plane service."""

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from control_plane.app.core.config import settings
from control_plane.app.core.database import engine, Base
from control_plane.app.core.logging import setup_logging
from control_plane.app.core.middleware import ErrorLoggingMiddleware
from control_plane.app.api import api_router

# Setup logging before creating the app
setup_logging(settings.LOG_LEVEL, settings.LOG_FORMAT)

logger = logging.getLogger(__name__)

# Create FastAPI application
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="Multi-tenant Airflow control plane service for managing data integrations",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add error logging middleware
app.add_middleware(ErrorLoggingMiddleware)

# Include API router
app.include_router(api_router, prefix=settings.API_V1_STR)


@app.get("/")
def root():
    """
    Root endpoint.

    Returns:
        Welcome message and API information
    """
    return {
        "message": f"Welcome to {settings.PROJECT_NAME}",
        "version": settings.VERSION,
        "docs": "/docs",
        "health": f"{settings.API_V1_STR}/health",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "control_plane.app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
