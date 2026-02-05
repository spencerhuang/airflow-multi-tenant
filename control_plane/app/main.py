"""Main FastAPI application for the control plane service."""

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from control_plane.app.core.config import settings
from control_plane.app.core.database import engine, Base
from control_plane.app.core.logging import setup_logging
from control_plane.app.core.middleware import ErrorLoggingMiddleware
from control_plane.app.api import api_router
from control_plane.app.services.kafka_consumer_service import (
    initialize_kafka_consumer,
    shutdown_kafka_consumer,
)

# Setup logging before creating the app
setup_logging(settings.LOG_LEVEL, settings.LOG_FORMAT)

logger = logging.getLogger(__name__)

# Note: Database tables should be created using Alembic migrations
# For async engines, use: asyncio.run(init_models()) in a separate script
# Example async initialization:
#   async def init_models():
#       async with engine.begin() as conn:
#           await conn.run_sync(Base.metadata.create_all)

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


@app.on_event("startup")
async def startup_event():
    """
    Startup event handler.

    Initializes and starts the Kafka consumer service.
    """
    logger.info("Starting up Control Plane service...")

    try:
        # Initialize Kafka consumer for CDC events
        initialize_kafka_consumer()
        logger.info("Kafka consumer initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Kafka consumer: {e}", exc_info=True)
        # Don't crash the application if Kafka is not available
        # This allows the API to still function


@app.on_event("shutdown")
async def shutdown_event():
    """
    Shutdown event handler.

    Gracefully stops the Kafka consumer service.
    """
    logger.info("Shutting down Control Plane service...")

    try:
        shutdown_kafka_consumer()
        logger.info("Kafka consumer shut down successfully")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}", exc_info=True)


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
