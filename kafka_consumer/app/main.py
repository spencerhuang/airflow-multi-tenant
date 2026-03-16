"""Main FastAPI application for the Kafka CDC consumer service."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from kafka_consumer.app.core.config import settings
from kafka_consumer.app.core.logging import setup_logging
from kafka_consumer.app.api.health import router as health_router
from kafka_consumer.app.api.dlq import router as dlq_router
from kafka_consumer.app.services.kafka_consumer_service import (
    initialize_kafka_consumer,
    shutdown_kafka_consumer,
)

# Setup logging before creating the app
setup_logging(settings.LOG_LEVEL, settings.LOG_FORMAT)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle: start/stop Kafka consumer."""
    logger.info("Starting Kafka CDC Consumer service...")
    try:
        initialize_kafka_consumer()
        logger.info("Kafka consumer initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Kafka consumer: {e}", exc_info=True)

    yield

    logger.info("Shutting down Kafka CDC Consumer service...")
    try:
        shutdown_kafka_consumer()
        logger.info("Kafka consumer shut down successfully")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}", exc_info=True)


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="Standalone Kafka CDC consumer for processing integration events",
    docs_url="/docs",
    lifespan=lifespan,
)

app.include_router(health_router)
app.include_router(dlq_router)


@app.get("/")
def root():
    """Root endpoint."""
    return {
        "message": f"Welcome to {settings.PROJECT_NAME}",
        "version": settings.VERSION,
        "health": "/health",
        "health_detailed": "/health/detailed",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "kafka_consumer.app.main:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
    )
