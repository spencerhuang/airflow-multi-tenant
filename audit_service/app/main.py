"""Audit Service — FastAPI application.

Consumes audit events from Kafka and writes them to per-customer MySQL schemas.
Provides a query API for retrieving audit events.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from audit_service.app.core.config import settings
from audit_service.app.core.database import get_engine
from audit_service.app.services.schema_manager import AuditSchemaManager
from audit_service.app.services.audit_consumer import AuditConsumerService
from audit_service.app.services.audit_query import AuditQueryService
from audit_service.app.api import audit as audit_api

logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL, logging.INFO))
logger = logging.getLogger(__name__)

_consumer_service = None
_schema_manager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle: start/stop audit consumer."""
    global _consumer_service, _schema_manager

    logger.info("Starting Audit Service...")
    try:
        engine = get_engine()
        _schema_manager = AuditSchemaManager(engine)
        _schema_manager.ensure_template()

        query_service = AuditQueryService(engine, _schema_manager)
        audit_api.set_services(query_service, _schema_manager)

        _consumer_service = AuditConsumerService(engine, _schema_manager, settings)
        _consumer_service.start()
        logger.info("Audit consumer started successfully")
    except Exception as e:
        logger.error(f"Failed to initialise audit service: {e}", exc_info=True)

    yield

    logger.info("Shutting down Audit Service...")
    if _consumer_service:
        try:
            _consumer_service.stop()
            logger.info("Audit consumer stopped successfully")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}", exc_info=True)


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    lifespan=lifespan,
)

app.include_router(audit_api.router)


@app.get("/health")
def health():
    """Basic liveness check."""
    return {"status": "ok"}


@app.get("/health/ready")
def health_ready():
    """Readiness check — consumer must be connected."""
    if _consumer_service and _consumer_service.is_connected:
        return {"status": "ready"}
    return {"status": "not_ready"}, 503


@app.get("/health/detailed")
def health_detailed():
    """Detailed health with consumer stats."""
    consumer_stats = {}
    if _consumer_service:
        consumer_stats = {
            "started_at": (
                _consumer_service.started_at.isoformat()
                if _consumer_service.started_at
                else None
            ),
            "is_connected": _consumer_service.is_connected,
            "messages_processed": _consumer_service.messages_processed,
            "messages_failed": _consumer_service.messages_failed,
            "last_error": _consumer_service.last_error,
        }

    schemas = []
    if _schema_manager:
        try:
            schemas = _schema_manager.list_customer_schemas()
        except Exception:
            pass

    return {
        "status": "ok",
        "consumer": consumer_stats,
        "customer_schemas": schemas,
    }
