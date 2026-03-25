"""Health check endpoints for the Kafka CDC consumer service."""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Response

from kafka_consumer.app.core.config import settings
from kafka_consumer.app.services.kafka_consumer_service import get_kafka_consumer_service

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health")
def liveness():
    """Liveness probe — returns 200 if the process is up."""
    return {
        "status": "alive",
        "service": settings.PROJECT_NAME,
        "version": settings.VERSION,
    }


@router.get("/health/ready")
def readiness(response: Response):
    """Readiness probe — returns 200 only if the consumer is connected."""
    service = get_kafka_consumer_service()
    if service and service.running and service.is_connected:
        return {"status": "ready"}

    response.status_code = 503
    return {"status": "not_ready"}


@router.get("/health/detailed")
def detailed(response: Response):
    """Full diagnostic endpoint with consumer metrics."""
    service = get_kafka_consumer_service()

    if not service:
        response.status_code = 503
        return {"status": "unhealthy", "consumer": None}

    now = datetime.now(timezone.utc)
    uptime_seconds = None
    if service.started_at:
        uptime_seconds = (now - service.started_at).total_seconds()

    # Determine status
    if service.running and service.is_connected:
        if service.last_error and service.last_message_time:
            status = "degraded"
        else:
            status = "healthy"
    else:
        status = "unhealthy"

    if status == "unhealthy":
        response.status_code = 503

    # DLQ stats (best-effort, don't fail health check if DB is unavailable)
    dlq_info = {}
    if settings.KAFKA_DLQ_DB_ENABLED:
        try:
            from kafka_consumer.app.services.dlq_repository import get_dlq_stats
            dlq_stats = get_dlq_stats()
            dlq_info = {
                "counts_by_status": dlq_stats,
                "total_pending": dlq_stats.get("pending", 0),
            }
        except Exception as e:
            logger.warning(f"Failed to fetch DLQ stats for health check: {e}")
            dlq_info = {"error": "unable to query DLQ stats"}

    return {
        "status": status,
        "consumer": {
            "running": service.running,
            "connected": service.is_connected,
            "consumer_group": service.group_id,
            "topic": service.topic,
            "last_message_time": service.last_message_time.isoformat() if service.last_message_time else None,
            "messages_processed": service.messages_processed,
            "messages_failed": service.messages_failed,
            "messages_deduplicated": service.messages_deduplicated,
            "last_error": service.last_error,
        },
        "dlq": dlq_info,
        "uptime_seconds": uptime_seconds,
        "version": settings.VERSION,
    }
