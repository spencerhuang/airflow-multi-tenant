"""DLQ management API endpoints for viewing, retrying, and resolving dead letter messages."""

import json
import logging
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from kafka_consumer.app.services import dlq_repository
from kafka_consumer.app.services.kafka_consumer_service import get_kafka_consumer_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dlq", tags=["dlq"])


# -- Request/Response models --------------------------------------------------


class ResolveRequest(BaseModel):
    resolution_notes: Optional[str] = None


class BulkResolveRequest(BaseModel):
    dlq_ids: List[int]
    resolution_notes: Optional[str] = None


class BulkRetryRequest(BaseModel):
    dlq_ids: List[int]


# -- Endpoints ----------------------------------------------------------------


@router.get("/stats")
def dlq_stats():
    """Get DLQ entry counts grouped by status."""
    stats = dlq_repository.get_dlq_stats()
    return {
        "counts_by_status": stats,
        "total_pending": stats.get("pending", 0),
    }


@router.get("")
def list_dlq(
    status: Optional[str] = Query(None, description="Filter by status: pending|retrying|resolved|expired"),
    integration_id: Optional[int] = Query(None, description="Filter by integration ID"),
    created_after: Optional[datetime] = Query(None, description="Filter entries created after (ISO 8601)"),
    created_before: Optional[datetime] = Query(None, description="Filter entries created before (ISO 8601)"),
    limit: int = Query(50, ge=1, le=200, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """List DLQ entries with optional filtering and pagination."""
    if status and status not in ("pending", "retrying", "resolved", "expired"):
        raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

    entries = dlq_repository.list_dlq_messages(
        status=status,
        integration_id=integration_id,
        created_after=created_after,
        created_before=created_before,
        limit=limit,
        offset=offset,
    )
    return {"entries": entries, "count": len(entries), "limit": limit, "offset": offset}


@router.post("/bulk/resolve")
def bulk_resolve(body: BulkResolveRequest):
    """Bulk mark DLQ entries as resolved."""
    if not body.dlq_ids:
        raise HTTPException(status_code=400, detail="dlq_ids must not be empty")

    count = dlq_repository.bulk_update_status(
        dlq_ids=body.dlq_ids,
        status="resolved",
        resolution_notes=body.resolution_notes,
    )
    return {"updated": count, "status": "resolved"}


@router.post("/bulk/retry")
def bulk_retry(body: BulkRetryRequest):
    """Bulk retry DLQ entries. Processes each sequentially."""
    if not body.dlq_ids:
        raise HTTPException(status_code=400, detail="dlq_ids must not be empty")

    results = []
    for dlq_id in body.dlq_ids:
        try:
            entry = dlq_repository.get_dlq_message(dlq_id)
            if entry is None:
                results.append({"dlq_id": dlq_id, "status": "not_found"})
                continue
            if entry["status"] == "resolved":
                results.append({"dlq_id": dlq_id, "status": "already_resolved"})
                continue

            dlq_repository.update_dlq_status(dlq_id, status="retrying")

            service = get_kafka_consumer_service()
            if not service:
                dlq_repository.update_dlq_status(dlq_id, status="pending")
                results.append({"dlq_id": dlq_id, "status": "service_unavailable"})
                continue

            original_message = entry["original_message"]
            if isinstance(original_message, str):
                original_message = json.loads(original_message)

            service._process_message(original_message)
            dlq_repository.update_dlq_status(
                dlq_id,
                status="resolved",
                resolution_notes="Automatically resolved via bulk retry",
            )
            results.append({"dlq_id": dlq_id, "status": "resolved"})

        except Exception as e:
            logger.error(f"Bulk retry failed for DLQ entry {dlq_id}: {e}", exc_info=True)
            dlq_repository.update_dlq_status(
                dlq_id,
                status="pending",
                error_type=type(e).__name__,
                error_message=str(e),
                increment_retry=True,
            )
            results.append({"dlq_id": dlq_id, "status": "failed", "error": str(e)})

    return {"results": results}


@router.get("/{dlq_id}")
def get_dlq(dlq_id: int):
    """Get a single DLQ entry by ID."""
    entry = dlq_repository.get_dlq_message(dlq_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"DLQ entry {dlq_id} not found")
    return entry


@router.post("/{dlq_id}/retry")
def retry_dlq(dlq_id: int):
    """Re-process a DLQ message.

    Sets status to 'retrying', attempts to process the original message,
    then updates to 'resolved' on success or back to 'pending' on failure.
    """
    entry = dlq_repository.get_dlq_message(dlq_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"DLQ entry {dlq_id} not found")

    if entry["status"] == "resolved":
        raise HTTPException(status_code=400, detail="Cannot retry a resolved entry")

    # Mark as retrying
    dlq_repository.update_dlq_status(dlq_id, status="retrying")

    # Get the consumer service to re-process
    service = get_kafka_consumer_service()
    if not service:
        dlq_repository.update_dlq_status(dlq_id, status="pending")
        raise HTTPException(status_code=503, detail="Kafka consumer service not available")

    # Deserialize and re-process
    original_message = entry["original_message"]
    if isinstance(original_message, str):
        try:
            original_message = json.loads(original_message)
        except json.JSONDecodeError:
            dlq_repository.update_dlq_status(dlq_id, status="pending")
            raise HTTPException(status_code=400, detail="Cannot parse original_message as JSON")

    try:
        service._process_message(original_message)
        dlq_repository.update_dlq_status(
            dlq_id,
            status="resolved",
            resolution_notes="Automatically resolved via retry",
        )
        return {"dlq_id": dlq_id, "status": "resolved", "message": "Message re-processed successfully"}

    except Exception as e:
        logger.error(f"Retry failed for DLQ entry {dlq_id}: {e}", exc_info=True)
        dlq_repository.update_dlq_status(
            dlq_id,
            status="pending",
            error_type=type(e).__name__,
            error_message=str(e),
            increment_retry=True,
        )
        return {"dlq_id": dlq_id, "status": "pending", "message": f"Retry failed: {e}"}


@router.put("/{dlq_id}/resolve")
def resolve_dlq(dlq_id: int, body: ResolveRequest):
    """Mark a DLQ entry as manually resolved."""
    entry = dlq_repository.get_dlq_message(dlq_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"DLQ entry {dlq_id} not found")

    updated = dlq_repository.update_dlq_status(
        dlq_id,
        status="resolved",
        resolution_notes=body.resolution_notes,
    )
    if not updated:
        raise HTTPException(status_code=500, detail="Failed to update DLQ entry")

    return {"dlq_id": dlq_id, "status": "resolved"}
