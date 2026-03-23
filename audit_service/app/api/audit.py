"""Audit API endpoints."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from audit_service.app.schemas.audit import (
    AuditEventResponse,
    AuditQueryResponse,
    ProvisionRequest,
    ProvisionResponse,
)

router = APIRouter(prefix="/api/v1/audit", tags=["audit"])

# These are set by main.py at startup
_query_service = None
_schema_manager = None


def set_services(query_service, schema_manager):
    """Called by main.py to inject service instances."""
    global _query_service, _schema_manager
    _query_service = query_service
    _schema_manager = schema_manager


@router.get("", response_model=AuditQueryResponse)
def query_audit_events(
    customer_guid: str = Query(..., description="Customer GUID (required)"),
    event_type: Optional[str] = Query(None),
    resource_type: Optional[str] = Query(None),
    resource_id: Optional[str] = Query(None),
    actor_id: Optional[str] = Query(None),
    action: Optional[str] = Query(None),
    outcome: Optional[str] = Query(None),
    trace_id: Optional[str] = Query(None),
    from_ts: Optional[datetime] = Query(None),
    to_ts: Optional[datetime] = Query(None),
    cursor: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
):
    """Query audit events for a customer."""
    if not _query_service:
        raise HTTPException(status_code=503, detail="Query service not available")

    events, next_cursor, has_more = _query_service.query_events(
        customer_guid=customer_guid,
        event_type=event_type,
        resource_type=resource_type,
        resource_id=resource_id,
        actor_id=actor_id,
        action=action,
        outcome=outcome,
        trace_id=trace_id,
        from_ts=from_ts,
        to_ts=to_ts,
        cursor=cursor,
        limit=limit,
    )
    return AuditQueryResponse(
        events=[AuditEventResponse(**e) for e in events],
        next_cursor=next_cursor,
        has_more=has_more,
    )


@router.post("/schemas/provision", response_model=ProvisionResponse)
def provision_schema(request: ProvisionRequest):
    """Provision a new audit schema for a customer."""
    if not _schema_manager:
        raise HTTPException(status_code=503, detail="Schema manager not available")

    schema_name = _schema_manager.provision_customer(request.customer_guid)
    return ProvisionResponse(
        customer_guid=request.customer_guid,
        schema_name=schema_name,
    )


@router.delete("/schemas/{customer_guid}")
def deprovision_schema(customer_guid: str):
    """Deprovision (drop) a customer's audit schema (GDPR erasure)."""
    if not _schema_manager:
        raise HTTPException(status_code=503, detail="Schema manager not available")

    _schema_manager.deprovision_customer(customer_guid)
    return {"customer_guid": customer_guid, "status": "deprovisioned"}
