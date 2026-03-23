"""Integration management API endpoints."""

import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from control_plane.app.core.database import get_db
from control_plane.app.services.integration_service import IntegrationService
from control_plane.app.schemas.integration import (
    IntegrationCreate,
    IntegrationUpdate,
    IntegrationResponse,
    TriggerIntegrationRequest,
    TriggerIntegrationResponse,
)
from shared_models import workspaces

logger = logging.getLogger(__name__)
router = APIRouter()


async def _resolve_customer_guid(db: AsyncSession, workspace_id: str) -> str:
    """Look up customer_guid from workspace_id. Returns 'unknown' on failure."""
    try:
        result = await db.execute(
            select(workspaces.c.customer_guid).where(
                workspaces.c.workspace_id == workspace_id
            )
        )
        row = result.scalar_one_or_none()
        return row or "unknown"
    except Exception:
        return "unknown"


def _audit_emit(request: Request, **kwargs):
    """Emit an audit event from the request's audit context.  Never raises."""
    try:
        producer = getattr(request.app.state, "audit_producer", None)
        if not producer:
            return
        state = request.state
        producer.emit(
            actor_id=getattr(state, "audit_actor_id", "anonymous"),
            actor_type=getattr(state, "audit_actor_type", "user"),
            actor_ip=getattr(state, "audit_actor_ip", None),
            trace_id=getattr(state, "audit_trace_id", None),
            request_id=getattr(state, "audit_request_id", None),
            **kwargs,
        )
    except Exception:
        logger.debug("Failed to emit audit event", exc_info=True)


@router.post("/", response_model=IntegrationResponse, status_code=201)
async def create_integration(
    integration_data: IntegrationCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new integration.

    Args:
        integration_data: Integration creation data
        request: HTTP request (for audit context)
        db: Async database session

    Returns:
        Created integration

    Example:
        ```json
        {
            "workspace_id": "workspace-123",
            "workflow_id": 1,
            "auth_id": 1,
            "source_access_pt_id": 1,
            "dest_access_pt_id": 2,
            "integration_type": "S3ToMongo",
            "usr_sch_cron": "0 2 * * *",
            "usr_timezone": "America/New_York",
            "schedule_type": "daily",
            "json_data": "{\"bucket\": \"my-bucket\", \"prefix\": \"data/\"}"
        }
        ```
    """
    service = IntegrationService(db)
    integration = await service.create_integration(integration_data)
    cust_guid = await _resolve_customer_guid(db, integration_data.workspace_id)
    _audit_emit(
        request,
        customer_guid=cust_guid,
        event_type="integration.created",
        resource_type="integration",
        resource_id=str(integration.integration_id),
        action="create",
        outcome="success",
        after_state=integration_data.model_dump(mode="json"),
    )
    return integration


@router.get("/", response_model=List[IntegrationResponse])
async def list_integrations(
    workspace_id: Optional[str] = Query(None, description="Filter by workspace ID"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    db: AsyncSession = Depends(get_db),
):
    """
    List integrations with optional filtering.

    Args:
        workspace_id: Optional workspace ID filter
        skip: Number of records to skip (for pagination)
        limit: Maximum number of records to return
        db: Async database session

    Returns:
        List of integrations
    """
    service = IntegrationService(db)
    integrations = await service.list_integrations(workspace_id, skip, limit)
    return integrations


@router.get("/{integration_id}", response_model=IntegrationResponse)
async def get_integration(
    integration_id: int,
    db: AsyncSession = Depends(get_db),
):
    """
    Get integration by ID.

    Args:
        integration_id: Integration identifier
        db: Async database session

    Returns:
        Integration details

    Raises:
        HTTPException: If integration not found
    """
    service = IntegrationService(db)
    integration = await service.get_integration(integration_id)

    if not integration:
        raise HTTPException(status_code=404, detail=f"Integration {integration_id} not found")

    return integration


@router.put("/{integration_id}", response_model=IntegrationResponse)
async def update_integration(
    integration_id: int,
    update_data: IntegrationUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """
    Update an existing integration.

    Args:
        integration_id: Integration identifier
        update_data: Update data
        request: HTTP request (for audit context)
        db: Async database session

    Returns:
        Updated integration

    Raises:
        HTTPException: If integration not found
    """
    service = IntegrationService(db)
    integration = await service.update_integration(integration_id, update_data)

    if not integration:
        raise HTTPException(status_code=404, detail=f"Integration {integration_id} not found")

    cust_guid = await _resolve_customer_guid(db, integration.workspace_id)
    _audit_emit(
        request,
        customer_guid=cust_guid,
        event_type="integration.updated",
        resource_type="integration",
        resource_id=str(integration_id),
        action="update",
        outcome="success",
        after_state=update_data.model_dump(exclude_unset=True, mode="json"),
    )
    return integration


@router.delete("/{integration_id}", status_code=204)
async def delete_integration(
    integration_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """
    Delete an integration.

    Args:
        integration_id: Integration identifier
        request: HTTP request (for audit context)
        db: Async database session

    Raises:
        HTTPException: If integration not found
    """
    service = IntegrationService(db)
    # Resolve customer_guid before deletion
    integration = await service.get_integration(integration_id)
    cust_guid = await _resolve_customer_guid(db, integration.workspace_id) if integration else "unknown"

    deleted = await service.delete_integration(integration_id)

    if not deleted:
        raise HTTPException(status_code=404, detail=f"Integration {integration_id} not found")

    _audit_emit(
        request,
        customer_guid=cust_guid,
        event_type="integration.deleted",
        resource_type="integration",
        resource_id=str(integration_id),
        action="delete",
        outcome="success",
    )


@router.post("/trigger", response_model=TriggerIntegrationResponse)
async def trigger_integration(
    trigger_request: TriggerIntegrationRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """
    Manually trigger an integration to run immediately.

    This endpoint triggers the on-demand DAG for the integration.

    Args:
        trigger_request: Trigger request containing integration_id and optional config
        request: HTTP request (for audit context)
        db: Async database session

    Returns:
        Trigger response with DAG run ID

    Raises:
        HTTPException: If integration not found or trigger fails

    Example:
        ```json
        {
            "integration_id": 1,
            "execution_config": {
                "backfill_date": "2024-01-01"
            }
        }
        ```
    """
    service = IntegrationService(db)

    try:
        # Resolve customer_guid before triggering
        integration = await service.get_integration(trigger_request.integration_id)
        cust_guid = await _resolve_customer_guid(db, integration.workspace_id) if integration else "unknown"

        result = await service.trigger_dag_run(trigger_request.integration_id, trigger_request.execution_config)
        _audit_emit(
            request,
            customer_guid=cust_guid,
            event_type="integration.triggered",
            resource_type="integration",
            resource_id=str(trigger_request.integration_id),
            action="trigger",
            outcome="success",
            metadata={"dag_run_id": result.get("dag_run_id")},
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger integration: {str(e)}")
