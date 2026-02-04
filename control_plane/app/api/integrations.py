"""Integration management API endpoints."""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from control_plane.app.core.database import get_db
from control_plane.app.services.integration_service import IntegrationService
from control_plane.app.schemas.integration import (
    IntegrationCreate,
    IntegrationUpdate,
    IntegrationResponse,
    TriggerIntegrationRequest,
    TriggerIntegrationResponse,
)

router = APIRouter()


@router.post("/", response_model=IntegrationResponse, status_code=201)
def create_integration(
    integration_data: IntegrationCreate,
    db: Session = Depends(get_db),
):
    """
    Create a new integration.

    Args:
        integration_data: Integration creation data
        db: Database session

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
    integration = service.create_integration(integration_data)
    return integration


@router.get("/", response_model=List[IntegrationResponse])
def list_integrations(
    workspace_id: Optional[str] = Query(None, description="Filter by workspace ID"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    db: Session = Depends(get_db),
):
    """
    List integrations with optional filtering.

    Args:
        workspace_id: Optional workspace ID filter
        skip: Number of records to skip (for pagination)
        limit: Maximum number of records to return
        db: Database session

    Returns:
        List of integrations
    """
    service = IntegrationService(db)
    integrations = service.list_integrations(workspace_id, skip, limit)
    return integrations


@router.get("/{integration_id}", response_model=IntegrationResponse)
def get_integration(
    integration_id: int,
    db: Session = Depends(get_db),
):
    """
    Get integration by ID.

    Args:
        integration_id: Integration identifier
        db: Database session

    Returns:
        Integration details

    Raises:
        HTTPException: If integration not found
    """
    service = IntegrationService(db)
    integration = service.get_integration(integration_id)

    if not integration:
        raise HTTPException(status_code=404, detail=f"Integration {integration_id} not found")

    return integration


@router.put("/{integration_id}", response_model=IntegrationResponse)
def update_integration(
    integration_id: int,
    update_data: IntegrationUpdate,
    db: Session = Depends(get_db),
):
    """
    Update an existing integration.

    Args:
        integration_id: Integration identifier
        update_data: Update data
        db: Database session

    Returns:
        Updated integration

    Raises:
        HTTPException: If integration not found
    """
    service = IntegrationService(db)
    integration = service.update_integration(integration_id, update_data)

    if not integration:
        raise HTTPException(status_code=404, detail=f"Integration {integration_id} not found")

    return integration


@router.delete("/{integration_id}", status_code=204)
def delete_integration(
    integration_id: int,
    db: Session = Depends(get_db),
):
    """
    Delete an integration.

    Args:
        integration_id: Integration identifier
        db: Database session

    Raises:
        HTTPException: If integration not found
    """
    service = IntegrationService(db)
    deleted = service.delete_integration(integration_id)

    if not deleted:
        raise HTTPException(status_code=404, detail=f"Integration {integration_id} not found")


@router.post("/trigger", response_model=TriggerIntegrationResponse)
def trigger_integration(
    request: TriggerIntegrationRequest,
    db: Session = Depends(get_db),
):
    """
    Manually trigger an integration to run immediately.

    This endpoint triggers the on-demand DAG for the integration.

    Args:
        request: Trigger request containing integration_id and optional config
        db: Database session

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
        result = service.trigger_dag_run(request.integration_id, request.execution_config)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger integration: {str(e)}")
