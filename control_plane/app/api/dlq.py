"""DLQ management API endpoints.

Provides REST endpoints for viewing, retrying, and resolving dead letter
messages.  Migrated from the standalone kafka_consumer service to the
control plane so that DLQ management is available without a separate service.

Retry re-triggers the ondemand DAG via the Airflow REST API (instead of
calling the old kafka_consumer's internal _process_message method).
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional, List, Any, Dict

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, select, update, func, and_, desc
from sqlalchemy.engine import Engine

from shared_models.tables import dead_letter_messages

logger = logging.getLogger(__name__)

router = APIRouter()

# Lazy-init sync engine for DLQ operations (pymysql, not aiomysql)
_sync_engine: Optional[Engine] = None


def _get_sync_engine() -> Engine:
    """Get a sync SQLAlchemy engine for DLQ table operations."""
    global _sync_engine
    if _sync_engine is None:
        import os
        # Derive sync URL from env (the async URL uses aiomysql, we need pymysql)
        db_url = os.getenv(
            "DATABASE_URL",
            "mysql+pymysql://control_plane:control_plane@localhost:3306/control_plane",
        )
        # Ensure we're using pymysql (sync driver)
        db_url = db_url.replace("mysql+aiomysql://", "mysql+pymysql://")
        _sync_engine = create_engine(db_url, pool_pre_ping=True, pool_size=3, max_overflow=2)
    return _sync_engine


def _row_to_dict(row) -> Dict[str, Any]:
    """Convert a SQLAlchemy Row to a dict with JSON-parsed original_message."""
    d = dict(row._mapping)
    if isinstance(d.get("original_message"), str):
        try:
            d["original_message"] = json.loads(d["original_message"])
        except json.JSONDecodeError:
            pass
    for key in ("created_at", "updated_at", "resolved_at"):
        if isinstance(d.get(key), datetime):
            d[key] = d[key].isoformat()
    return d


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
    engine = _get_sync_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            select(
                dead_letter_messages.c.status,
                func.count().label("count"),
            ).group_by(dead_letter_messages.c.status)
        ).fetchall()

    stats = {row.status: row.count for row in rows}
    return {"counts_by_status": stats, "total_pending": stats.get("pending", 0)}


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

    query = select(dead_letter_messages)
    conditions = []
    if status is not None:
        conditions.append(dead_letter_messages.c.status == status)
    if integration_id is not None:
        conditions.append(dead_letter_messages.c.integration_id == integration_id)
    if created_after is not None:
        conditions.append(dead_letter_messages.c.created_at >= created_after)
    if created_before is not None:
        conditions.append(dead_letter_messages.c.created_at <= created_before)
    if conditions:
        query = query.where(and_(*conditions))

    query = query.order_by(desc(dead_letter_messages.c.created_at)).limit(limit).offset(offset)

    engine = _get_sync_engine()
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    entries = [_row_to_dict(row) for row in rows]
    return {"entries": entries, "count": len(entries), "limit": limit, "offset": offset}


@router.get("/{dlq_id}")
def get_dlq(dlq_id: int):
    """Get a single DLQ entry by ID."""
    engine = _get_sync_engine()
    with engine.connect() as conn:
        row = conn.execute(
            select(dead_letter_messages).where(dead_letter_messages.c.dlq_id == dlq_id)
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail=f"DLQ entry {dlq_id} not found")
    return _row_to_dict(row)


@router.post("/{dlq_id}/retry")
def retry_dlq(dlq_id: int):
    """Re-process a DLQ message by triggering the ondemand DAG via Airflow API.

    Instead of calling the old kafka consumer's internal method, this
    endpoint builds the DAG conf from the original message and triggers
    the appropriate ondemand DAG directly via the Airflow REST API.
    """
    from shared_utils import (
        build_integration_conf,
        merge_json_data,
        resolve_auth_credentials_sync,
        determine_dag_id,
        trigger_airflow_dag,
        TraceContext,
    )
    from shared_models.tables import integrations as integrations_t

    engine = _get_sync_engine()

    # Fetch entry
    with engine.connect() as conn:
        row = conn.execute(
            select(dead_letter_messages).where(dead_letter_messages.c.dlq_id == dlq_id)
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail=f"DLQ entry {dlq_id} not found")

    entry = _row_to_dict(row)
    if entry["status"] == "resolved":
        raise HTTPException(status_code=400, detail="Cannot retry a resolved entry")

    # Mark as retrying
    _update_status(engine, dlq_id, "retrying")

    # Parse original message
    original_message = entry["original_message"]
    if isinstance(original_message, str):
        try:
            original_message = json.loads(original_message)
        except json.JSONDecodeError:
            _update_status(engine, dlq_id, "pending")
            raise HTTPException(status_code=400, detail="Cannot parse original_message as JSON")

    # Extract integration_id
    integration_id = original_message.get("integration_id")
    if not integration_id and isinstance(original_message.get("data"), dict):
        integration_id = original_message["data"].get("integration_id")

    if not integration_id:
        _update_status(engine, dlq_id, "pending")
        raise HTTPException(status_code=400, detail="Cannot determine integration_id from message")

    try:
        # Query integration
        with engine.connect() as conn:
            int_row = conn.execute(
                select(integrations_t).where(integrations_t.c.integration_id == int(integration_id))
            ).fetchone()

        if not int_row:
            raise ValueError(f"Integration {integration_id} not found")

        # Build conf and trigger DAG (same pipeline as the processor DAG)
        dag_id = determine_dag_id(int_row.integration_type, int_row.schedule_type, int_row.utc_sch_cron)
        conf = build_integration_conf(int_row)
        trace_ctx = TraceContext.new()
        conf["traceparent"] = trace_ctx.traceparent
        merge_json_data(conf, int_row.json_data, log=logger)
        auth_creds = resolve_auth_credentials_sync(engine, int_row.workspace_id, log=logger)
        conf.update(auth_creds)

        from control_plane.app.core.config import settings
        dag_run_id = trigger_airflow_dag(
            settings.AIRFLOW_API_URL,
            settings.AIRFLOW_USERNAME,
            settings.AIRFLOW_PASSWORD,
            dag_id,
            conf,
            trigger_source="dlq_retry",
            log=logger,
        )

        _update_status(engine, dlq_id, "resolved", resolution_notes=f"Retried via API, dag_run_id={dag_run_id}")
        return {"dlq_id": dlq_id, "status": "resolved", "dag_run_id": dag_run_id}

    except Exception as e:
        logger.error("Retry failed for DLQ entry %s: %s", dlq_id, e, exc_info=True)
        _update_status(
            engine, dlq_id, "pending",
            error_type=type(e).__name__,
            error_message=str(e),
            increment_retry=True,
        )
        return {"dlq_id": dlq_id, "status": "pending", "message": f"Retry failed: {e}"}


@router.put("/{dlq_id}/resolve")
def resolve_dlq(dlq_id: int, body: ResolveRequest):
    """Mark a DLQ entry as manually resolved."""
    engine = _get_sync_engine()
    with engine.connect() as conn:
        row = conn.execute(
            select(dead_letter_messages).where(dead_letter_messages.c.dlq_id == dlq_id)
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail=f"DLQ entry {dlq_id} not found")

    _update_status(engine, dlq_id, "resolved", resolution_notes=body.resolution_notes)
    return {"dlq_id": dlq_id, "status": "resolved"}


@router.post("/bulk/resolve")
def bulk_resolve(body: BulkResolveRequest):
    """Bulk mark DLQ entries as resolved."""
    if not body.dlq_ids:
        raise HTTPException(status_code=400, detail="dlq_ids must not be empty")

    now = datetime.now(timezone.utc)
    values: Dict[str, Any] = {
        "status": "resolved",
        "resolved_at": now,
        "updated_at": now,
    }
    if body.resolution_notes:
        values["resolution_notes"] = body.resolution_notes

    engine = _get_sync_engine()
    with engine.begin() as conn:
        result = conn.execute(
            update(dead_letter_messages)
            .where(dead_letter_messages.c.dlq_id.in_(body.dlq_ids))
            .values(**values)
        )

    return {"updated": result.rowcount, "status": "resolved"}


@router.post("/bulk/retry")
def bulk_retry(body: BulkRetryRequest):
    """Bulk retry DLQ entries. Processes each sequentially."""
    if not body.dlq_ids:
        raise HTTPException(status_code=400, detail="dlq_ids must not be empty")

    results = []
    for dlq_id in body.dlq_ids:
        try:
            result = retry_dlq(dlq_id)
            results.append({"dlq_id": dlq_id, **result})
        except HTTPException as e:
            results.append({"dlq_id": dlq_id, "status": "error", "message": e.detail})
        except Exception as e:
            results.append({"dlq_id": dlq_id, "status": "error", "message": str(e)})

    return {"results": results}


# -- Helpers -------------------------------------------------------------------


def _update_status(
    engine: Engine,
    dlq_id: int,
    status: str,
    resolution_notes: Optional[str] = None,
    error_type: Optional[str] = None,
    error_message: Optional[str] = None,
    increment_retry: bool = False,
) -> None:
    """Update DLQ entry status."""
    now = datetime.now(timezone.utc)
    values: Dict[str, Any] = {"status": status, "updated_at": now}

    if status == "resolved":
        values["resolved_at"] = now
    if resolution_notes is not None:
        values["resolution_notes"] = resolution_notes
    if error_type is not None:
        values["error_type"] = error_type
    if error_message is not None:
        values["error_message"] = error_message

    with engine.begin() as conn:
        if increment_retry:
            row = conn.execute(
                select(dead_letter_messages.c.retry_count).where(
                    dead_letter_messages.c.dlq_id == dlq_id
                )
            ).fetchone()
            if row:
                values["retry_count"] = row.retry_count + 1

        conn.execute(
            update(dead_letter_messages)
            .where(dead_letter_messages.c.dlq_id == dlq_id)
            .values(**values)
        )
