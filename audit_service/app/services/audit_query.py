"""Query service for audit events with cursor-based pagination."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from audit_service.app.services.schema_manager import AuditSchemaManager

logger = logging.getLogger(__name__)


class AuditQueryService:
    """Queries audit events from per-customer schemas."""

    def __init__(self, engine: Engine, schema_manager: AuditSchemaManager):
        self._engine = engine
        self._schema_manager = schema_manager

    def query_events(
        self,
        customer_guid: str,
        event_type: Optional[str] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        actor_id: Optional[str] = None,
        action: Optional[str] = None,
        outcome: Optional[str] = None,
        trace_id: Optional[str] = None,
        from_ts: Optional[datetime] = None,
        to_ts: Optional[datetime] = None,
        cursor: Optional[str] = None,
        limit: int = 50,
    ) -> Tuple[List[Dict[str, Any]], Optional[str], bool]:
        """Query audit events for a customer.

        Returns (events, next_cursor, has_more).
        """
        if not self._schema_manager.schema_exists(customer_guid):
            return [], None, False

        schema = self._schema_manager._schema_name(customer_guid)

        # Build WHERE clauses
        conditions = []
        params: Dict[str, Any] = {}

        if event_type:
            conditions.append("event_type = :event_type")
            params["event_type"] = event_type
        if resource_type:
            conditions.append("resource_type = :resource_type")
            params["resource_type"] = resource_type
        if resource_id:
            conditions.append("resource_id = :resource_id")
            params["resource_id"] = resource_id
        if actor_id:
            conditions.append("actor_id = :actor_id")
            params["actor_id"] = actor_id
        if action:
            conditions.append("action = :action")
            params["action"] = action
        if outcome:
            conditions.append("outcome = :outcome")
            params["outcome"] = outcome
        if trace_id:
            conditions.append("trace_id = :trace_id")
            params["trace_id"] = trace_id
        if from_ts:
            conditions.append("timestamp >= :from_ts")
            params["from_ts"] = from_ts
        if to_ts:
            conditions.append("timestamp <= :to_ts")
            params["to_ts"] = to_ts
        if cursor:
            conditions.append("event_id > :cursor")
            params["cursor"] = cursor

        where = " AND ".join(conditions) if conditions else "1=1"
        params["limit"] = limit + 1  # fetch one extra to check has_more

        query = (
            f"SELECT event_id, timestamp, event_type, actor_id, actor_type, "
            f"actor_ip, resource_type, resource_id, action, outcome, "
            f"before_state, after_state, trace_id, request_id, metadata_json "
            f"FROM `{schema}`.`audit_events` "
            f"WHERE {where} "
            f"ORDER BY event_id ASC "
            f"LIMIT :limit"
        )

        with self._engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = result.fetchall()

        has_more = len(rows) > limit
        events = []
        for row in rows[:limit]:
            events.append({
                "event_id": row[0],
                "timestamp": row[1].isoformat() if isinstance(row[1], datetime) else str(row[1]),
                "event_type": row[2],
                "actor_id": row[3],
                "actor_type": row[4],
                "actor_ip": row[5],
                "resource_type": row[6],
                "resource_id": row[7],
                "action": row[8],
                "outcome": row[9],
                "before_state": row[10],
                "after_state": row[11],
                "trace_id": row[12],
                "request_id": row[13],
                "metadata_json": row[14],
            })

        next_cursor = events[-1]["event_id"] if has_more and events else None
        return events, next_cursor, has_more
