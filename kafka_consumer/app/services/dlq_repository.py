"""Synchronous repository for dead_letter_messages table operations.

Uses pymysql (sync) to match the kafka_consumer's threading model.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from sqlalchemy import create_engine, select, update, func, and_, desc
from sqlalchemy.engine import Engine

from kafka_consumer.app.core.config import settings
from shared_models.tables import dead_letter_messages

logger = logging.getLogger(__name__)

_engine: Optional[Engine] = None


def _get_engine() -> Engine:
    """Lazy-initialized singleton engine."""
    global _engine
    if _engine is None:
        _engine = create_engine(
            settings.DATABASE_URL,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=2,
        )
    return _engine


def _row_to_dict(row) -> Dict[str, Any]:
    """Convert a SQLAlchemy Row to a dict with JSON-parsed original_message."""
    d = dict(row._mapping)
    # Parse original_message from JSON string
    if isinstance(d.get("original_message"), str):
        try:
            d["original_message"] = json.loads(d["original_message"])
        except json.JSONDecodeError:
            pass  # leave as string if not valid JSON
    # Serialize datetimes for API responses
    for key in ("created_at", "updated_at", "resolved_at"):
        if isinstance(d.get(key), datetime):
            d[key] = d[key].isoformat()
    return d


def persist_dlq_message(
    original_message: dict,
    error: Exception,
    retry_count: int,
    source_topic: str,
    consumer_group: str,
    message_key: Optional[str] = None,
) -> int:
    """Insert a DLQ entry into the database.

    Returns:
        The dlq_id of the inserted row.
    """
    # Extract integration_id (soft reference)
    integration_id = original_message.get("integration_id")
    if not integration_id and isinstance(original_message.get("data"), dict):
        integration_id = original_message.get("data", {}).get("integration_id")

    # Validate integration_id is an int or None
    try:
        integration_id = int(integration_id) if integration_id is not None else None
    except (ValueError, TypeError):
        integration_id = None

    now = datetime.now(timezone.utc)
    engine = _get_engine()
    with engine.begin() as conn:
        result = conn.execute(
            dead_letter_messages.insert().values(
                integration_id=integration_id,
                source_topic=source_topic,
                consumer_group=consumer_group,
                message_key=message_key,
                original_message=json.dumps(original_message),
                error_type=type(error).__name__,
                error_message=str(error),
                retry_count=retry_count,
                status="pending",
                created_at=now,
                updated_at=now,
            )
        )
        dlq_id = result.lastrowid

    logger.info(f"DLQ message persisted: dlq_id={dlq_id}, integration_id={integration_id}")
    return dlq_id


def list_dlq_messages(
    status: Optional[str] = None,
    integration_id: Optional[int] = None,
    created_after: Optional[datetime] = None,
    created_before: Optional[datetime] = None,
    limit: int = 50,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """Query DLQ entries with optional filters.

    Returns:
        List of DLQ entry dicts, ordered by created_at descending.
    """
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

    query = query.order_by(desc(dead_letter_messages.c.created_at))
    query = query.limit(limit).offset(offset)

    engine = _get_engine()
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    return [_row_to_dict(row) for row in rows]


def get_dlq_message(dlq_id: int) -> Optional[Dict[str, Any]]:
    """Get a single DLQ entry by ID."""
    engine = _get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            select(dead_letter_messages).where(
                dead_letter_messages.c.dlq_id == dlq_id
            )
        ).fetchone()

    if row is None:
        return None
    return _row_to_dict(row)


def update_dlq_status(
    dlq_id: int,
    status: str,
    resolution_notes: Optional[str] = None,
    error_type: Optional[str] = None,
    error_message: Optional[str] = None,
    increment_retry: bool = False,
) -> bool:
    """Update status of a DLQ entry.

    Sets resolved_at automatically when status is 'resolved'.

    Returns:
        True if a row was updated, False if dlq_id not found.
    """
    now = datetime.now(timezone.utc)
    values: Dict[str, Any] = {
        "status": status,
        "updated_at": now,
    }

    if status == "resolved":
        values["resolved_at"] = now
    if resolution_notes is not None:
        values["resolution_notes"] = resolution_notes
    if error_type is not None:
        values["error_type"] = error_type
    if error_message is not None:
        values["error_message"] = error_message

    engine = _get_engine()
    with engine.begin() as conn:
        if increment_retry:
            # Fetch current retry_count first
            row = conn.execute(
                select(dead_letter_messages.c.retry_count).where(
                    dead_letter_messages.c.dlq_id == dlq_id
                )
            ).fetchone()
            if row:
                values["retry_count"] = row.retry_count + 1

        result = conn.execute(
            update(dead_letter_messages)
            .where(dead_letter_messages.c.dlq_id == dlq_id)
            .values(**values)
        )

    return result.rowcount > 0


def bulk_update_status(
    dlq_ids: List[int],
    status: str,
    resolution_notes: Optional[str] = None,
) -> int:
    """Bulk update status for multiple DLQ entries.

    Returns:
        Number of rows updated.
    """
    if not dlq_ids:
        return 0

    now = datetime.now(timezone.utc)
    values: Dict[str, Any] = {
        "status": status,
        "updated_at": now,
    }
    if status == "resolved":
        values["resolved_at"] = now
    if resolution_notes is not None:
        values["resolution_notes"] = resolution_notes

    engine = _get_engine()
    with engine.begin() as conn:
        result = conn.execute(
            update(dead_letter_messages)
            .where(dead_letter_messages.c.dlq_id.in_(dlq_ids))
            .values(**values)
        )

    return result.rowcount


def get_dlq_stats() -> Dict[str, int]:
    """Get DLQ entry counts grouped by status.

    Returns:
        Dict mapping status to count, e.g. {"pending": 5, "resolved": 12}.
    """
    engine = _get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            select(
                dead_letter_messages.c.status,
                func.count().label("count"),
            ).group_by(dead_letter_messages.c.status)
        ).fetchall()

    return {row.status: row.count for row in rows}
