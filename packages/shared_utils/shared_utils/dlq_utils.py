"""Dead Letter Queue utilities — shared across Airflow tasks and control plane.

Provides a single ``send_to_dlq`` function that performs the three-step
DLQ persistence strategy:
  1. Insert into ``dead_letter_messages`` MySQL table (durable, queryable).
  2. Publish to Kafka DLQ topic (real-time alerting subscribers).
  3. Emit an audit event (``message.dead_lettered``).

Each step is best-effort: a failure in one does not block the others.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def send_to_dlq(
    original_message: dict,
    error_message: str,
    source_topic: str,
    consumer_group: str,
    message_key: str,
    trace_id: str = "",
    db_url: str = "",
    kafka_bootstrap_servers: str = "",
    dlq_topic: str = "cdc.integration.events.dlq",
    retry_count: int = 3,
) -> None:
    """Persist a failed message to DLQ via all three channels.

    Args:
        original_message: The original Kafka message payload (dict).
        error_message: Human-readable error description.
        source_topic: Kafka topic the message originated from.
        consumer_group: Consumer group that was processing the message.
        message_key: Correlation key (typically integration_id as str).
        trace_id: W3C trace_id for log correlation.
        db_url: SQLAlchemy connection URL for the control-plane DB.
        kafka_bootstrap_servers: Comma-separated Kafka broker addresses.
        dlq_topic: Kafka topic to send the DLQ message to.
        retry_count: Number of retry attempts before DLQ.
    """
    # 1. Persist to database
    if db_url:
        _persist_to_db(
            db_url=db_url,
            original_message=original_message,
            error_message=error_message,
            source_topic=source_topic,
            consumer_group=consumer_group,
            message_key=message_key,
            retry_count=retry_count,
        )

    # 2. Send to Kafka DLQ topic
    if kafka_bootstrap_servers:
        _send_to_kafka_dlq(
            bootstrap_servers=kafka_bootstrap_servers,
            dlq_topic=dlq_topic,
            original_message=original_message,
            error_message=error_message,
            source_topic=source_topic,
            consumer_group=consumer_group,
            message_key=message_key,
            retry_count=retry_count,
        )

    # 3. Emit audit event
    _emit_dlq_audit(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        original_message=original_message,
        error_message=error_message,
        source_topic=source_topic,
        message_key=message_key,
        retry_count=retry_count,
        trace_id=trace_id,
    )


def _persist_to_db(
    db_url: str,
    original_message: dict,
    error_message: str,
    source_topic: str,
    consumer_group: str,
    message_key: str,
    retry_count: int,
) -> Optional[int]:
    """Insert a DLQ entry into the dead_letter_messages table."""
    try:
        from shared_models.tables import dead_letter_messages
        from shared_utils.db import create_control_plane_engine

        # Extract integration_id (soft reference)
        integration_id = original_message.get("integration_id")
        if not integration_id and isinstance(original_message.get("data"), dict):
            integration_id = original_message["data"].get("integration_id")

        try:
            integration_id = int(integration_id) if integration_id is not None else None
        except (ValueError, TypeError):
            integration_id = None

        now = datetime.now(timezone.utc)
        engine = create_control_plane_engine(db_url)
        try:
            with engine.begin() as conn:
                result = conn.execute(
                    dead_letter_messages.insert().values(
                        integration_id=integration_id,
                        source_topic=source_topic,
                        consumer_group=consumer_group,
                        message_key=message_key,
                        original_message=json.dumps(original_message),
                        error_type="TaskFailure",
                        error_message=error_message,
                        retry_count=retry_count,
                        status="pending",
                        created_at=now,
                        updated_at=now,
                    )
                )
                dlq_id = result.lastrowid
        finally:
            engine.dispose()

        logger.info("DLQ message persisted to DB: dlq_id=%s", dlq_id)
        return dlq_id

    except Exception as e:
        logger.error("Failed to persist DLQ message to DB: %s", e, exc_info=True)
        return None


def _send_to_kafka_dlq(
    bootstrap_servers: str,
    dlq_topic: str,
    original_message: dict,
    error_message: str,
    source_topic: str,
    consumer_group: str,
    message_key: str,
    retry_count: int,
) -> None:
    """Send the failed message to a Kafka DLQ topic."""
    try:
        from kafka import KafkaProducer

        dlq_payload = {
            "original_message": original_message,
            "error": {
                "type": "TaskFailure",
                "message": error_message,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            "retry_count": retry_count,
            "source_topic": source_topic,
            "consumer_group": consumer_group,
        }

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            max_block_ms=5000,
        )
        try:
            future = producer.send(dlq_topic, key=message_key, value=dlq_payload)
            future.get(timeout=10)
            logger.info("DLQ message sent to Kafka topic: %s", dlq_topic)
        finally:
            producer.close(timeout=5)

    except Exception as e:
        logger.error("Failed to send DLQ message to Kafka: %s", e, exc_info=True)


def _emit_dlq_audit(
    kafka_bootstrap_servers: str,
    original_message: dict,
    error_message: str,
    source_topic: str,
    message_key: str,
    retry_count: int,
    trace_id: str,
) -> None:
    """Emit an audit event for the dead-lettered message."""
    try:
        from shared_utils.audit_producer import get_audit_producer

        audit = get_audit_producer()
        if audit:
            audit.emit(
                customer_guid="unknown",
                event_type="message.dead_lettered",
                actor_id="airflow-triggerer",
                actor_type="system",
                resource_type="message",
                resource_id=message_key,
                action="dead_letter",
                outcome="failure",
                trace_id=trace_id,
                metadata={
                    "error_type": "TaskFailure",
                    "error_message": error_message[:500],
                    "retry_count": retry_count,
                    "source_topic": source_topic,
                },
            )
    except Exception:
        logger.debug("Failed to emit DLQ audit event", exc_info=True)
