"""Fire-and-forget audit event producer backed by Kafka.

Uses a daemon thread + bounded queue (ThreadedStrategy) so that:
- ``emit()`` never blocks the caller and never raises.
- Kafka failures are logged and dropped — business logic is unaffected.
- The daemon thread lazily initialises KafkaProducer on first send.

Usage::

    from shared_utils import get_audit_producer

    producer = get_audit_producer("kafka:29092", "audit.events")
    producer.emit(
        customer_guid="cust-001",
        event_type="integration.created",
        actor_id="user@example.com",
        actor_type="user",
        resource_type="integration",
        resource_id="42",
        action="create",
        outcome="success",
    )
"""

import json
import logging
import queue
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Sentinel value to signal the drain loop to shut down.
_SHUTDOWN = object()

DEFAULT_TOPIC = "audit.events"
DEFAULT_QUEUE_SIZE = 10_000


class AuditProducer:
    """Thread-safe, fire-and-forget audit event producer.

    A bounded ``queue.Queue`` decouples the caller from Kafka I/O.
    A daemon thread drains the queue and sends events via ``kafka-python``.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str = DEFAULT_TOPIC,
        queue_size: int = DEFAULT_QUEUE_SIZE,
    ):
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self._kafka_producer = None  # lazy init on daemon thread
        self._stats = {"emitted": 0, "sent": 0, "dropped": 0, "errors": 0}
        self._lock = threading.Lock()

        self._thread = threading.Thread(
            target=self._drain_loop, name="audit-producer", daemon=True
        )
        self._thread.start()

    # ── public API ──────────────────────────────────────────────────────

    def emit(
        self,
        customer_guid: str,
        event_type: str,
        actor_id: str,
        actor_type: str,
        resource_type: str,
        resource_id: str,
        action: str,
        outcome: str,
        actor_ip: Optional[str] = None,
        before_state: Optional[Dict[str, Any]] = None,
        after_state: Optional[Dict[str, Any]] = None,
        trace_id: Optional[str] = None,
        request_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Enqueue an audit event.  Never raises, never blocks."""
        try:
            event = {
                "event_id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "customer_guid": customer_guid,
                "event_type": event_type,
                "actor_id": actor_id,
                "actor_type": actor_type,
                "actor_ip": actor_ip,
                "resource_type": resource_type,
                "resource_id": str(resource_id),
                "action": action,
                "outcome": outcome,
                "before_state": json.dumps(before_state) if before_state else None,
                "after_state": json.dumps(after_state) if after_state else None,
                "trace_id": trace_id,
                "request_id": request_id,
                "metadata_json": json.dumps(metadata) if metadata else None,
            }
            self._queue.put_nowait(event)
            with self._lock:
                self._stats["emitted"] += 1
        except queue.Full:
            with self._lock:
                self._stats["dropped"] += 1
            logger.warning("Audit queue full — event dropped")
        except Exception:
            with self._lock:
                self._stats["dropped"] += 1
            logger.exception("Unexpected error in audit emit()")

    def close(self, timeout: float = 5.0) -> None:
        """Signal shutdown and wait for the drain thread to finish."""
        try:
            self._queue.put(_SHUTDOWN, timeout=2.0)
        except queue.Full:
            pass
        self._thread.join(timeout=timeout)
        if self._kafka_producer:
            try:
                self._kafka_producer.close(timeout=2.0)
            except Exception:
                pass

    @property
    def stats(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._stats)

    # ── drain loop (runs on daemon thread) ──────────────────────────────

    def _drain_loop(self) -> None:
        """Drain the queue and send events to Kafka."""
        while True:
            try:
                event = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue

            if event is _SHUTDOWN:
                break

            self._send(event)

    def _send(self, event: Dict[str, Any]) -> None:
        """Send a single event to Kafka, with lazy producer init."""
        try:
            if self._kafka_producer is None:
                self._kafka_producer = self._create_kafka_producer()

            key = event.get("customer_guid", "").encode("utf-8")
            value = json.dumps(event).encode("utf-8")
            self._kafka_producer.send(self._topic, key=key, value=value)
            with self._lock:
                self._stats["sent"] += 1
        except Exception:
            with self._lock:
                self._stats["errors"] += 1
            logger.exception("Failed to send audit event to Kafka")
            # Reset producer on failure so next attempt reconnects
            self._kafka_producer = None

    def _create_kafka_producer(self):
        """Lazy-init KafkaProducer."""
        from kafka import KafkaProducer as _KafkaProducer

        return _KafkaProducer(
            bootstrap_servers=self._bootstrap_servers.split(","),
            value_serializer=None,  # we serialize ourselves
            key_serializer=None,
            retries=3,
            acks="all",
            max_block_ms=5000,
        )


class _NoOpProducer:
    """Drop-in replacement that silently discards all events.

    Used when Kafka is not configured (e.g. KAFKA_BOOTSTRAP_SERVERS is empty).
    """

    def emit(self, **kwargs) -> None:
        pass

    def close(self, timeout: float = 5.0) -> None:
        pass

    @property
    def stats(self) -> Dict[str, int]:
        return {"emitted": 0, "sent": 0, "dropped": 0, "errors": 0}


def get_audit_producer(
    bootstrap_servers: str = "",
    topic: str = DEFAULT_TOPIC,
    queue_size: int = DEFAULT_QUEUE_SIZE,
) -> "AuditProducer | _NoOpProducer":
    """Factory: returns AuditProducer if Kafka is configured, else NoOp."""
    if not bootstrap_servers:
        logger.info("KAFKA_BOOTSTRAP_SERVERS not set — audit producer disabled")
        return _NoOpProducer()
    return AuditProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        queue_size=queue_size,
    )
