"""Kafka consumer that reads audit events and writes them to per-customer schemas.

Runs on a background daemon thread (same pattern as kafka_consumer service).
Applies sensitive data masking before persisting.
"""

import json
import logging
import re
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from audit_service.app.core.config import Settings
from audit_service.app.services.schema_manager import AuditSchemaManager

logger = logging.getLogger(__name__)

# Fields to scan for sensitive data
_SENSITIVE_PATTERN = re.compile(
    r"(password|secret|token|key|credential|api_key|access_key|secret_key)",
    re.IGNORECASE,
)

# Maximum size for state fields (1KB)
_MAX_STATE_SIZE = 1024


def _mask_sensitive(value: Optional[str]) -> Optional[str]:
    """Mask sensitive values in a JSON string and truncate if > 1KB."""
    if not value:
        return value
    try:
        data = json.loads(value)
        _mask_dict(data)
        result = json.dumps(data)
        if len(result) > _MAX_STATE_SIZE:
            return result[:_MAX_STATE_SIZE] + "...[TRUNCATED]"
        return result
    except (json.JSONDecodeError, TypeError):
        if len(value) > _MAX_STATE_SIZE:
            return value[:_MAX_STATE_SIZE] + "...[TRUNCATED]"
        return value


def _mask_dict(data: Any) -> None:
    """Recursively mask sensitive keys in a dict.

    Only masks leaf values (str, int, float, bool). If a sensitive key
    holds a dict or list, recurse into it instead of replacing.
    """
    if isinstance(data, dict):
        for key in data:
            if _SENSITIVE_PATTERN.search(str(key)):
                if isinstance(data[key], (dict, list)):
                    _mask_dict(data[key])
                else:
                    data[key] = "[REDACTED]"
            else:
                _mask_dict(data[key])
    elif isinstance(data, list):
        for item in data:
            _mask_dict(item)


class AuditConsumerService:
    """Consumes audit events from Kafka and writes to per-customer schemas."""

    def __init__(
        self,
        engine: Engine,
        schema_manager: AuditSchemaManager,
        settings: Settings,
    ):
        self._engine = engine
        self._schema_manager = schema_manager
        self._settings = settings
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._consumer = None

        # Health stats
        self.started_at: Optional[datetime] = None
        self.messages_processed: int = 0
        self.messages_failed: int = 0
        self.last_error: Optional[str] = None
        self.is_connected: bool = False

    def start(self) -> None:
        """Start the consumer in a background thread."""
        self._running = True
        self.started_at = datetime.now(timezone.utc)
        self._thread = threading.Thread(
            target=self._consume_loop, name="audit-consumer", daemon=True
        )
        self._thread.start()
        logger.info("Audit consumer started")

    def stop(self) -> None:
        """Stop the consumer."""
        self._running = False
        if self._consumer:
            try:
                self._consumer.close()
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Audit consumer stopped")

    def _consume_loop(self) -> None:
        """Main consume loop running on daemon thread."""
        from kafka import KafkaConsumer

        try:
            self._consumer = KafkaConsumer(
                self._settings.KAFKA_TOPIC,
                bootstrap_servers=self._settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=self._settings.KAFKA_CONSUMER_GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            self.is_connected = True
            logger.info(
                f"Audit consumer connected to {self._settings.KAFKA_BOOTSTRAP_SERVERS}"
            )
        except Exception as e:
            self.last_error = str(e)
            logger.exception("Failed to create Kafka consumer")
            return

        while self._running:
            try:
                # poll returns immediately if no messages (consumer_timeout_ms)
                for message in self._consumer:
                    if not self._running:
                        break
                    self._process_message(message.value)
            except Exception as e:
                if self._running:
                    self.last_error = str(e)
                    logger.exception("Error in audit consume loop")

    def _process_message(self, event: Dict[str, Any]) -> None:
        """Process a single audit event: validate, mask, route, insert."""
        try:
            customer_guid = event.get("customer_guid")
            if not customer_guid or customer_guid == "unknown":
                logger.warning("Audit event missing customer_guid — skipping")
                self.messages_failed += 1
                return

            if not self._schema_manager.schema_exists(customer_guid):
                logger.warning(
                    f"No audit schema for customer {customer_guid} — auto-provisioning"
                )
                try:
                    self._schema_manager.provision_customer(customer_guid)
                except Exception:
                    logger.exception(
                        f"Failed to auto-provision schema for {customer_guid}"
                    )
                    self.messages_failed += 1
                    return

            # Mask sensitive fields
            event["before_state"] = _mask_sensitive(event.get("before_state"))
            event["after_state"] = _mask_sensitive(event.get("after_state"))
            event["metadata_json"] = _mask_sensitive(event.get("metadata_json"))

            self._insert_event(customer_guid, event)
            self.messages_processed += 1

        except Exception as e:
            self.messages_failed += 1
            self.last_error = str(e)
            logger.exception("Failed to process audit event")

    def _insert_event(self, customer_guid: str, event: Dict[str, Any]) -> None:
        """Insert an audit event into the customer's schema."""
        schema = self._schema_manager._schema_name(customer_guid)
        with self._engine.connect() as conn:
            conn.execute(
                text(
                    f"INSERT INTO `{schema}`.`audit_events` "
                    "(event_id, timestamp, event_type, actor_id, actor_type, "
                    "actor_ip, resource_type, resource_id, action, outcome, "
                    "before_state, after_state, trace_id, request_id, metadata_json) "
                    "VALUES (:event_id, :timestamp, :event_type, :actor_id, :actor_type, "
                    ":actor_ip, :resource_type, :resource_id, :action, :outcome, "
                    ":before_state, :after_state, :trace_id, :request_id, :metadata_json) "
                    "ON DUPLICATE KEY UPDATE event_id = event_id"
                ),
                {
                    "event_id": event.get("event_id", ""),
                    "timestamp": event.get("timestamp", datetime.now(timezone.utc).isoformat()),
                    "event_type": event.get("event_type", ""),
                    "actor_id": event.get("actor_id", ""),
                    "actor_type": event.get("actor_type", ""),
                    "actor_ip": event.get("actor_ip"),
                    "resource_type": event.get("resource_type", ""),
                    "resource_id": event.get("resource_id", ""),
                    "action": event.get("action", ""),
                    "outcome": event.get("outcome", ""),
                    "before_state": event.get("before_state"),
                    "after_state": event.get("after_state"),
                    "trace_id": event.get("trace_id"),
                    "request_id": event.get("request_id"),
                    "metadata_json": event.get("metadata_json"),
                },
            )
            conn.commit()
