"""Kafka consumer service for processing CDC events."""

import json
import logging
import threading
import time
from typing import Optional, Callable
from datetime import datetime, timezone

import redis as redis_lib
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from sqlalchemy.exc import SQLAlchemyError

from kafka_consumer.app.core.config import settings
from kafka_consumer.app.services.message_deduplicator import MessageDeduplicator
from shared_utils.audit_producer import get_audit_producer

logger = logging.getLogger(__name__)

# Lazy-init audit producer for the kafka consumer service
_audit_producer = None
_audit_producer_init = False


def _get_audit_producer():
    """Lazy singleton for audit producer."""
    global _audit_producer, _audit_producer_init
    if _audit_producer_init:
        return _audit_producer
    _audit_producer_init = True
    try:
        _audit_producer = get_audit_producer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        )
    except Exception:
        logger.exception("Failed to init audit producer in kafka consumer")
        _audit_producer = None
    return _audit_producer


# Retry backoff schedule (seconds) for transient errors
RETRY_BACKOFF = [1, 5, 30]


class KafkaConsumerService:
    """
    Kafka consumer service for processing CDC events.

    Subscribes to CDC topic and processes integration events:
    - integration.created
    - integration.updated
    - integration.deleted
    - integration.run.started
    - integration.run.completed
    - integration.run.failed
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str = "cdc-consumer",
        event_handler: Optional[Callable] = None,
        dlq_topic: Optional[str] = None,
        max_retries: int = 3,
        enable_dlq: bool = True,
    ):
        """
        Initialize Kafka consumer service.

        Args:
            bootstrap_servers: Kafka bootstrap servers (e.g., "localhost:9092")
            topic: Kafka topic to subscribe to
            group_id: Consumer group ID
            event_handler: Optional callback function to handle events
            dlq_topic: Dead Letter Queue topic name (default: {topic}.dlq)
            max_retries: Maximum retry attempts before sending to DLQ (default: 3)
            enable_dlq: Enable Dead Letter Queue (default: True)
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.event_handler = event_handler
        self.dlq_topic = dlq_topic or f"{topic}.dlq"
        self.max_retries = max_retries
        self.enable_dlq = enable_dlq
        self.consumer: Optional[KafkaConsumer] = None
        self.dlq_producer: Optional[KafkaProducer] = None
        self.running = False
        self.thread: Optional[threading.Thread] = None

        # Deduplication
        self.deduplicator: Optional[MessageDeduplicator] = None
        if settings.KAFKA_DEDUP_ENABLED:
            self.deduplicator = MessageDeduplicator(
                ttl_seconds=settings.KAFKA_DEDUP_TTL_SECONDS,
                claim_ttl_seconds=settings.KAFKA_DEDUP_CLAIM_TTL_SECONDS,
            )

        # Health observability
        self.started_at: Optional[datetime] = None
        self.last_message_time: Optional[datetime] = None
        self.messages_processed: int = 0
        self.messages_failed: int = 0
        self.messages_deduplicated: int = 0
        self.last_error: Optional[str] = None
        self.is_connected: bool = False

        # DLQ producer is lazily initialized when first needed,
        # because Kafka may not be ready at startup time.

    def _get_dlq_producer(self) -> Optional[KafkaProducer]:
        """Get or lazily initialize the DLQ producer."""
        if self.dlq_producer is not None:
            return self.dlq_producer
        if not self.enable_dlq:
            return None
        try:
            self.dlq_producer = KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            logger.info(f"DLQ producer initialized for topic: {self.dlq_topic}")
        except Exception as e:
            logger.error(f"Failed to initialize DLQ producer: {e}")
            self.dlq_producer = None
        return self.dlq_producer

    def _send_to_dlq(self, message: dict, error: Exception, retry_count: int):
        """
        Send failed message to Dead Letter Queue.

        Primary: persist to database (durable, queryable).
        Secondary: send to Kafka DLQ topic (for real-time alerting subscribers).

        Args:
            message: Original message that failed
            error: Exception that caused the failure
            retry_count: Number of retry attempts
        """
        # Extract message key for correlation
        integration_id = message.get("integration_id")
        if not integration_id and isinstance(message.get("data"), dict):
            integration_id = message.get("data", {}).get("integration_id")
        message_key = str(integration_id if integration_id else "unknown")

        # 1. PRIMARY: Persist to database
        if settings.KAFKA_DLQ_DB_ENABLED:
            try:
                from kafka_consumer.app.services.dlq_repository import persist_dlq_message
                dlq_id = persist_dlq_message(
                    original_message=message,
                    error=error,
                    retry_count=retry_count,
                    source_topic=self.topic,
                    consumer_group=self.group_id,
                    message_key=message_key,
                )
                logger.info(f"DLQ message persisted to database: dlq_id={dlq_id}")
            except Exception as db_err:
                logger.error(f"Failed to persist DLQ message to database: {db_err}", exc_info=True)

        # 2. SECONDARY: Send to Kafka DLQ topic
        producer = self._get_dlq_producer()
        if not self.enable_dlq or not producer:
            logger.debug("Kafka DLQ producer not enabled or not initialized, skipping Kafka DLQ send")
            return

        try:
            dlq_message = {
                "original_message": message,
                "error": {
                    "type": type(error).__name__,
                    "message": str(error),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                "retry_count": retry_count,
                "source_topic": self.topic,
                "consumer_group": self.group_id,
            }

            future = producer.send(
                self.dlq_topic,
                key=message_key,
                value=dlq_message
            )
            future.get(timeout=10)

            logger.warning(
                f"Message sent to Kafka DLQ: integration_id={message.get('integration_id')}, "
                f"error={type(error).__name__}, retries={retry_count}",
                extra={"dlq_message": dlq_message}
            )

        except Exception as e:
            logger.error(f"Failed to send message to Kafka DLQ: {e}", exc_info=True)

        # Emit audit event for dead-lettered message
        try:
            audit = _get_audit_producer()
            if audit:
                audit.emit(
                    customer_guid="unknown",
                    event_type="message.dead_lettered",
                    actor_id="kafka-consumer",
                    actor_type="system",
                    resource_type="message",
                    resource_id=message_key,
                    action="dead_letter",
                    outcome="failure",
                    metadata={
                        "error_type": type(error).__name__,
                        "error_message": str(error)[:500],
                        "retry_count": retry_count,
                        "source_topic": self.topic,
                    },
                )
        except Exception:
            logger.debug("Failed to emit audit event for DLQ", exc_info=True)

        # Reset retry counter (no-op now; retry is handled inline in consume loop)

    @staticmethod
    def _extract_trace_context(record):
        """Extract TraceContext from Kafka message headers (W3C traceparent format).

        Debezium's tracing interceptor adds a 'traceparent' header like:
        '00-<32-char-trace-id>-<16-char-span-id>-<2-char-flags>'

        Falls back to generating a new TraceContext if header is missing.

        Returns:
            TraceContext instance
        """
        from shared_utils import TraceContext
        return TraceContext.from_kafka_headers(record.headers)

    def start(self) -> None:
        """Start the Kafka consumer in a background thread."""
        if self.running:
            logger.warning("Kafka consumer already running")
            return

        logger.info(f"Starting Kafka consumer for topic: {self.topic}")
        self.running = True
        self.started_at = datetime.now(timezone.utc)
        self.thread = threading.Thread(target=self._consume_loop, daemon=True)
        self.thread.start()
        logger.info("Kafka consumer started successfully")

    def stop(self) -> None:
        """Stop the Kafka consumer."""
        if not self.running:
            return

        logger.info("Stopping Kafka consumer...")
        self.running = False

        if self.consumer:
            try:
                self.consumer.close()
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")

        if self.dlq_producer:
            try:
                self.dlq_producer.flush()
                self.dlq_producer.close()
                logger.info("DLQ producer closed")
            except Exception as e:
                logger.error(f"Error closing DLQ producer: {e}")

        if self.thread:
            self.thread.join(timeout=5)

        self.is_connected = False
        logger.info("Kafka consumer stopped")

    def _consume_loop(self) -> None:
        """Main consumer loop running in background thread."""
        try:
            # Initialize Kafka consumer with retry on broker unavailability.
            # Kafka may not be ready when the service starts, so we
            # retry up to 30 times (60 seconds total) before giving up.
            max_connect_retries = 30
            for attempt in range(1, max_connect_retries + 1):
                if not self.running:
                    return
                try:
                    self.consumer = KafkaConsumer(
                        self.topic,
                        bootstrap_servers=[self.bootstrap_servers],
                        group_id=self.group_id,
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m is not None else None,
                        consumer_timeout_ms=1000,
                    )
                    break
                except KafkaError as e:
                    if attempt < max_connect_retries:
                        logger.warning(
                            f"Kafka not ready (attempt {attempt}/{max_connect_retries}): {e}"
                        )
                        time.sleep(2)
                    else:
                        raise

            self.is_connected = True
            logger.info(f"Kafka consumer connected to {self.bootstrap_servers}")
            logger.info(f"Subscribed to topic: {self.topic}")

            # Consume messages
            while self.running:
                try:
                    messages = self.consumer.poll(timeout_ms=1000, max_records=10)

                    for topic_partition, records in messages.items():
                        for record in records:
                            if record.value is None:
                                logger.debug("Skipping tombstone message (None value)")
                                continue

                            # Extract trace context early so it's available in all error handlers
                            message = record.value
                            trace_ctx = self._extract_trace_context(record)
                            trace_id = trace_ctx.trace_id

                            success = False
                            for attempt in range(self.max_retries + 1):
                                try:
                                    if self._process_with_dedup(record, trace_ctx):
                                        success = True  # processed successfully
                                    else:
                                        success = True  # duplicate, considered handled
                                    break

                                except (redis_lib.RedisError, SQLAlchemyError, KafkaError,
                                        ConnectionError, TimeoutError, OSError) as e:
                                    # Transient error — retry with backoff
                                    self.messages_failed += 1
                                    self.last_error = str(e)
                                    if attempt < self.max_retries:
                                        wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                                        logger.warning(
                                            f"Transient error (attempt {attempt + 1}/{self.max_retries}): {e}. "
                                            f"Retrying in {wait}s...",
                                            exc_info=True,
                                            extra={"kafka_message": message, "trace_id": trace_id},
                                        )
                                        time.sleep(wait)
                                    else:
                                        logger.error(
                                            f"Max retries exceeded for transient error: {e}",
                                            exc_info=True,
                                            extra={"kafka_message": message, "trace_id": trace_id},
                                        )
                                        self._send_to_dlq(message, e, attempt + 1)
                                        success = True

                                except (ValueError, KeyError, TypeError) as e:
                                    # Permanent error — DLQ immediately, no retry
                                    self.messages_failed += 1
                                    self.last_error = str(e)
                                    logger.error(
                                        f"Permanent error, sending to DLQ: {e}",
                                        exc_info=True,
                                        extra={"kafka_message": message, "trace_id": trace_id},
                                    )
                                    self._send_to_dlq(message, e, 0)
                                    success = True
                                    break

                                except Exception as e:
                                    # Unexpected error — treat as permanent, DLQ immediately
                                    self.messages_failed += 1
                                    self.last_error = str(e)
                                    logger.error(
                                        f"Unexpected error, sending to DLQ: {e}",
                                        exc_info=True,
                                        extra={"kafka_message": message, "trace_id": trace_id},
                                    )
                                    self._send_to_dlq(message, e, 0)
                                    success = True
                                    break

                            if success:
                                self.consumer.commit()

                except Exception as e:
                    if self.running:
                        logger.error(f"Error in consumer loop: {e}", exc_info=True)

        except KafkaError as e:
            self.last_error = str(e)
            logger.error(f"Kafka error: {e}", exc_info=True)
        except Exception as e:
            self.last_error = str(e)
            logger.error(f"Unexpected error in consumer: {e}", exc_info=True)
        finally:
            self.is_connected = False
            if self.consumer:
                try:
                    self.consumer.close()
                except Exception as e:
                    logger.error(f"Error closing consumer: {e}")

    def _process_with_dedup(self, record, trace_ctx) -> bool:
        """Process message with two-phase deduplication.

        Phase 1 (claim): SET NX key="P" with short TTL (lease)
        Phase 2 (confirm): SET key="C" with long TTL after success

        Returns True if message was processed, False if it was a duplicate.
        Raises on processing failure (caller handles retry/DLQ).
        """
        message = record.value

        # Phase 1: claim
        dedup_key = None
        if self.deduplicator:
            dedup_key = self.deduplicator.build_dedup_key(self.topic, record, message)
            status = self.deduplicator.claim(dedup_key)

            if status == "completed":
                logger.info(
                    "Skipping duplicate message (dedup_key=%s, status=C)",
                    dedup_key,
                    extra={"trace_id": trace_ctx.trace_id},
                )
                self.messages_deduplicated += 1
                return False

            if status == "processing":
                logger.info(
                    "Re-processing message from crashed attempt (dedup_key=%s, status=P)",
                    dedup_key,
                    extra={"trace_id": trace_ctx.trace_id},
                )

        try:
            self._process_message(
                message,
                trace_id=trace_ctx.trace_id,
                traceparent=trace_ctx.traceparent,
            )
            # Phase 2: confirm
            if self.deduplicator and dedup_key:
                self.deduplicator.confirm(dedup_key)
            self.messages_processed += 1
            self.last_message_time = datetime.now(timezone.utc)
            return True
        except Exception:
            # Don't remove key — leave "P" so it auto-expires via claim TTL.
            # Kafka will redeliver and claim() will see "P" → allow re-process.
            raise

    def _process_message(self, message: dict, trace_id: str = "", traceparent: str = "") -> None:
        """
        Process a CDC event message.

        Supports two formats:
        1. Real Debezium CDC events (has __op field)
        2. Legacy manually-published events (has event_type field)

        Args:
            message: CDC event message
            trace_id: 32-char hex trace ID for log correlation
            traceparent: Full W3C traceparent header for propagation to Airflow
        """
        # Check if this is a real Debezium CDC event
        if "__op" in message:
            # Real Debezium CDC event
            operation = message.get("__op")
            integration_id = message.get("integration_id")

            logger.info(
                f"Processing Debezium CDC event: operation={operation}, integration_id={integration_id}",
                extra={"operation": operation, "integration_id": integration_id, "trace_id": trace_id},
            )

            # Map Debezium operations to event types
            if operation == "c":  # Create
                event_type = "integration.created"
            elif operation == "u":  # Update
                event_type = "integration.updated"
            elif operation == "d":  # Delete
                event_type = "integration.deleted"
            else:
                logger.warning(f"Unknown Debezium operation: {operation}", extra={"trace_id": trace_id})
                return

            # Use the entire message as data (includes all database columns)
            data = message.copy()
            data["is_debezium_event"] = True

        elif "event_type" in message:
            # Legacy format (manually published)
            event_type = message.get("event_type")
            event_id = message.get("event_id")
            timestamp = message.get("timestamp")
            data = message.get("data", {})
            data["is_debezium_event"] = False

            logger.info(
                f"Processing legacy CDC event: {event_type}",
                extra={
                    "event_id": event_id,
                    "event_type": event_type,
                    "timestamp": timestamp,
                    "trace_id": trace_id,
                },
            )
        else:
            logger.warning(f"Unknown message format: {list(message.keys())}", extra={"trace_id": trace_id})
            return

        # Call event handler if provided
        if self.event_handler:
            try:
                self.event_handler(event_type, data)
            except Exception as e:
                logger.error(
                    f"Error in event handler: {e}",
                    exc_info=True,
                    extra={"event_type": event_type, "data": data, "trace_id": trace_id},
                )
                # Re-raise only if DLQ is enabled (for retry tracking)
                if self.enable_dlq:
                    raise
        else:
            # Default processing based on event type
            self._default_event_processing(event_type, data, trace_id=trace_id, traceparent=traceparent)

    def _default_event_processing(self, event_type: str, data: dict, trace_id: str = "", traceparent: str = "") -> None:
        """
        Default event processing logic.

        Args:
            event_type: Type of CDC event
            data: Event data
            trace_id: 32-char hex trace ID for log correlation
            traceparent: Full W3C traceparent header for propagation to Airflow
        """
        if event_type == "integration.created":
            integration_id = data.get('integration_id')
            logger.info(
                f"Integration created: {integration_id}",
                extra={"data": data, "trace_id": trace_id},
            )

            # Trigger initial data sync if integration data is provided
            if self._should_trigger_integration(data):
                try:
                    self._trigger_integration_workflow(data, trace_id=trace_id, traceparent=traceparent)
                except Exception as e:
                    logger.error(
                        f"Failed to trigger workflow for integration {integration_id}: {e}",
                        exc_info=True,
                        extra={"trace_id": trace_id},
                    )

        elif event_type == "integration.updated":
            logger.info(
                f"Integration updated: {data.get('integration_id')}",
                extra={"data": data, "trace_id": trace_id},
            )
            # Configuration updates don't automatically trigger workflows

        elif event_type == "integration.deleted":
            logger.info(
                f"Integration deleted: {data.get('integration_id')}",
                extra={"data": data, "trace_id": trace_id},
            )
            # Cleanup is handled by cascade delete in database

        elif event_type == "integration.run.started":
            logger.info(
                f"Integration run started: {data.get('run_id')}",
                extra={"data": data, "trace_id": trace_id},
            )

        elif event_type == "integration.run.completed":
            logger.info(
                f"Integration run completed: {data.get('run_id')}",
                extra={"data": data, "trace_id": trace_id},
            )

        elif event_type == "integration.run.failed":
            logger.error(
                f"Integration run failed: {data.get('run_id')}",
                extra={"data": data, "trace_id": trace_id},
            )
            # TODO: Send alerts, retry logic, etc.

        else:
            logger.warning(f"Unknown event type: {event_type}", extra={"data": data, "trace_id": trace_id})

    def _should_trigger_integration(self, data: dict) -> bool:
        """
        Determine if integration workflow should be triggered.

        Args:
            data: Event data

        Returns:
            True if workflow should be triggered
        """
        # For Debezium CDC events, always trigger on create
        if data.get("is_debezium_event"):
            return data.get("integration_id") is not None

        # For legacy events, check if we have the necessary config
        return (
            data.get("workflow_type")
            and data.get("source_config")
            and data.get("destination_config")
        )

    def _trigger_integration_workflow(self, data: dict, trace_id: str = "", traceparent: str = "") -> None:
        """
        Trigger Airflow DAG for integration.

        Uses sync SQLAlchemy Core (pymysql) and sync requests to avoid asyncio
        event loop conflicts — this method runs in the Kafka consumer's
        background thread, which has no event loop.

        Args:
            data: Integration event data with workflow configuration
            trace_id: 32-char hex trace ID for log correlation
            traceparent: Full W3C traceparent header for propagation to Airflow
        """
        import json as json_module
        from sqlalchemy import create_engine, select, func
        from shared_models.tables import integrations as integrations_t, integration_runs as integration_runs_t
        from shared_utils import (
            build_integration_conf,
            merge_json_data,
            resolve_auth_credentials_sync,
            determine_dag_id,
            trigger_airflow_dag,
        )

        integration_id = data.get("integration_id")
        logger.info(
            f"Triggering workflow for integration {integration_id}",
            extra={"trace_id": trace_id},
        )

        try:
            engine = create_engine(settings.DATABASE_URL)

            # --- Step 1: Build execution_config from event data ---
            execution_config = {}
            if not data.get("is_debezium_event"):
                # Legacy events carry config inline
                if data.get("source_config"):
                    execution_config.update(data["source_config"])
                if data.get("destination_config"):
                    execution_config.update(data["destination_config"])

            # --- Step 2: Query integration from DB (both paths need this) ---
            with engine.connect() as conn:
                row = conn.execute(
                    select(integrations_t).where(
                        integrations_t.c.integration_id == integration_id
                    )
                ).fetchone()

            if not row:
                logger.error(
                    f"Integration {integration_id} not found in database",
                    extra={"trace_id": trace_id},
                )
                return

            # For Debezium events, extract execution_config from json_data
            if data.get("is_debezium_event"):
                logger.info(
                    f"Processing Debezium CDC event - querying database for integration {integration_id}",
                    extra={"trace_id": trace_id},
                )
                if row.json_data:
                    try:
                        execution_config = json_module.loads(row.json_data)
                        logger.info(
                            f"Extracted execution config from database: {list(execution_config.keys())}",
                            extra={"trace_id": trace_id},
                        )
                    except json_module.JSONDecodeError as e:
                        logger.error(
                            f"Failed to parse json_data for integration {integration_id}: {e}",
                            extra={"trace_id": trace_id},
                        )
                else:
                    logger.warning(
                        f"Integration {integration_id} has no json_data",
                        extra={"trace_id": trace_id},
                    )

            # --- Step 3: Determine DAG ID (shared) ---
            dag_id = determine_dag_id(row.integration_type, row.schedule_type, row.utc_sch_cron)

            # --- Step 4: Build DAG run conf (shared) ---
            conf = build_integration_conf(row)
            conf["traceparent"] = traceparent
            merge_json_data(conf, row.json_data, log=logger)

            # --- Step 5: Resolve auth credentials (shared) ---
            auth_creds = resolve_auth_credentials_sync(engine, row.workspace_id, log=logger)
            conf.update(auth_creds)

            # Override with execution_config (but preserve traceparent)
            if execution_config:
                conf.update(execution_config)
            conf["traceparent"] = traceparent

            # --- Step 6: Trigger Airflow DAG via REST API (shared) ---
            dag_run_id = trigger_airflow_dag(
                settings.AIRFLOW_API_URL,
                settings.AIRFLOW_USERNAME,
                settings.AIRFLOW_PASSWORD,
                dag_id,
                conf,
                trigger_source="manual",
                log=logger,
            )

            # --- Step 7: Record IntegrationRun in DB ---
            with engine.begin() as conn:
                conn.execute(
                    integration_runs_t.insert().values(
                        integration_id=integration_id,
                        dag_run_id=dag_run_id,
                        execution_date=func.now(),
                        started=func.now(),
                    )
                )

            engine.dispose()

            logger.info(
                f"Workflow triggered successfully for integration {integration_id}",
                extra={"dag_run_id": dag_run_id, "dag_id": dag_id, "trace_id": trace_id},
            )

            # Emit audit event for CDC-triggered integration
            try:
                audit = _get_audit_producer()
                if audit:
                    audit.emit(
                        customer_guid=conf.get("tenant_id", "unknown"),
                        event_type="integration.cdc_triggered",
                        actor_id="kafka-consumer",
                        actor_type="system",
                        resource_type="integration",
                        resource_id=str(integration_id),
                        action="trigger",
                        outcome="success",
                        trace_id=trace_id,
                        metadata={
                            "dag_id": dag_id,
                            "dag_run_id": dag_run_id,
                            "trigger_source": "cdc",
                        },
                    )
            except Exception:
                logger.debug("Failed to emit audit event for CDC trigger", exc_info=True)

        except Exception as e:
            logger.error(
                f"Error triggering workflow: {e}",
                exc_info=True,
                extra={"integration_id": integration_id, "trace_id": trace_id},
            )
            raise


# Global consumer service instance
_consumer_service: Optional[KafkaConsumerService] = None


def get_kafka_consumer_service() -> Optional[KafkaConsumerService]:
    """Get the global Kafka consumer service instance."""
    return _consumer_service


def initialize_kafka_consumer(event_handler: Optional[Callable] = None) -> None:
    """
    Initialize and start the Kafka consumer service.

    Args:
        event_handler: Optional callback function to handle events
    """
    global _consumer_service

    if _consumer_service is not None:
        logger.warning("Kafka consumer already initialized")
        return

    # Use settings from config
    bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
    topic = settings.KAFKA_TOPIC_CDC
    dlq_topic = settings.KAFKA_TOPIC_DLQ
    enable_dlq = settings.KAFKA_DLQ_ENABLED
    max_retries = settings.KAFKA_DLQ_MAX_RETRIES

    logger.info(
        f"Initializing Kafka consumer: {bootstrap_servers}, topic: {topic}, "
        f"DLQ: {enable_dlq} (topic: {dlq_topic}, max_retries: {max_retries})"
    )

    _consumer_service = KafkaConsumerService(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=settings.KAFKA_CONSUMER_GROUP,
        dlq_topic=dlq_topic,
        max_retries=max_retries,
        enable_dlq=enable_dlq,
        event_handler=event_handler,
    )

    _consumer_service.start()
    logger.info(
        f"Kafka consumer initialized successfully with DLQ support "
        f"(max_retries: {max_retries})"
    )


def shutdown_kafka_consumer() -> None:
    """Shutdown the Kafka consumer service."""
    global _consumer_service

    if _consumer_service is None:
        return

    logger.info("Shutting down Kafka consumer")
    _consumer_service.stop()
    _consumer_service = None
