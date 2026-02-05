"""Kafka consumer service for processing CDC events."""

import json
import logging
import threading
import time
from typing import Optional, Callable, Dict
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from control_plane.app.core.config import settings

logger = logging.getLogger(__name__)


class MessageRetryTracker:
    """Track message retry attempts in memory."""

    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries
        self.retry_counts: Dict[str, int] = {}
        self.lock = threading.Lock()

    def get_retry_count(self, message_key: str) -> int:
        """Get current retry count for a message."""
        with self.lock:
            return self.retry_counts.get(message_key, 0)

    def increment_retry(self, message_key: str) -> int:
        """Increment retry count and return new count."""
        with self.lock:
            count = self.retry_counts.get(message_key, 0) + 1
            self.retry_counts[message_key] = count
            return count

    def reset_retry(self, message_key: str):
        """Reset retry count for a message."""
        with self.lock:
            if message_key in self.retry_counts:
                del self.retry_counts[message_key]

    def should_send_to_dlq(self, message_key: str) -> bool:
        """Check if message should be sent to DLQ."""
        return self.get_retry_count(message_key) >= self.max_retries

    def cleanup_old_entries(self, max_age_seconds: int = 3600):
        """Remove entries older than max_age_seconds (not implemented - in-memory only)."""
        # In production, use Redis or similar for persistent retry tracking
        pass


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
        group_id: str = "control-plane-consumer",
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
        self.retry_tracker = MessageRetryTracker(max_retries=max_retries)
        self.running = False
        self.thread: Optional[threading.Thread] = None

        # Initialize DLQ producer if enabled
        if self.enable_dlq:
            self._init_dlq_producer()

    def _init_dlq_producer(self):
        """Initialize Dead Letter Queue producer."""
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

    def _send_to_dlq(self, message: dict, error: Exception, retry_count: int):
        """
        Send failed message to Dead Letter Queue.

        Args:
            message: Original message that failed
            error: Exception that caused the failure
            retry_count: Number of retry attempts
        """
        if not self.enable_dlq or not self.dlq_producer:
            logger.warning("DLQ not enabled or producer not initialized")
            return

        try:
            # Enrich message with error metadata
            dlq_message = {
                "original_message": message,
                "error": {
                    "type": type(error).__name__,
                    "message": str(error),
                    "timestamp": datetime.utcnow().isoformat(),
                },
                "retry_count": retry_count,
                "source_topic": self.topic,
                "consumer_group": self.group_id,
            }

            # Use integration_id as key if available (try top level first, then data field)
            integration_id = message.get("integration_id")
            if not integration_id and isinstance(message.get("data"), dict):
                integration_id = message.get("data", {}).get("integration_id")
            message_key = str(integration_id if integration_id else "unknown")

            # Send to DLQ
            future = self.dlq_producer.send(
                self.dlq_topic,
                key=message_key,
                value=dlq_message
            )
            future.get(timeout=10)  # Wait for send confirmation

            logger.warning(
                f"Message sent to DLQ: integration_id={message.get('integration_id')}, "
                f"error={type(error).__name__}, retries={retry_count}",
                extra={"dlq_message": dlq_message}
            )

            # Reset retry counter
            self.retry_tracker.reset_retry(message_key)

        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}", exc_info=True)

    def start(self) -> None:
        """Start the Kafka consumer in a background thread."""
        if self.running:
            logger.warning("Kafka consumer already running")
            return

        logger.info(f"Starting Kafka consumer for topic: {self.topic}")
        self.running = True
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

        logger.info("Kafka consumer stopped")

    def _consume_loop(self) -> None:
        """Main consumer loop running in background thread."""
        try:
            # Initialize Kafka consumer
            # Disable auto-commit when DLQ is enabled so we can control when to commit offsets
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=[self.bootstrap_servers],
                group_id=self.group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=not self.enable_dlq,  # Manual commit for DLQ
                value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m is not None else None,
                consumer_timeout_ms=1000,  # 1 second timeout for graceful shutdown
            )

            logger.info(f"Kafka consumer connected to {self.bootstrap_servers}")
            logger.info(f"Subscribed to topic: {self.topic}")

            # Consume messages
            while self.running:
                try:
                    # Poll for messages with timeout
                    messages = self.consumer.poll(timeout_ms=1000, max_records=10)

                    for topic_partition, records in messages.items():
                        for record in records:
                            try:
                                # Skip None values (tombstone messages from Debezium deletes)
                                if record.value is None:
                                    logger.debug("Skipping tombstone message (None value)")
                                    continue

                                # Generate message key for retry tracking
                                message = record.value
                                # Try to get integration_id from top level (Debezium) or from data field (legacy)
                                integration_id = message.get("integration_id")
                                if not integration_id and isinstance(message.get("data"), dict):
                                    integration_id = message.get("data", {}).get("integration_id")
                                message_key = str(integration_id if integration_id else f"offset_{record.offset}")

                                # Check if message should go to DLQ
                                if self.enable_dlq and self.retry_tracker.should_send_to_dlq(message_key):
                                    logger.warning(
                                        f"Message exceeded max retries, sending to DLQ: {message_key}"
                                    )
                                    self._send_to_dlq(
                                        message,
                                        Exception("Max retries exceeded"),
                                        self.retry_tracker.get_retry_count(message_key)
                                    )
                                    # Commit offset after sending to DLQ
                                    if self.enable_dlq:
                                        self.consumer.commit()
                                    continue

                                # Process message
                                self._process_message(message)

                                # Reset retry counter on success
                                if self.enable_dlq:
                                    self.retry_tracker.reset_retry(message_key)
                                    # Commit offset after successful processing
                                    self.consumer.commit()

                            except Exception as e:
                                # Handle processing error
                                message = record.value if record.value else {}
                                # Try to get integration_id from top level (Debezium) or from data field (legacy)
                                integration_id = message.get("integration_id")
                                if not integration_id and isinstance(message.get("data"), dict):
                                    integration_id = message.get("data", {}).get("integration_id")
                                message_key = str(integration_id if integration_id else f"offset_{record.offset}")

                                # Increment retry count
                                if self.enable_dlq:
                                    retry_count = self.retry_tracker.increment_retry(message_key)

                                    logger.error(
                                        f"Error processing message (attempt {retry_count}/{self.max_retries}): {e}",
                                        exc_info=True,
                                        extra={"kafka_message": message, "message_key": message_key},
                                    )

                                    # Check if we should send to DLQ
                                    if retry_count >= self.max_retries:
                                        self._send_to_dlq(message, e, retry_count)
                                        # Reset retry tracker and commit after DLQ
                                        self.retry_tracker.reset_retry(message_key)
                                        self.consumer.commit()
                                    # else: Don't commit - let message be redelivered for retry
                                else:
                                    logger.error(
                                        f"Error processing message: {e}",
                                        exc_info=True,
                                        extra={"kafka_message": message},
                                    )

                except Exception as e:
                    if self.running:
                        logger.error(f"Error in consumer loop: {e}", exc_info=True)

        except KafkaError as e:
            logger.error(f"Kafka error: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error in consumer: {e}", exc_info=True)
        finally:
            if self.consumer:
                try:
                    self.consumer.close()
                except Exception as e:
                    logger.error(f"Error closing consumer: {e}")

    def _process_message(self, message: dict) -> None:
        """
        Process a CDC event message.

        Supports two formats:
        1. Real Debezium CDC events (has __op field)
        2. Legacy manually-published events (has event_type field)

        Args:
            message: CDC event message
                Debezium format:
                {
                    "integration_id": 123,
                    "workspace_id": "ws-001",
                    "__op": "c",  # c=create, u=update, d=delete
                    "__source_ts_ms": 1234567890,
                    ...all database columns...
                }
                Legacy format:
                {
                    "event_type": "integration.created",
                    "event_id": "uuid",
                    "timestamp": "2026-02-04T12:00:00Z",
                    "data": {...}
                }
        """
        # Check if this is a real Debezium CDC event
        if "__op" in message:
            # Real Debezium CDC event
            operation = message.get("__op")
            integration_id = message.get("integration_id")

            logger.info(
                f"Processing Debezium CDC event: operation={operation}, integration_id={integration_id}",
                extra={"operation": operation, "integration_id": integration_id},
            )

            # Map Debezium operations to event types
            if operation == "c":  # Create
                event_type = "integration.created"
            elif operation == "u":  # Update
                event_type = "integration.updated"
            elif operation == "d":  # Delete
                event_type = "integration.deleted"
            else:
                logger.warning(f"Unknown Debezium operation: {operation}")
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
                },
            )
        else:
            logger.warning(f"Unknown message format: {list(message.keys())}")
            return

        # Call event handler if provided
        if self.event_handler:
            try:
                self.event_handler(event_type, data)
            except Exception as e:
                logger.error(
                    f"Error in event handler: {e}",
                    exc_info=True,
                    extra={"event_type": event_type, "data": data},
                )
                # Re-raise only if DLQ is enabled (for retry tracking)
                if self.enable_dlq:
                    raise
        else:
            # Default processing based on event type
            self._default_event_processing(event_type, data)

    def _default_event_processing(self, event_type: str, data: dict) -> None:
        """
        Default event processing logic.

        Args:
            event_type: Type of CDC event
            data: Event data
        """
        if event_type == "integration.created":
            integration_id = data.get('integration_id')
            logger.info(
                f"Integration created: {integration_id}",
                extra={"data": data},
            )

            # Trigger initial data sync if integration data is provided
            if self._should_trigger_integration(data):
                try:
                    self._trigger_integration_workflow(data)
                except Exception as e:
                    logger.error(
                        f"Failed to trigger workflow for integration {integration_id}: {e}",
                        exc_info=True,
                    )

        elif event_type == "integration.updated":
            logger.info(
                f"Integration updated: {data.get('integration_id')}",
                extra={"data": data},
            )
            # Configuration updates don't automatically trigger workflows

        elif event_type == "integration.deleted":
            logger.info(
                f"Integration deleted: {data.get('integration_id')}",
                extra={"data": data},
            )
            # Cleanup is handled by cascade delete in database

        elif event_type == "integration.run.started":
            logger.info(
                f"Integration run started: {data.get('run_id')}",
                extra={"data": data},
            )

        elif event_type == "integration.run.completed":
            logger.info(
                f"Integration run completed: {data.get('run_id')}",
                extra={"data": data},
            )

        elif event_type == "integration.run.failed":
            logger.error(
                f"Integration run failed: {data.get('run_id')}",
                extra={"data": data},
            )
            # TODO: Send alerts, retry logic, etc.

        else:
            logger.warning(f"Unknown event type: {event_type}", extra={"data": data})

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

    def _trigger_integration_workflow(self, data: dict) -> None:
        """
        Trigger Airflow DAG for integration.

        Args:
            data: Integration event data with workflow configuration
        """
        import asyncio
        import json as json_module
        from control_plane.app.services.integration_service import IntegrationService
        from control_plane.app.models.integration import Integration
        from control_plane.app.core.database import get_db
        from sqlalchemy import select

        integration_id = data.get("integration_id")
        logger.info(f"Triggering workflow for integration {integration_id}")

        async def _run_async_logic():
            # Get database session from async generator
            async for db in get_db():
                try:
                    service = IntegrationService(db)

                    # Extract execution config based on event type
                    execution_config = {}

                    if data.get("is_debezium_event"):
                        # For Debezium events, query database to get integration json_data
                        logger.info(f"Processing Debezium CDC event - querying database for integration {integration_id}")

                        # Use execute(select(...)) for async query
                        result = await db.execute(
                            select(Integration).where(Integration.integration_id == integration_id)
                        )
                        integration = result.scalars().first()

                        if not integration:
                            logger.error(f"Integration {integration_id} not found in database")
                            return

                        # Parse json_data to get execution config
                        if integration.json_data:
                            try:
                                execution_config = json_module.loads(integration.json_data)
                                logger.info(
                                    f"Extracted execution config from database: {list(execution_config.keys())}"
                                )
                            except json_module.JSONDecodeError as e:
                                logger.error(f"Failed to parse json_data for integration {integration_id}: {e}")
                                execution_config = {}
                        else:
                            logger.warning(f"Integration {integration_id} has no json_data")
                            execution_config = {}

                    else:
                        # For legacy events, extract from event data
                        if data.get("source_config"):
                            execution_config.update(data["source_config"])
                        if data.get("destination_config"):
                            execution_config.update(data["destination_config"])

                    # Trigger the integration DAG run (await the async method)
                    result = await service.trigger_dag_run(
                        integration_id=integration_id,
                        execution_config=execution_config if execution_config else None
                    )

                    logger.info(
                        f"✓ Workflow triggered successfully for integration {integration_id}",
                        extra={"dag_run_id": result.get("dag_run_id")},
                    )
                finally:
                    # Session is automatically closed by async context manager
                    pass
                
                # We only need one session
                break

        try:
            asyncio.run(_run_async_logic())
        except Exception as e:
            logger.error(
                f"Error triggering workflow: {e}",
                exc_info=True,
                extra={"integration_id": integration_id},
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
        group_id="control-plane-consumer",
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
