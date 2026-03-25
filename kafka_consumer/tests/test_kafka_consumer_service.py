"""Tests for Kafka consumer service."""

import json
import time
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

from kafka_consumer.app.services.kafka_consumer_service import (
    KafkaConsumerService,
    initialize_kafka_consumer,
    shutdown_kafka_consumer,
    get_kafka_consumer_service,
)


# Kafka connection settings
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "test.cdc.integration.events"


@pytest.fixture(scope="session")
def wait_for_kafka():
    """Wait for Kafka to be ready."""
    from kafka.admin import KafkaAdminClient

    max_retries = 30
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                request_timeout_ms=5000,
            )
            admin_client.close()
            print(f"✓ Kafka is ready at {KAFKA_BOOTSTRAP_SERVERS}")
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Waiting for Kafka... ({attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                pytest.skip(f"Kafka not available after {max_retries} retries: {e}")


@pytest.fixture
def kafka_producer(wait_for_kafka):
    """Create Kafka producer for testing."""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=5000,
    )
    yield producer
    producer.close()


class TestKafkaConsumerService:
    """Test suite for KafkaConsumerService."""

    def test_consumer_initialization(self):
        """Test consumer service initialization."""
        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id="test-group-init",
        )

        assert consumer.bootstrap_servers == KAFKA_BOOTSTRAP_SERVERS
        assert consumer.topic == KAFKA_TOPIC
        assert consumer.group_id == "test-group-init"
        assert consumer.running is False
        assert consumer.consumer is None

    def test_consumer_start_stop(self):
        """Test consumer service start and stop."""
        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id="test-group-start-stop",
        )

        # Start consumer
        consumer.start()
        time.sleep(2)  # Give it time to connect

        assert consumer.running is True
        assert consumer.thread is not None
        assert consumer.thread.is_alive()

        # Stop consumer
        consumer.stop()
        time.sleep(1)

        assert consumer.running is False

    def test_process_message_integration_created(self):
        """Test processing integration.created event."""
        events_processed = []

        def test_handler(event_type, data):
            events_processed.append((event_type, data))

        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id="test-group-created",
            event_handler=test_handler,
        )

        message = {
            "event_type": "integration.created",
            "event_id": "test-123",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {
                "integration_id": 1,
                "tenant_id": "customer_abc",
                "workflow_type": "s3_to_mongo",
            },
        }

        consumer._process_message(message)

        assert len(events_processed) == 1
        assert events_processed[0][0] == "integration.created"
        assert events_processed[0][1]["integration_id"] == 1

    def test_process_message_integration_updated(self):
        """Test processing integration.updated event."""
        events_processed = []

        def test_handler(event_type, data):
            events_processed.append((event_type, data))

        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id="test-group-updated",
            event_handler=test_handler,
        )

        message = {
            "event_type": "integration.updated",
            "event_id": "test-456",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {
                "integration_id": 1,
                "tenant_id": "customer_abc",
            },
        }

        consumer._process_message(message)

        assert len(events_processed) == 1
        assert events_processed[0][0] == "integration.updated"

    def test_process_message_integration_deleted(self):
        """Test processing integration.deleted event."""
        events_processed = []

        def test_handler(event_type, data):
            events_processed.append((event_type, data))

        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id="test-group-deleted",
            event_handler=test_handler,
        )

        message = {
            "event_type": "integration.deleted",
            "event_id": "test-789",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {
                "integration_id": 1,
                "tenant_id": "customer_abc",
            },
        }

        consumer._process_message(message)

        assert len(events_processed) == 1
        assert events_processed[0][0] == "integration.deleted"

    def test_process_message_run_events(self):
        """Test processing integration run events."""
        events_processed = []

        def test_handler(event_type, data):
            events_processed.append(event_type)

        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id="test-group-run-events",
            event_handler=test_handler,
        )

        # Test run started
        consumer._process_message({
            "event_type": "integration.run.started",
            "event_id": "run-1",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {"run_id": "run-1", "integration_id": 1},
        })

        # Test run completed
        consumer._process_message({
            "event_type": "integration.run.completed",
            "event_id": "run-2",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {"run_id": "run-2", "integration_id": 1},
        })

        # Test run failed
        consumer._process_message({
            "event_type": "integration.run.failed",
            "event_id": "run-3",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {"run_id": "run-3", "integration_id": 1},
        })

        assert len(events_processed) == 3
        assert "integration.run.started" in events_processed
        assert "integration.run.completed" in events_processed
        assert "integration.run.failed" in events_processed

    def test_process_message_unknown_event(self):
        """Test processing unknown event type."""
        events_processed = []

        def test_handler(event_type, data):
            events_processed.append(event_type)

        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id="test-group-unknown",
            event_handler=test_handler,
        )

        message = {
            "event_type": "unknown.event.type",
            "event_id": "unknown-1",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {},
        }

        # Should not raise exception
        consumer._process_message(message)

        assert len(events_processed) == 1

    def test_process_message_error_handling(self):
        """Test error handling in message processing."""
        def error_handler(event_type, data):
            raise ValueError("Test error")

        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id="test-group-error",
            event_handler=error_handler,
            enable_dlq=False,  # Disable DLQ so errors are caught and not re-raised
        )

        message = {
            "event_type": "integration.created",
            "event_id": "error-1",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {"integration_id": 1},
        }

        # Should not raise exception - error should be caught and logged
        consumer._process_message(message)

    def test_consumer_with_custom_handler(self):
        """Test consumer with custom event handler."""
        processed_events = []

        def custom_handler(event_type, data):
            processed_events.append({
                "type": event_type,
                "integration_id": data.get("integration_id"),
            })

        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id="test-group-custom",
            event_handler=custom_handler,
        )

        message = {
            "event_type": "integration.created",
            "event_id": "custom-1",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {"integration_id": 999},
        }

        consumer._process_message(message)

        assert len(processed_events) == 1
        assert processed_events[0]["type"] == "integration.created"
        assert processed_events[0]["integration_id"] == 999


class TestKafkaConsumerIntegration:
    """Integration tests with real Kafka."""

    def test_consumer_receives_published_message(self, kafka_producer):
        """Test that consumer receives messages published to Kafka."""
        received_messages = []

        def message_handler(event_type, data):
            received_messages.append((event_type, data))

        # Create and start consumer
        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id=f"test-group-integration-{int(time.time())}",
            event_handler=message_handler,
        )

        consumer.start()
        time.sleep(3)  # Give consumer time to connect and subscribe

        # Publish a test message
        test_message = {
            "event_type": "integration.created",
            "event_id": f"test-{int(time.time())}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {
                "integration_id": 123,
                "tenant_id": "test_tenant",
                "workflow_type": "s3_to_mongo",
            },
        }

        kafka_producer.send(KAFKA_TOPIC, test_message)
        kafka_producer.flush()

        # Wait for message to be consumed
        time.sleep(5)

        # Stop consumer
        consumer.stop()

        # Verify message was received
        assert len(received_messages) > 0
        assert any(msg[0] == "integration.created" for msg in received_messages)

    def test_multiple_messages_in_sequence(self, kafka_producer):
        """Test processing multiple messages in sequence."""
        received_messages = []

        def message_handler(event_type, data):
            received_messages.append(event_type)

        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id=f"test-group-sequence-{int(time.time())}",
            event_handler=message_handler,
        )

        consumer.start()
        time.sleep(3)

        # Publish multiple messages
        messages = [
            {
                "event_type": "integration.created",
                "event_id": f"seq-1-{int(time.time())}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": {"integration_id": 1},
            },
            {
                "event_type": "integration.updated",
                "event_id": f"seq-2-{int(time.time())}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": {"integration_id": 1},
            },
            {
                "event_type": "integration.run.started",
                "event_id": f"seq-3-{int(time.time())}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": {"run_id": "run-1", "integration_id": 1},
            },
        ]

        for msg in messages:
            kafka_producer.send(KAFKA_TOPIC, msg)

        kafka_producer.flush()
        time.sleep(5)

        consumer.stop()

        # Should have received all 3 messages (at least)
        assert len(received_messages) >= 3


class TestConsumerServiceGlobals:
    """Test global consumer service functions."""

    def test_initialize_and_shutdown(self):
        """Test initialize and shutdown functions."""
        # Should start with no global consumer
        assert get_kafka_consumer_service() is None

        # Initialize consumer
        with patch("kafka_consumer.app.services.kafka_consumer_service.settings") as mock_settings:
            mock_settings.KAFKA_BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP_SERVERS

            initialize_kafka_consumer()
            time.sleep(2)

            # Should now have global consumer
            consumer = get_kafka_consumer_service()
            assert consumer is not None
            assert consumer.running is True

            # Shutdown consumer
            shutdown_kafka_consumer()
            time.sleep(1)

            # Should be gone
            assert get_kafka_consumer_service() is None

    def test_initialize_twice_warning(self):
        """Test that initializing twice logs a warning."""
        with patch("kafka_consumer.app.services.kafka_consumer_service.settings") as mock_settings:
            mock_settings.KAFKA_BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP_SERVERS

            # First initialization
            initialize_kafka_consumer()
            time.sleep(1)

            # Second initialization should log warning but not crash
            initialize_kafka_consumer()

            # Cleanup
            shutdown_kafka_consumer()
            time.sleep(1)

    def test_shutdown_when_not_initialized(self):
        """Test that shutdown works even if consumer not initialized."""
        # Should not raise exception
        shutdown_kafka_consumer()


class TestKafkaConsumerDLQ:
    """Test suite for Dead Letter Queue functionality."""

    def test_dlq_initialization(self):
        """Test DLQ-enabled consumer initialization."""
        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id="test-dlq-init",
            dlq_topic="test.dlq",
            max_retries=3,
            enable_dlq=True,
        )

        assert consumer.dlq_topic == "test.dlq"
        assert consumer.max_retries == 3
        assert consumer.enable_dlq is True

    def test_dlq_disabled(self):
        """Test consumer with DLQ disabled."""
        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id="test-dlq-disabled",
            enable_dlq=False,
        )

        assert consumer.enable_dlq is False

    def test_message_sent_to_dlq_after_max_retries(self, kafka_producer):
        """Test that message is sent to DLQ after max retries (transient errors)."""
        dlq_topic = f"test.dlq.{int(time.time())}"
        received_messages = []

        # Handler that always fails with a transient error (triggers retry)
        def failing_handler(event_type, data):
            received_messages.append(event_type)
            raise OSError("Simulated transient error")

        # Create main consumer with DLQ enabled
        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id=f"test-dlq-maxretries-{int(time.time())}",
            event_handler=failing_handler,
            dlq_topic=dlq_topic,
            max_retries=2,  # 2 retries + 1 initial = 3 total attempts
            enable_dlq=True,
        )

        consumer.start()
        time.sleep(3)

        # Publish a message that will fail processing
        test_message = {
            "event_type": "integration.created",
            "event_id": f"dlq-test-{int(time.time())}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {"integration_id": 999},
        }

        kafka_producer.send(KAFKA_TOPIC, test_message)
        kafka_producer.flush()

        # Wait for retries (backoff: 1s + 5s) and DLQ send
        time.sleep(15)

        consumer.stop()

        # Create DLQ consumer AFTER messages have been sent to ensure topic exists
        from kafka import KafkaConsumer
        dlq_consumer = KafkaConsumer(
            dlq_topic,
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
            auto_offset_reset="earliest",
            group_id=f"dlq-test-consumer-{int(time.time())}",
            consumer_timeout_ms=5000,  # Increased timeout
        )

        # Check DLQ topic for message
        dlq_messages = []
        for msg in dlq_consumer:
            dlq_messages.append(msg.value)

        dlq_consumer.close()

        # Verify message was sent to DLQ
        assert len(dlq_messages) > 0, f"Expected DLQ messages but found none. Check logs for 'Message sent to DLQ'"
        dlq_msg = dlq_messages[0]
        assert "original_message" in dlq_msg
        assert "error" in dlq_msg
        assert "retry_count" in dlq_msg
        assert dlq_msg["retry_count"] == 3  # max_retries(2) + 1 initial attempt
        assert dlq_msg["source_topic"] == KAFKA_TOPIC

    def test_dlq_message_format(self):
        """Test DLQ message contains proper error metadata."""
        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id="test-dlq-format",
            enable_dlq=True,
        )

        test_message = {
            "event_type": "integration.created",
            "event_id": "format-test",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {"integration_id": 456},
        }

        test_error = ValueError("Test error message")

        # Mock the DLQ producer to capture sent messages
        sent_messages = []
        mock_producer = MagicMock()

        def mock_send(topic, key, value):
            sent_messages.append(value)
            return MagicMock()

        mock_producer.send = mock_send
        consumer.dlq_producer = mock_producer

        # Send to DLQ
        consumer._send_to_dlq(test_message, test_error, 3)

        # Verify message format
        assert len(sent_messages) == 1
        dlq_msg = sent_messages[0]

        assert "original_message" in dlq_msg
        assert dlq_msg["original_message"] == test_message

        assert "error" in dlq_msg
        assert dlq_msg["error"]["type"] == "ValueError"
        assert dlq_msg["error"]["message"] == "Test error message"
        assert "timestamp" in dlq_msg["error"]

        assert dlq_msg["retry_count"] == 3
        assert dlq_msg["source_topic"] == KAFKA_TOPIC
        assert dlq_msg["consumer_group"] == "test-dlq-format"

    def test_consumer_continues_after_dlq(self, kafka_producer):
        """Test that consumer continues processing after sending message to DLQ."""
        received_messages = []
        fail_integration_id = 999
        ts = int(time.time())

        def selective_failing_handler(event_type, data):
            received_messages.append(data.get("integration_id"))
            if data.get("integration_id") == fail_integration_id:
                raise ValueError("This message should go to DLQ")

        consumer = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            group_id=f"test-dlq-continue-{ts}",
            event_handler=selective_failing_handler,
            max_retries=2,
            enable_dlq=True,
        )

        # Publish messages BEFORE starting consumer to ensure they're available
        messages = [
            {
                "event_type": "integration.created",
                "event_id": f"good-1-{ts}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": {"integration_id": 1},
            },
            {
                "event_type": "integration.created",
                "event_id": f"bad-{ts}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": {"integration_id": fail_integration_id},
            },
            {
                "event_type": "integration.created",
                "event_id": f"good-2-{ts}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": {"integration_id": 2},
            },
        ]

        for msg in messages:
            kafka_producer.send(KAFKA_TOPIC, msg)
        kafka_producer.flush()

        # Start consumer after messages are published
        consumer.start()
        time.sleep(10)  # Wait for processing

        consumer.stop()

        # Consumer should have attempted to process all messages
        # The bad message (ValueError) goes to DLQ immediately (permanent error)
        assert 1 in received_messages
        assert 2 in received_messages
        assert fail_integration_id in received_messages


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
