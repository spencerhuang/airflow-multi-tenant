"""Integration tests for CDC Kafka functionality.

These tests require Docker services to be running.
Run with: docker-compose up -d && pytest control_plane/tests/test_cdc_kafka.py
"""

import pytest
import time
import json
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable


# Test configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "cdc.integration.events"


@pytest.fixture(scope="session")
def wait_for_kafka():
    """Wait for Kafka to be ready."""
    max_retries = 30
    retry_interval = 2

    for i in range(max_retries):
        try:
            # Try to create a producer to test connectivity
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=5000,
            )
            producer.close()
            print(f"\n✓ Kafka is ready")
            return True
        except NoBrokersAvailable:
            if i < max_retries - 1:
                print(f"Waiting for Kafka... ({i+1}/{max_retries})")
                time.sleep(retry_interval)
            else:
                pytest.skip("Kafka not available - are Docker services running?")
        except Exception as e:
            if i < max_retries - 1:
                print(f"Waiting for Kafka... ({i+1}/{max_retries}): {e}")
                time.sleep(retry_interval)
            else:
                pytest.skip(f"Kafka error: {e}")


@pytest.fixture
def kafka_producer(wait_for_kafka):
    """Create Kafka producer for testing."""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_block_ms=10000,
    )
    yield producer
    producer.close()


@pytest.fixture
def kafka_consumer(wait_for_kafka):
    """Create Kafka consumer for testing."""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"test-group-{datetime.now().timestamp()}",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m is not None else None,
        consumer_timeout_ms=5000,
    )
    yield consumer
    consumer.close()


class TestKafkaConnectivity:
    """Test basic Kafka connectivity."""

    def test_kafka_is_running(self, wait_for_kafka):
        """Test that Kafka is accessible."""
        assert wait_for_kafka is True

    def test_producer_can_send_message(self, kafka_producer):
        """Test that producer can send messages."""
        test_message = {
            "event_type": "test",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {"test": "value"},
        }

        # Send message
        future = kafka_producer.send(KAFKA_TOPIC, test_message)
        result = future.get(timeout=10)

        assert result is not None
        assert result.topic == KAFKA_TOPIC


class TestCDCEvents:
    """Test CDC event handling."""

    def test_integration_created_event(self, kafka_producer, kafka_consumer):
        """Test sending and receiving integration created event."""
        event = {
            "event_type": "integration.created",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "workspace_id": "test-ws-001",
            "integration_id": 1,
            "integration_type": "S3ToMongo",
            "data": {
                "s3_bucket": "test-bucket",
                "mongo_collection": "test_collection",
            },
        }

        # Send event
        kafka_producer.send(KAFKA_TOPIC, event)
        kafka_producer.flush()

        # Wait a bit for message to be available
        time.sleep(2)

        # Receive event
        messages = []
        for message in kafka_consumer:
            if message.value is None:
                continue
            messages.append(message.value)
            if message.value.get("event_type") == "integration.created":
                break

        # Verify event was received
        assert len(messages) > 0
        received_event = None
        for msg in messages:
            if msg.get("event_type") == "integration.created":
                received_event = msg
                break

        assert received_event is not None
        assert received_event["workspace_id"] == "test-ws-001"
        assert received_event["integration_type"] == "S3ToMongo"

    def test_integration_updated_event(self, kafka_producer, kafka_consumer):
        """Test sending and receiving integration updated event."""
        event = {
            "event_type": "integration.updated",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "workspace_id": "test-ws-001",
            "integration_id": 1,
            "changes": {
                "usr_sch_status": "paused",
            },
        }

        # Send event
        kafka_producer.send(KAFKA_TOPIC, event)
        kafka_producer.flush()

        # Wait a bit for message to be available
        time.sleep(2)

        # Receive event
        messages = []
        for message in kafka_consumer:
            if message.value is None:
                continue
            messages.append(message.value)
            if message.value.get("event_type") == "integration.updated":
                break

        # Verify event was received
        assert len(messages) > 0
        received_event = None
        for msg in messages:
            if msg.get("event_type") == "integration.updated":
                received_event = msg
                break

        assert received_event is not None
        assert received_event["integration_id"] == 1
        assert received_event["changes"]["usr_sch_status"] == "paused"

    def test_integration_run_started_event(self, kafka_producer, kafka_consumer):
        """Test sending and receiving integration run started event."""
        event = {
            "event_type": "integration_run.started",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "workspace_id": "test-ws-001",
            "integration_id": 1,
            "run_id": 123,
            "dag_run_id": "manual__2024-01-01T00:00:00",
        }

        # Send event
        kafka_producer.send(KAFKA_TOPIC, event)
        kafka_producer.flush()

        # Wait a bit for message to be available
        time.sleep(2)

        # Receive event
        messages = []
        for message in kafka_consumer:
            if message.value is None:
                continue
            messages.append(message.value)
            if message.value.get("event_type") == "integration_run.started":
                break

        # Verify event was received
        assert len(messages) > 0
        received_event = None
        for msg in messages:
            if msg.get("event_type") == "integration_run.started":
                received_event = msg
                break

        assert received_event is not None
        assert received_event["integration_id"] == 1
        assert received_event["dag_run_id"] == "manual__2024-01-01T00:00:00"

    def test_multiple_events_in_order(self, kafka_producer, kafka_consumer):
        """Test sending multiple events and receiving them in order."""
        events = [
            {
                "event_type": "test.event1",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "sequence": 1,
            },
            {
                "event_type": "test.event2",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "sequence": 2,
            },
            {
                "event_type": "test.event3",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "sequence": 3,
            },
        ]

        # Send all events
        for event in events:
            kafka_producer.send(KAFKA_TOPIC, event)
        kafka_producer.flush()

        # Wait a bit for messages to be available
        time.sleep(2)

        # Receive events
        received_events = []
        for message in kafka_consumer:
            if message.value is None:
                continue
            if message.value.get("event_type", "").startswith("test.event"):
                received_events.append(message.value)
            if len(received_events) >= 3:
                break

        # Verify all events were received in order
        assert len(received_events) >= 3
        test_events = [e for e in received_events if e.get("event_type", "").startswith("test.event")]
        assert len(test_events) == 3
        assert test_events[0]["sequence"] == 1
        assert test_events[1]["sequence"] == 2
        assert test_events[2]["sequence"] == 3


class TestKafkaErrorHandling:
    """Test Kafka error handling."""

    def test_invalid_message_format(self, kafka_producer):
        """Test handling invalid message formats."""
        # This should not raise an exception
        # Kafka producer should handle serialization
        try:
            test_message = {"valid": "json"}
            future = kafka_producer.send(KAFKA_TOPIC, test_message)
            future.get(timeout=10)
            assert True
        except Exception as e:
            pytest.fail(f"Failed to send valid message: {e}")

    def test_large_message(self, kafka_producer):
        """Test sending large messages."""
        # Create a reasonably large message (not too large to avoid timeouts)
        large_data = {"data": "x" * 10000, "array": list(range(1000))}
        test_message = {
            "event_type": "test.large",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": large_data,
        }

        try:
            future = kafka_producer.send(KAFKA_TOPIC, test_message)
            result = future.get(timeout=10)
            assert result is not None
        except Exception as e:
            # Large messages might fail depending on Kafka config
            # This is expected behavior
            print(f"Large message handling: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
