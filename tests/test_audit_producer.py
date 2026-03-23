"""Tests for the AuditProducer (ThreadedStrategy).

These tests verify that:
- emit() never raises (even with Kafka down, malformed data, full queue)
- NoOpProducer works as a drop-in replacement
- Event IDs and timestamps are auto-generated
- Queue bounded behaviour (drops events when full)
"""

import json
import time
import uuid
from unittest.mock import patch, MagicMock

import pytest

from shared_utils.audit_producer import (
    AuditProducer,
    _NoOpProducer,
    get_audit_producer,
)


class TestAuditProducer:
    """Test the threaded AuditProducer."""

    @patch("shared_utils.audit_producer.AuditProducer._create_kafka_producer")
    def test_emit_never_raises(self, mock_create):
        """emit() should never raise, even if Kafka fails."""
        mock_producer = MagicMock()
        mock_producer.send.side_effect = Exception("Kafka down")
        mock_create.return_value = mock_producer

        producer = AuditProducer("localhost:9092", queue_size=10)
        # Should not raise
        producer.emit(
            customer_guid="cust-001",
            event_type="test.event",
            actor_id="test",
            actor_type="system",
            resource_type="test",
            resource_id="1",
            action="test",
            outcome="success",
        )
        time.sleep(0.5)  # Let drain loop process
        stats = producer.stats
        assert stats["emitted"] == 1
        producer.close()

    @patch("shared_utils.audit_producer.AuditProducer._create_kafka_producer")
    def test_emit_generates_event_id_and_timestamp(self, mock_create):
        """emit() should auto-generate event_id (uuid4) and timestamp."""
        sent_events = []
        mock_producer = MagicMock()
        mock_producer.send.side_effect = lambda topic, key, value: sent_events.append(
            json.loads(value.decode("utf-8"))
        )
        mock_create.return_value = mock_producer

        producer = AuditProducer("localhost:9092", queue_size=10)
        producer.emit(
            customer_guid="cust-001",
            event_type="test.event",
            actor_id="test",
            actor_type="system",
            resource_type="test",
            resource_id="1",
            action="test",
            outcome="success",
        )
        time.sleep(0.5)
        producer.close()

        assert len(sent_events) == 1
        event = sent_events[0]
        # Validate event_id is a valid UUID
        uuid.UUID(event["event_id"])
        # Validate timestamp is ISO format
        assert "T" in event["timestamp"]
        assert event["customer_guid"] == "cust-001"
        assert event["event_type"] == "test.event"

    @patch("shared_utils.audit_producer.AuditProducer._create_kafka_producer")
    def test_emit_with_full_queue_drops_event(self, mock_create):
        """When queue is full, emit() should drop the event, not raise."""
        mock_create.return_value = MagicMock()

        producer = AuditProducer("localhost:9092", queue_size=100)
        # Directly mock the queue to simulate Full
        original_put = producer._queue.put_nowait
        call_count = [0]

        def mock_put_nowait(item):
            call_count[0] += 1
            if call_count[0] > 1:
                import queue as q
                raise q.Full()
            return original_put(item)

        producer._queue.put_nowait = mock_put_nowait

        # First emit succeeds
        producer.emit(
            customer_guid="cust-001", event_type="test.event",
            actor_id="test", actor_type="system",
            resource_type="test", resource_id="1",
            action="test", outcome="success",
        )
        # Second emit hits Full
        producer.emit(
            customer_guid="cust-001", event_type="test.event",
            actor_id="test", actor_type="system",
            resource_type="test", resource_id="2",
            action="test", outcome="success",
        )

        stats = producer.stats
        assert stats["emitted"] == 1
        assert stats["dropped"] == 1
        producer.close()

    @patch("shared_utils.audit_producer.AuditProducer._create_kafka_producer")
    def test_emit_serializes_before_after_state(self, mock_create):
        """before_state and after_state should be JSON-serialized."""
        sent_events = []
        mock_producer = MagicMock()
        mock_producer.send.side_effect = lambda topic, key, value: sent_events.append(
            json.loads(value.decode("utf-8"))
        )
        mock_create.return_value = mock_producer

        producer = AuditProducer("localhost:9092", queue_size=10)
        producer.emit(
            customer_guid="cust-001",
            event_type="integration.updated",
            actor_id="user@test.com",
            actor_type="user",
            resource_type="integration",
            resource_id="42",
            action="update",
            outcome="success",
            before_state={"schedule": "daily"},
            after_state={"schedule": "weekly"},
        )
        time.sleep(0.5)
        producer.close()

        assert len(sent_events) == 1
        event = sent_events[0]
        assert json.loads(event["before_state"]) == {"schedule": "daily"}
        assert json.loads(event["after_state"]) == {"schedule": "weekly"}

    @patch("shared_utils.audit_producer.AuditProducer._create_kafka_producer")
    def test_close_flushes_queue(self, mock_create):
        """close() should drain remaining events before stopping."""
        sent_events = []
        mock_producer = MagicMock()
        mock_producer.send.side_effect = lambda topic, key, value: sent_events.append(1)
        mock_create.return_value = mock_producer

        producer = AuditProducer("localhost:9092", queue_size=100)
        for i in range(5):
            producer.emit(
                customer_guid="cust-001",
                event_type="test.event",
                actor_id="test",
                actor_type="system",
                resource_type="test",
                resource_id=str(i),
                action="test",
                outcome="success",
            )
        producer.close(timeout=5.0)
        assert producer.stats["emitted"] == 5


class TestNoOpProducer:
    """Test the NoOp fallback producer."""

    def test_emit_does_nothing(self):
        """NoOpProducer.emit() should silently discard events."""
        producer = _NoOpProducer()
        # Should not raise
        producer.emit(customer_guid="test", event_type="test")

    def test_close_does_nothing(self):
        producer = _NoOpProducer()
        producer.close()

    def test_stats_returns_zeros(self):
        producer = _NoOpProducer()
        assert producer.stats == {"emitted": 0, "sent": 0, "dropped": 0, "errors": 0}


class TestGetAuditProducer:
    """Test the factory function."""

    def test_returns_noop_when_no_servers(self):
        """get_audit_producer('') should return NoOpProducer."""
        producer = get_audit_producer(bootstrap_servers="")
        assert isinstance(producer, _NoOpProducer)

    def test_returns_real_producer_when_servers_set(self):
        """get_audit_producer with servers should return AuditProducer."""
        producer = get_audit_producer(bootstrap_servers="localhost:9092")
        assert isinstance(producer, AuditProducer)
        producer.close()
