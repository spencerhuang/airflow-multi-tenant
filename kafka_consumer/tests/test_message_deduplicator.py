"""Tests for Redis-based two-phase message deduplication."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from kafka_consumer.app.services.message_deduplicator import (
    MessageDeduplicator,
    STATUS_PROCESSING,
    STATUS_COMPLETED,
)


@pytest.fixture
def deduplicator():
    """Create a MessageDeduplicator with mocked Redis."""
    dedup = MessageDeduplicator(ttl_seconds=86400, claim_ttl_seconds=420)
    dedup._redis_client = MagicMock()
    return dedup


@pytest.fixture
def mock_record():
    """Create a mock Kafka record."""
    record = Mock()
    record.partition = 0
    record.offset = 42
    return record


class TestBuildDedupKey:
    """Test dedup key construction for different message formats."""

    def test_debezium_event(self, deduplicator, mock_record):
        """Debezium CDC events use integration_id:source_ts:op."""
        message = {
            "__op": "c",
            "integration_id": 123,
            "__source_ts_ms": 1711234567890,
        }
        key = deduplicator.build_dedup_key("cdc.integration.events", mock_record, message)
        assert key == "dedup:kafka:cdc.integration.events:123:1711234567890:c"

    def test_debezium_event_missing_fields_falls_back(self, deduplicator, mock_record):
        """Debezium event without source_ts falls back to partition:offset."""
        message = {"__op": "c", "integration_id": 123}
        key = deduplicator.build_dedup_key("cdc.integration.events", mock_record, message)
        assert key == "dedup:kafka:cdc.integration.events:0:42"

    def test_legacy_event_with_event_id(self, deduplicator, mock_record):
        """Legacy events use event_id."""
        message = {
            "event_type": "integration.created",
            "event_id": "abc-123-def",
        }
        key = deduplicator.build_dedup_key("cdc.integration.events", mock_record, message)
        assert key == "dedup:kafka:cdc.integration.events:abc-123-def"

    def test_legacy_event_empty_event_id_falls_back(self, deduplicator, mock_record):
        """Legacy event with empty event_id falls back to partition:offset."""
        message = {"event_type": "integration.created", "event_id": ""}
        key = deduplicator.build_dedup_key("cdc.integration.events", mock_record, message)
        assert key == "dedup:kafka:cdc.integration.events:0:42"

    def test_fallback_partition_offset(self, deduplicator, mock_record):
        """Unknown message format falls back to partition:offset."""
        message = {"some_field": "value"}
        key = deduplicator.build_dedup_key("cdc.integration.events", mock_record, message)
        assert key == "dedup:kafka:cdc.integration.events:0:42"


class TestClaim:
    """Test two-phase claim (phase 1) for different scenarios."""

    def test_new_message(self, deduplicator):
        """SET NX succeeds — new message, returns 'new'."""
        deduplicator._redis_client.set.return_value = True
        result = deduplicator.claim("dedup:kafka:test:1:123:c")
        assert result == "new"
        deduplicator._redis_client.set.assert_called_once_with(
            "dedup:kafka:test:1:123:c", STATUS_PROCESSING, nx=True, ex=420,
        )

    def test_completed_message(self, deduplicator):
        """Key exists with value 'C' — true duplicate, returns 'completed'."""
        deduplicator._redis_client.set.return_value = None  # NX failed
        deduplicator._redis_client.get.return_value = b"C"
        result = deduplicator.claim("dedup:kafka:test:1:123:c")
        assert result == "completed"

    def test_processing_message_from_crashed_attempt(self, deduplicator):
        """Key exists with value 'P' — previous crash, returns 'processing'."""
        deduplicator._redis_client.set.return_value = None  # NX failed
        deduplicator._redis_client.get.return_value = b"P"
        result = deduplicator.claim("dedup:kafka:test:1:123:c")
        assert result == "processing"
        # Should reset the lease TTL
        assert deduplicator._redis_client.set.call_count == 2
        deduplicator._redis_client.set.assert_called_with(
            "dedup:kafka:test:1:123:c", STATUS_PROCESSING, ex=420,
        )

    def test_key_expired_between_nx_and_get(self, deduplicator):
        """Race condition: key expires between SET NX and GET — treat as new."""
        deduplicator._redis_client.set.return_value = None  # NX failed
        deduplicator._redis_client.get.return_value = None  # expired
        result = deduplicator.claim("dedup:kafka:test:1:123:c")
        assert result == "new"

    def test_redis_failure_returns_unknown(self, deduplicator):
        """Redis failure returns 'unknown' (fail-open)."""
        import redis as redis_lib
        deduplicator._redis_client.set.side_effect = redis_lib.RedisError("connection lost")
        result = deduplicator.claim("dedup:kafka:test:1:123:c")
        assert result == "unknown"

    def test_redis_unavailable_returns_unknown(self):
        """When Redis client can't be initialized, returns 'unknown' (fail-open)."""
        dedup = MessageDeduplicator()
        with patch(
            "kafka_consumer.app.services.message_deduplicator.get_redis_client",
            side_effect=Exception("Redis down"),
            create=True,
        ):
            dedup._redis_client = None
            assert dedup.claim("some-key") == "unknown"


class TestConfirm:
    """Test phase 2 — marking message as completed."""

    def test_confirms_with_long_ttl(self, deduplicator):
        """Confirm overwrites key with 'C' and long TTL."""
        deduplicator.confirm("dedup:kafka:test:1:123:c")
        deduplicator._redis_client.set.assert_called_once_with(
            "dedup:kafka:test:1:123:c", STATUS_COMPLETED, ex=86400,
        )

    def test_redis_failure_does_not_raise(self, deduplicator):
        """Redis failure during confirm is swallowed."""
        import redis as redis_lib
        deduplicator._redis_client.set.side_effect = redis_lib.RedisError("connection lost")
        deduplicator.confirm("dedup:kafka:test:1:123:c")  # Should not raise

    def test_no_client_does_not_raise(self):
        """When Redis client is None, confirm is a no-op."""
        dedup = MessageDeduplicator()
        dedup._redis_client = None
        with patch(
            "kafka_consumer.app.services.message_deduplicator.get_redis_client",
            side_effect=Exception("Redis down"),
            create=True,
        ):
            dedup.confirm("some-key")  # Should not raise


class TestRemoveDedupKey:
    """Test dedup key removal (DLQ cleanup)."""

    def test_removes_key(self, deduplicator):
        """Successful key removal calls DELETE."""
        deduplicator.remove_dedup_key("dedup:kafka:test:1:123:c")
        deduplicator._redis_client.delete.assert_called_once_with("dedup:kafka:test:1:123:c")

    def test_redis_failure_does_not_raise(self, deduplicator):
        """Redis failure during removal is swallowed."""
        import redis as redis_lib
        deduplicator._redis_client.delete.side_effect = redis_lib.RedisError("connection lost")
        deduplicator.remove_dedup_key("dedup:kafka:test:1:123:c")

    def test_no_client_does_not_raise(self):
        """When Redis client is None, removal is a no-op."""
        dedup = MessageDeduplicator()
        dedup._redis_client = None
        with patch(
            "kafka_consumer.app.services.message_deduplicator.get_redis_client",
            side_effect=Exception("Redis down"),
            create=True,
        ):
            dedup.remove_dedup_key("some-key")  # Should not raise
