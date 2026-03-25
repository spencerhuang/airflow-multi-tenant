"""Tests for Redis-based message deduplication."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from kafka_consumer.app.services.message_deduplicator import MessageDeduplicator


@pytest.fixture
def deduplicator():
    """Create a MessageDeduplicator with mocked Redis."""
    dedup = MessageDeduplicator(ttl_seconds=86400)
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


class TestIsDuplicate:
    """Test duplicate detection via Redis SET NX."""

    def test_new_message(self, deduplicator):
        """SET NX returns True for new keys — message is NOT a duplicate."""
        deduplicator._redis_client.set.return_value = True
        assert deduplicator.is_duplicate("dedup:kafka:test:1:123:c") is False
        deduplicator._redis_client.set.assert_called_once_with(
            "dedup:kafka:test:1:123:c", "1", nx=True, ex=86400
        )

    def test_existing_message(self, deduplicator):
        """SET NX returns None for existing keys — message IS a duplicate."""
        deduplicator._redis_client.set.return_value = None
        assert deduplicator.is_duplicate("dedup:kafka:test:1:123:c") is True

    def test_redis_failure_returns_false(self, deduplicator):
        """Redis failure returns False (fail-open — proceed with processing)."""
        import redis as redis_lib
        deduplicator._redis_client.set.side_effect = redis_lib.RedisError("connection lost")
        assert deduplicator.is_duplicate("dedup:kafka:test:1:123:c") is False

    def test_redis_unavailable_returns_false(self):
        """When Redis client can't be initialized, returns False (fail-open)."""
        dedup = MessageDeduplicator()
        with patch(
            "kafka_consumer.app.services.message_deduplicator.get_redis_client",
            side_effect=Exception("Redis down"),
            create=True,
        ):
            # Force re-init by clearing cached client
            dedup._redis_client = None
            assert dedup.is_duplicate("some-key") is False


class TestRemoveDedupKey:
    """Test dedup key removal on processing failure."""

    def test_removes_key(self, deduplicator):
        """Successful key removal calls DELETE."""
        deduplicator.remove_dedup_key("dedup:kafka:test:1:123:c")
        deduplicator._redis_client.delete.assert_called_once_with("dedup:kafka:test:1:123:c")

    def test_redis_failure_does_not_raise(self, deduplicator):
        """Redis failure during removal is swallowed (logged, not raised)."""
        import redis as redis_lib
        deduplicator._redis_client.delete.side_effect = redis_lib.RedisError("connection lost")
        # Should not raise
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
