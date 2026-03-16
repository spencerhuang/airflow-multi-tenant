"""Tests for DLQ repository (database persistence layer).

Uses an in-memory SQLite database to test all CRUD operations without
requiring a running MySQL instance.
"""

import json
import time
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from sqlalchemy import create_engine

from shared_models.tables import metadata, dead_letter_messages
from kafka_consumer.app.services import dlq_repository


@pytest.fixture(autouse=True)
def setup_sqlite_engine():
    """Replace the repository engine with an in-memory SQLite database."""
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)

    # Patch the module-level engine
    dlq_repository._engine = engine
    yield engine
    dlq_repository._engine = None


@pytest.fixture
def sample_message():
    """Return a sample Kafka message dict."""
    return {
        "event_type": "integration.created",
        "event_id": "test-123",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": {"integration_id": 42, "tenant_id": "ws-001"},
    }


@pytest.fixture
def sample_error():
    """Return a sample exception."""
    return ValueError("Simulated processing error")


class TestPersistDlqMessage:
    """Tests for persist_dlq_message."""

    def test_persist_basic(self, sample_message, sample_error):
        dlq_id = dlq_repository.persist_dlq_message(
            original_message=sample_message,
            error=sample_error,
            retry_count=3,
            source_topic="cdc.integration.events",
            consumer_group="cdc-consumer",
            message_key="42",
        )
        assert dlq_id is not None
        assert dlq_id > 0

    def test_persist_stores_correct_data(self, sample_message, sample_error):
        dlq_id = dlq_repository.persist_dlq_message(
            original_message=sample_message,
            error=sample_error,
            retry_count=3,
            source_topic="cdc.integration.events",
            consumer_group="cdc-consumer",
            message_key="42",
        )

        entry = dlq_repository.get_dlq_message(dlq_id)
        assert entry is not None
        assert entry["status"] == "pending"
        assert entry["error_type"] == "ValueError"
        assert entry["error_message"] == "Simulated processing error"
        assert entry["retry_count"] == 3
        assert entry["source_topic"] == "cdc.integration.events"
        assert entry["consumer_group"] == "cdc-consumer"
        assert entry["message_key"] == "42"
        # original_message should be parsed back to dict
        assert entry["original_message"]["event_type"] == "integration.created"

    def test_persist_extracts_integration_id_from_top_level(self):
        message = {"integration_id": 99, "__op": "c"}
        dlq_id = dlq_repository.persist_dlq_message(
            original_message=message,
            error=Exception("test"),
            retry_count=1,
            source_topic="topic",
            consumer_group="group",
        )
        entry = dlq_repository.get_dlq_message(dlq_id)
        assert entry["integration_id"] == 99

    def test_persist_extracts_integration_id_from_data_field(self):
        message = {"event_type": "integration.created", "data": {"integration_id": 77}}
        dlq_id = dlq_repository.persist_dlq_message(
            original_message=message,
            error=Exception("test"),
            retry_count=1,
            source_topic="topic",
            consumer_group="group",
        )
        entry = dlq_repository.get_dlq_message(dlq_id)
        assert entry["integration_id"] == 77

    def test_persist_handles_missing_integration_id(self):
        message = {"event_type": "unknown", "data": {}}
        dlq_id = dlq_repository.persist_dlq_message(
            original_message=message,
            error=Exception("test"),
            retry_count=1,
            source_topic="topic",
            consumer_group="group",
        )
        entry = dlq_repository.get_dlq_message(dlq_id)
        assert entry["integration_id"] is None

    def test_persist_handles_invalid_integration_id(self):
        message = {"integration_id": "not-a-number"}
        dlq_id = dlq_repository.persist_dlq_message(
            original_message=message,
            error=Exception("test"),
            retry_count=1,
            source_topic="topic",
            consumer_group="group",
        )
        entry = dlq_repository.get_dlq_message(dlq_id)
        assert entry["integration_id"] is None


class TestListDlqMessages:
    """Tests for list_dlq_messages."""

    def _insert_entries(self, count=5):
        """Helper to insert multiple DLQ entries."""
        ids = []
        for i in range(count):
            dlq_id = dlq_repository.persist_dlq_message(
                original_message={"integration_id": i, "event_type": "test"},
                error=ValueError(f"Error {i}"),
                retry_count=i,
                source_topic="topic",
                consumer_group="group",
                message_key=str(i),
            )
            ids.append(dlq_id)
        return ids

    def test_list_returns_all(self):
        self._insert_entries(3)
        entries = dlq_repository.list_dlq_messages()
        assert len(entries) == 3

    def test_list_filter_by_status(self):
        ids = self._insert_entries(3)
        # Resolve one
        dlq_repository.update_dlq_status(ids[0], "resolved")

        pending = dlq_repository.list_dlq_messages(status="pending")
        assert len(pending) == 2

        resolved = dlq_repository.list_dlq_messages(status="resolved")
        assert len(resolved) == 1

    def test_list_filter_by_integration_id(self):
        self._insert_entries(5)
        entries = dlq_repository.list_dlq_messages(integration_id=2)
        assert len(entries) == 1
        assert entries[0]["integration_id"] == 2

    def test_list_pagination(self):
        self._insert_entries(5)
        page1 = dlq_repository.list_dlq_messages(limit=2, offset=0)
        page2 = dlq_repository.list_dlq_messages(limit=2, offset=2)
        assert len(page1) == 2
        assert len(page2) == 2
        # Pages should not overlap
        page1_ids = {e["dlq_id"] for e in page1}
        page2_ids = {e["dlq_id"] for e in page2}
        assert page1_ids.isdisjoint(page2_ids)

    def test_list_ordered_by_created_at_desc(self):
        self._insert_entries(3)
        entries = dlq_repository.list_dlq_messages()
        # Most recent first
        for i in range(len(entries) - 1):
            assert entries[i]["created_at"] >= entries[i + 1]["created_at"]


class TestGetDlqMessage:
    """Tests for get_dlq_message."""

    def test_get_existing(self, sample_message, sample_error):
        dlq_id = dlq_repository.persist_dlq_message(
            original_message=sample_message,
            error=sample_error,
            retry_count=3,
            source_topic="topic",
            consumer_group="group",
        )
        entry = dlq_repository.get_dlq_message(dlq_id)
        assert entry is not None
        assert entry["dlq_id"] == dlq_id

    def test_get_nonexistent(self):
        entry = dlq_repository.get_dlq_message(99999)
        assert entry is None


class TestUpdateDlqStatus:
    """Tests for update_dlq_status."""

    def test_update_to_resolved(self, sample_message, sample_error):
        dlq_id = dlq_repository.persist_dlq_message(
            original_message=sample_message,
            error=sample_error,
            retry_count=3,
            source_topic="topic",
            consumer_group="group",
        )
        result = dlq_repository.update_dlq_status(
            dlq_id, "resolved", resolution_notes="Fixed the bug"
        )
        assert result is True

        entry = dlq_repository.get_dlq_message(dlq_id)
        assert entry["status"] == "resolved"
        assert entry["resolution_notes"] == "Fixed the bug"
        assert entry["resolved_at"] is not None

    def test_update_to_retrying(self, sample_message, sample_error):
        dlq_id = dlq_repository.persist_dlq_message(
            original_message=sample_message,
            error=sample_error,
            retry_count=3,
            source_topic="topic",
            consumer_group="group",
        )
        result = dlq_repository.update_dlq_status(dlq_id, "retrying")
        assert result is True

        entry = dlq_repository.get_dlq_message(dlq_id)
        assert entry["status"] == "retrying"
        assert entry["resolved_at"] is None

    def test_update_nonexistent_returns_false(self):
        result = dlq_repository.update_dlq_status(99999, "resolved")
        assert result is False

    def test_update_with_increment_retry(self, sample_message, sample_error):
        dlq_id = dlq_repository.persist_dlq_message(
            original_message=sample_message,
            error=sample_error,
            retry_count=3,
            source_topic="topic",
            consumer_group="group",
        )
        dlq_repository.update_dlq_status(
            dlq_id, "pending", increment_retry=True
        )
        entry = dlq_repository.get_dlq_message(dlq_id)
        assert entry["retry_count"] == 4

    def test_update_error_fields(self, sample_message, sample_error):
        dlq_id = dlq_repository.persist_dlq_message(
            original_message=sample_message,
            error=sample_error,
            retry_count=3,
            source_topic="topic",
            consumer_group="group",
        )
        dlq_repository.update_dlq_status(
            dlq_id, "pending",
            error_type="RuntimeError",
            error_message="New error on retry",
        )
        entry = dlq_repository.get_dlq_message(dlq_id)
        assert entry["error_type"] == "RuntimeError"
        assert entry["error_message"] == "New error on retry"


class TestBulkUpdateStatus:
    """Tests for bulk_update_status."""

    def test_bulk_resolve(self):
        ids = []
        for i in range(3):
            dlq_id = dlq_repository.persist_dlq_message(
                original_message={"integration_id": i},
                error=Exception("err"),
                retry_count=1,
                source_topic="topic",
                consumer_group="group",
            )
            ids.append(dlq_id)

        count = dlq_repository.bulk_update_status(ids, "resolved", "Bulk resolved")
        assert count == 3

        for dlq_id in ids:
            entry = dlq_repository.get_dlq_message(dlq_id)
            assert entry["status"] == "resolved"
            assert entry["resolution_notes"] == "Bulk resolved"

    def test_bulk_empty_list(self):
        count = dlq_repository.bulk_update_status([], "resolved")
        assert count == 0


class TestGetDlqStats:
    """Tests for get_dlq_stats."""

    def test_stats_empty(self):
        stats = dlq_repository.get_dlq_stats()
        assert stats == {}

    def test_stats_with_entries(self):
        for i in range(3):
            dlq_repository.persist_dlq_message(
                original_message={"integration_id": i},
                error=Exception("err"),
                retry_count=1,
                source_topic="topic",
                consumer_group="group",
            )

        # Resolve one
        dlq_repository.update_dlq_status(1, "resolved")

        stats = dlq_repository.get_dlq_stats()
        assert stats.get("pending", 0) == 2
        assert stats.get("resolved", 0) == 1
