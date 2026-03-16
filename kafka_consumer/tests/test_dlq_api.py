"""Tests for DLQ management API endpoints.

Uses FastAPI TestClient with an in-memory SQLite database.
"""

import json
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.pool import StaticPool

from shared_models.tables import metadata
from kafka_consumer.app.services import dlq_repository
from kafka_consumer.app.main import app


@pytest.fixture(autouse=True)
def setup_sqlite_engine():
    """Replace the repository engine with an in-memory SQLite database.

    Uses StaticPool so the same connection is shared across threads
    (FastAPI TestClient runs endpoints in a threadpool).
    """
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    metadata.create_all(engine)
    dlq_repository._engine = engine
    yield engine
    dlq_repository._engine = None


@pytest.fixture
def client():
    """FastAPI test client."""
    return TestClient(app)


@pytest.fixture
def seed_entries():
    """Insert 5 DLQ entries and return their IDs."""
    ids = []
    for i in range(5):
        dlq_id = dlq_repository.persist_dlq_message(
            original_message={
                "event_type": "integration.created",
                "event_id": f"seed-{i}",
                "data": {"integration_id": i + 1},
            },
            error=ValueError(f"Error {i}"),
            retry_count=3,
            source_topic="cdc.integration.events",
            consumer_group="cdc-consumer",
            message_key=str(i + 1),
        )
        ids.append(dlq_id)
    return ids


class TestListDlqEndpoint:
    """Tests for GET /dlq."""

    def test_list_empty(self, client):
        resp = client.get("/dlq")
        assert resp.status_code == 200
        data = resp.json()
        assert data["entries"] == []
        assert data["count"] == 0

    def test_list_with_entries(self, client, seed_entries):
        resp = client.get("/dlq")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 5

    def test_list_filter_by_status(self, client, seed_entries):
        # Resolve one
        dlq_repository.update_dlq_status(seed_entries[0], "resolved")

        resp = client.get("/dlq?status=pending")
        assert resp.json()["count"] == 4

        resp = client.get("/dlq?status=resolved")
        assert resp.json()["count"] == 1

    def test_list_filter_by_integration_id(self, client, seed_entries):
        resp = client.get("/dlq?integration_id=3")
        data = resp.json()
        assert data["count"] == 1
        assert data["entries"][0]["integration_id"] == 3

    def test_list_pagination(self, client, seed_entries):
        resp = client.get("/dlq?limit=2&offset=0")
        assert resp.json()["count"] == 2

        resp = client.get("/dlq?limit=2&offset=4")
        assert resp.json()["count"] == 1

    def test_list_invalid_status(self, client):
        resp = client.get("/dlq?status=bogus")
        assert resp.status_code == 400


class TestGetDlqEndpoint:
    """Tests for GET /dlq/{dlq_id}."""

    def test_get_existing(self, client, seed_entries):
        resp = client.get(f"/dlq/{seed_entries[0]}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["dlq_id"] == seed_entries[0]
        assert data["status"] == "pending"

    def test_get_not_found(self, client):
        resp = client.get("/dlq/99999")
        assert resp.status_code == 404


class TestStatsEndpoint:
    """Tests for GET /dlq/stats."""

    def test_stats_empty(self, client):
        resp = client.get("/dlq/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_pending"] == 0

    def test_stats_with_entries(self, client, seed_entries):
        dlq_repository.update_dlq_status(seed_entries[0], "resolved")

        resp = client.get("/dlq/stats")
        data = resp.json()
        assert data["counts_by_status"]["pending"] == 4
        assert data["counts_by_status"]["resolved"] == 1
        assert data["total_pending"] == 4


class TestResolveEndpoint:
    """Tests for PUT /dlq/{dlq_id}/resolve."""

    def test_resolve(self, client, seed_entries):
        resp = client.put(
            f"/dlq/{seed_entries[0]}/resolve",
            json={"resolution_notes": "Investigated and fixed"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "resolved"

        # Verify in DB
        entry = dlq_repository.get_dlq_message(seed_entries[0])
        assert entry["status"] == "resolved"
        assert entry["resolution_notes"] == "Investigated and fixed"
        assert entry["resolved_at"] is not None

    def test_resolve_not_found(self, client):
        resp = client.put("/dlq/99999/resolve", json={})
        assert resp.status_code == 404


class TestRetryEndpoint:
    """Tests for POST /dlq/{dlq_id}/retry."""

    def test_retry_success(self, client, seed_entries):
        """Test retrying a DLQ entry where processing succeeds."""
        mock_service = MagicMock()
        mock_service._process_message = MagicMock()  # No exception = success

        with patch(
            "kafka_consumer.app.api.dlq.get_kafka_consumer_service",
            return_value=mock_service,
        ):
            resp = client.post(f"/dlq/{seed_entries[0]}/retry")

        assert resp.status_code == 200
        assert resp.json()["status"] == "resolved"

        # Verify _process_message was called with the original message
        mock_service._process_message.assert_called_once()
        call_arg = mock_service._process_message.call_args[0][0]
        assert call_arg["event_type"] == "integration.created"

        # Verify DB updated
        entry = dlq_repository.get_dlq_message(seed_entries[0])
        assert entry["status"] == "resolved"

    def test_retry_failure(self, client, seed_entries):
        """Test retrying a DLQ entry where processing fails again."""
        mock_service = MagicMock()
        mock_service._process_message = MagicMock(
            side_effect=RuntimeError("Still broken")
        )

        with patch(
            "kafka_consumer.app.api.dlq.get_kafka_consumer_service",
            return_value=mock_service,
        ):
            resp = client.post(f"/dlq/{seed_entries[0]}/retry")

        assert resp.status_code == 200
        assert resp.json()["status"] == "pending"
        assert "Still broken" in resp.json()["message"]

        # Verify DB updated with new error
        entry = dlq_repository.get_dlq_message(seed_entries[0])
        assert entry["status"] == "pending"
        assert entry["error_type"] == "RuntimeError"
        assert entry["retry_count"] == 4  # incremented from 3

    def test_retry_not_found(self, client):
        resp = client.post("/dlq/99999/retry")
        assert resp.status_code == 404

    def test_retry_already_resolved(self, client, seed_entries):
        dlq_repository.update_dlq_status(seed_entries[0], "resolved")
        resp = client.post(f"/dlq/{seed_entries[0]}/retry")
        assert resp.status_code == 400

    def test_retry_service_unavailable(self, client, seed_entries):
        with patch(
            "kafka_consumer.app.api.dlq.get_kafka_consumer_service",
            return_value=None,
        ):
            resp = client.post(f"/dlq/{seed_entries[0]}/retry")
        assert resp.status_code == 503


class TestBulkResolveEndpoint:
    """Tests for POST /dlq/bulk/resolve."""

    def test_bulk_resolve(self, client, seed_entries):
        resp = client.post(
            "/dlq/bulk/resolve",
            json={"dlq_ids": seed_entries[:3], "resolution_notes": "Batch fix"},
        )
        assert resp.status_code == 200
        assert resp.json()["updated"] == 3

    def test_bulk_resolve_empty(self, client):
        resp = client.post("/dlq/bulk/resolve", json={"dlq_ids": []})
        assert resp.status_code == 400


class TestBulkRetryEndpoint:
    """Tests for POST /dlq/bulk/retry."""

    def test_bulk_retry_mixed_results(self, client, seed_entries):
        """Test bulk retry with some successes and some failures."""
        call_count = 0

        def side_effect(msg):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("Still broken")

        mock_service = MagicMock()
        mock_service._process_message = MagicMock(side_effect=side_effect)

        with patch(
            "kafka_consumer.app.api.dlq.get_kafka_consumer_service",
            return_value=mock_service,
        ):
            resp = client.post(
                "/dlq/bulk/retry",
                json={"dlq_ids": seed_entries[:3]},
            )

        assert resp.status_code == 200
        results = resp.json()["results"]
        assert len(results) == 3

        statuses = [r["status"] for r in results]
        assert statuses.count("resolved") == 2
        assert statuses.count("failed") == 1

    def test_bulk_retry_empty(self, client):
        resp = client.post("/dlq/bulk/retry", json={"dlq_ids": []})
        assert resp.status_code == 400


class TestDlqE2EFlow:
    """End-to-end test: poison pill → DLQ DB → API shows it."""

    def test_send_to_dlq_persists_to_db_and_shows_in_api(self, client):
        """Simulate the full flow: _send_to_dlq writes to DB, API returns it."""
        from kafka_consumer.app.services.kafka_consumer_service import KafkaConsumerService

        # Create a consumer with DLQ enabled but mock the Kafka producer
        consumer = KafkaConsumerService(
            bootstrap_servers="localhost:9092",
            topic="cdc.integration.events",
            group_id="test-e2e-dlq",
            enable_dlq=True,
            max_retries=2,
        )
        # Prevent actual Kafka producer creation
        consumer.dlq_producer = MagicMock()
        mock_future = MagicMock()
        consumer.dlq_producer.send.return_value = mock_future

        poison_pill = {
            "event_type": "integration.created",
            "event_id": "poison-001",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {"integration_id": 666, "tenant_id": "ws-bad"},
        }
        error = ValueError("Cannot process this message")

        # Act: send to DLQ (this should persist to DB)
        with patch("kafka_consumer.app.services.kafka_consumer_service.settings") as mock_settings:
            mock_settings.KAFKA_DLQ_DB_ENABLED = True
            consumer._send_to_dlq(poison_pill, error, retry_count=2)

        # Verify: API shows the DLQ entry
        resp = client.get("/dlq")
        data = resp.json()
        assert data["count"] == 1

        entry = data["entries"][0]
        assert entry["status"] == "pending"
        assert entry["error_type"] == "ValueError"
        assert entry["error_message"] == "Cannot process this message"
        assert entry["retry_count"] == 2
        assert entry["original_message"]["event_id"] == "poison-001"
        assert entry["original_message"]["data"]["integration_id"] == 666

        # Verify: stats endpoint
        resp = client.get("/dlq/stats")
        assert resp.json()["total_pending"] == 1

        # Verify: get by ID
        dlq_id = entry["dlq_id"]
        resp = client.get(f"/dlq/{dlq_id}")
        assert resp.status_code == 200
        assert resp.json()["dlq_id"] == dlq_id

    def test_poison_pill_retry_then_resolve(self, client):
        """Full lifecycle: poison pill → DLQ → retry (fail) → retry (succeed) → resolved."""
        from kafka_consumer.app.services.kafka_consumer_service import KafkaConsumerService

        consumer = KafkaConsumerService(
            bootstrap_servers="localhost:9092",
            topic="cdc.integration.events",
            group_id="test-lifecycle",
            enable_dlq=True,
        )
        consumer.dlq_producer = MagicMock()
        consumer.dlq_producer.send.return_value = MagicMock()

        poison_pill = {
            "event_type": "integration.created",
            "event_id": "lifecycle-001",
            "data": {"integration_id": 42},
        }

        with patch("kafka_consumer.app.services.kafka_consumer_service.settings") as mock_settings:
            mock_settings.KAFKA_DLQ_DB_ENABLED = True
            consumer._send_to_dlq(poison_pill, RuntimeError("Broken"), retry_count=3)

        # Step 1: Verify it's in DLQ
        resp = client.get("/dlq?status=pending")
        assert resp.json()["count"] == 1
        dlq_id = resp.json()["entries"][0]["dlq_id"]

        # Step 2: Retry but fail again
        mock_service = MagicMock()
        mock_service._process_message = MagicMock(
            side_effect=RuntimeError("Still broken")
        )
        with patch(
            "kafka_consumer.app.api.dlq.get_kafka_consumer_service",
            return_value=mock_service,
        ):
            resp = client.post(f"/dlq/{dlq_id}/retry")
        assert resp.json()["status"] == "pending"

        entry = dlq_repository.get_dlq_message(dlq_id)
        assert entry["retry_count"] == 4

        # Step 3: Retry and succeed
        mock_service._process_message = MagicMock()  # No exception
        with patch(
            "kafka_consumer.app.api.dlq.get_kafka_consumer_service",
            return_value=mock_service,
        ):
            resp = client.post(f"/dlq/{dlq_id}/retry")
        assert resp.json()["status"] == "resolved"

        # Step 4: Verify final state
        entry = dlq_repository.get_dlq_message(dlq_id)
        assert entry["status"] == "resolved"
        assert entry["resolved_at"] is not None

        # Stats should show 0 pending, 1 resolved
        resp = client.get("/dlq/stats")
        assert resp.json()["total_pending"] == 0
        assert resp.json()["counts_by_status"]["resolved"] == 1
