"""Tests for AuditSchemaManager.

These tests use a SQLite in-memory database to verify schema management
logic without requiring MySQL. Note: some MySQL-specific features
(CREATE TABLE LIKE, INFORMATION_SCHEMA queries) are mocked.
"""

from unittest.mock import MagicMock, patch, call

import pytest

from audit_service.app.services.schema_manager import AuditSchemaManager


class TestSchemaName:
    """Test the _schema_name helper."""

    def test_converts_hyphens_to_underscores(self):
        """customer_guid hyphens should become underscores in schema name."""
        manager = MagicMock(spec=AuditSchemaManager)
        result = AuditSchemaManager._schema_name(manager, "cust-0001-aaaa-bbbb-000000000001")
        assert result == "audit_cust_0001_aaaa_bbbb_000000000001"

    def test_rejects_invalid_characters(self):
        """customer_guid with invalid chars should raise ValueError."""
        manager = MagicMock(spec=AuditSchemaManager)
        with pytest.raises(ValueError, match="Invalid customer_guid"):
            AuditSchemaManager._schema_name(manager, "cust; DROP TABLE")


class TestSchemaExists:
    """Test the schema_exists cache-based lookup."""

    def test_returns_true_for_cached_schema(self):
        """schema_exists should return True if schema is in cache."""
        engine = MagicMock()
        with patch.object(AuditSchemaManager, "_refresh_cache"):
            manager = AuditSchemaManager(engine)
        manager._cache = {"audit_cust_0001_aaaa_bbbb_000000000001"}
        assert manager.schema_exists("cust-0001-aaaa-bbbb-000000000001") is True

    def test_returns_false_for_missing_schema(self):
        """schema_exists should return False if schema is not in cache."""
        engine = MagicMock()
        with patch.object(AuditSchemaManager, "_refresh_cache"):
            manager = AuditSchemaManager(engine)
        manager._cache = set()
        assert manager.schema_exists("cust-0001-aaaa-bbbb-000000000001") is False


class TestProvisionCustomer:
    """Test schema provisioning."""

    def test_provision_creates_schema_and_updates_cache(self):
        """provision_customer should CREATE SCHEMA, CREATE TABLE, and add to cache."""
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(AuditSchemaManager, "_refresh_cache"):
            manager = AuditSchemaManager(engine)

        schema_name = manager.provision_customer("cust-0001-aaaa-bbbb-000000000001")

        assert schema_name == "audit_cust_0001_aaaa_bbbb_000000000001"
        assert "audit_cust_0001_aaaa_bbbb_000000000001" in manager._cache
        # Verify SQL was executed (CREATE SCHEMA, CREATE TABLE)
        assert conn.execute.call_count == 2
        assert conn.commit.called


class TestDeprovisionCustomer:
    """Test schema deprovisioning (GDPR erasure)."""

    def test_deprovision_drops_schema_and_removes_from_cache(self):
        """deprovision_customer should DROP SCHEMA and remove from cache."""
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(AuditSchemaManager, "_refresh_cache"):
            manager = AuditSchemaManager(engine)
        manager._cache = {"audit_cust_0001_aaaa_bbbb_000000000001"}

        manager.deprovision_customer("cust-0001-aaaa-bbbb-000000000001")

        assert "audit_cust_0001_aaaa_bbbb_000000000001" not in manager._cache
        assert conn.execute.called
        assert conn.commit.called
