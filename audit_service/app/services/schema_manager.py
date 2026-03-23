"""Audit schema-per-customer manager.

Each customer gets their own MySQL schema (``audit_{customer_guid}``)
with an ``audit_events`` table cloned from the ``audit_template`` schema.

This provides:
- Hard tenant isolation at the DB level
- Simple GDPR erasure via DROP SCHEMA
- Independent retention policies per customer
"""

import logging
import re
import threading
from typing import List, Set

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# customer_guid chars allowed in schema names (alphanumeric + hyphens)
_SAFE_GUID_RE = re.compile(r"^[a-zA-Z0-9\-]+$")


class AuditSchemaManager:
    """Manages per-customer audit schemas."""

    TEMPLATE_SCHEMA = "audit_template"
    TEMPLATE_TABLE = "audit_events"

    def __init__(self, engine: Engine):
        self._engine = engine
        self._cache: Set[str] = set()
        self._lock = threading.Lock()
        self._refresh_cache()

    # ── public API ──────────────────────────────────────────────────────

    def provision_customer(self, customer_guid: str) -> str:
        """Create a new audit schema for a customer.

        Returns the schema name.
        """
        schema = self._schema_name(customer_guid)
        with self._engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS `{schema}`"))
            conn.execute(
                text(
                    f"CREATE TABLE IF NOT EXISTS `{schema}`.`{self.TEMPLATE_TABLE}` "
                    f"LIKE `{self.TEMPLATE_SCHEMA}`.`{self.TEMPLATE_TABLE}`"
                )
            )
            conn.commit()
        with self._lock:
            self._cache.add(schema)
        logger.info(f"Provisioned audit schema: {schema}")
        return schema

    def deprovision_customer(self, customer_guid: str) -> None:
        """Drop a customer's audit schema (GDPR erasure)."""
        schema = self._schema_name(customer_guid)
        with self._engine.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS `{schema}`"))
            conn.commit()
        with self._lock:
            self._cache.discard(schema)
        logger.info(f"Deprovisioned audit schema: {schema}")

    def schema_exists(self, customer_guid: str) -> bool:
        """Check if a customer's audit schema exists (uses cache)."""
        schema = self._schema_name(customer_guid)
        with self._lock:
            return schema in self._cache

    def list_customer_schemas(self) -> List[str]:
        """List all audit_* schemas from INFORMATION_SCHEMA."""
        with self._engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                    "WHERE SCHEMA_NAME LIKE 'audit\\_%' "
                    "AND SCHEMA_NAME != 'audit_template'"
                )
            )
            return [row[0] for row in result]

    def ensure_template(self) -> None:
        """Ensure the audit_template schema and table exist."""
        from shared_models.tables import audit_events, audit_metadata

        with self._engine.connect() as conn:
            conn.execute(
                text(f"CREATE SCHEMA IF NOT EXISTS `{self.TEMPLATE_SCHEMA}`")
            )
            conn.commit()

        template_engine = self._engine.execution_options(
            schema_translate_map={None: self.TEMPLATE_SCHEMA}
        )
        audit_metadata.create_all(template_engine)
        logger.info("Audit template schema ensured")

    def refresh_cache(self) -> None:
        """Public method to refresh the schema cache."""
        self._refresh_cache()

    # ── internal ────────────────────────────────────────────────────────

    def _schema_name(self, customer_guid: str) -> str:
        """Convert customer_guid to a safe schema name."""
        safe_guid = customer_guid.replace("-", "_")
        if not _SAFE_GUID_RE.match(customer_guid):
            raise ValueError(f"Invalid customer_guid for schema name: {customer_guid}")
        return f"audit_{safe_guid}"

    def _refresh_cache(self) -> None:
        """Refresh the in-memory schema cache from INFORMATION_SCHEMA."""
        try:
            schemas = set(self.list_customer_schemas())
            with self._lock:
                self._cache = schemas
            logger.info(f"Schema cache refreshed: {len(schemas)} customer schemas")
        except Exception:
            logger.exception("Failed to refresh schema cache")
