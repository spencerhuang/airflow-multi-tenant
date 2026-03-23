-- =============================================================================
-- Audit schema migration script
-- Creates audit_svc user, audit_template schema, seed customer schema, and
-- grants for the audit_svc user (dedicated audit service credentials).
-- Safe to run multiple times (all statements use IF NOT EXISTS / IF EXISTS).
--
-- Usage (Docker — against running MySQL container):
--   docker exec -i control-plane-mysql mysql -u root -proot < scripts/init_audit_schema.sql
--
-- Usage (local):
--   mysql -u root -proot -h 127.0.0.1 < scripts/init_audit_schema.sql
-- =============================================================================

-- ── 1. Create audit_svc user ─────────────────────────────────────────────────
CREATE USER IF NOT EXISTS 'audit_svc'@'%' IDENTIFIED WITH mysql_native_password BY 'audit_svc';

-- ── 2. Template schema (blueprint for per-customer schemas) ──────────────────
CREATE SCHEMA IF NOT EXISTS `audit_template`;

CREATE TABLE IF NOT EXISTS `audit_template`.`audit_events` (
    `event_id` VARCHAR(36) NOT NULL,
    `timestamp` DATETIME NOT NULL,
    `event_type` VARCHAR(100) NOT NULL,
    `actor_id` VARCHAR(255) NOT NULL,
    `actor_type` VARCHAR(50) NOT NULL,
    `actor_ip` VARCHAR(45) DEFAULT NULL,
    `resource_type` VARCHAR(100) NOT NULL,
    `resource_id` VARCHAR(255) NOT NULL,
    `action` VARCHAR(100) NOT NULL,
    `outcome` VARCHAR(20) NOT NULL,
    `before_state` TEXT DEFAULT NULL,
    `after_state` TEXT DEFAULT NULL,
    `trace_id` VARCHAR(64) DEFAULT NULL,
    `request_id` VARCHAR(36) DEFAULT NULL,
    `metadata_json` TEXT DEFAULT NULL,
    PRIMARY KEY (`event_id`),
    INDEX `idx_audit_event_type` (`event_type`),
    INDEX `idx_audit_timestamp` (`timestamp`),
    INDEX `idx_audit_resource` (`resource_type`, `resource_id`),
    INDEX `idx_audit_actor` (`actor_id`),
    INDEX `idx_audit_trace` (`trace_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 3. Grants for audit_svc user ────────────────────────────────────────────
-- DML + DDL on all schemas (it dynamically creates per-customer schemas)
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP ON *.* TO 'audit_svc'@'%';

-- Ability to list schemas
GRANT SHOW DATABASES ON *.* TO 'audit_svc'@'%';

-- ── 4. Seed customer schema (Acme Corp from scripts/seed_s3_to_mongo.sql) ───
CREATE SCHEMA IF NOT EXISTS `audit_cust_0001_aaaa_bbbb_000000000001`;

CREATE TABLE IF NOT EXISTS `audit_cust_0001_aaaa_bbbb_000000000001`.`audit_events`
    LIKE `audit_template`.`audit_events`;

FLUSH PRIVILEGES;

-- ── Verify ───────────────────────────────────────────────────────────────────
SELECT 'audit_svc user' AS component, COUNT(*) AS ok
FROM mysql.user WHERE User = 'audit_svc'
UNION ALL
SELECT 'audit_template schema', COUNT(*)
FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'audit_template'
UNION ALL
SELECT 'audit_template.audit_events table', COUNT(*)
FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'audit_template' AND TABLE_NAME = 'audit_events'
UNION ALL
SELECT 'acme_corp audit schema', COUNT(*)
FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'audit_cust_0001_aaaa_bbbb_000000000001';

SELECT '✅ Audit schema migration complete' AS status;
