-- MySQL initialization script for Control Plane database
-- This script runs automatically when MySQL container starts for the first time

-- Grant Debezium CDC permissions to control_plane user
-- These permissions are required for Debezium to capture change data
GRANT SELECT ON control_plane.* TO 'control_plane'@'%';
GRANT RELOAD ON *.* TO 'control_plane'@'%';
GRANT SHOW DATABASES ON *.* TO 'control_plane'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'control_plane'@'%';
GRANT REPLICATION CLIENT ON *.* TO 'control_plane'@'%';

-- Grant FLUSH_TABLES for snapshot operations
GRANT FLUSH_TABLES ON *.* TO 'control_plane'@'%';

-- Apply privileges
FLUSH PRIVILEGES;

-- Enable binlog (required for CDC)
-- Note: This must also be set in my.cnf for persistent configuration
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL binlog_row_image = 'FULL';

-- ── Audit Service Setup ──────────────────────────────────────────────────────
-- Dedicated audit_svc user with scoped privileges for the Audit Service.

-- Create audit_svc user
CREATE USER IF NOT EXISTS 'audit_svc'@'%' IDENTIFIED WITH mysql_native_password BY 'audit_svc';

-- Create audit_template schema and table (blueprint for per-customer schemas)
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

-- Grant audit_svc DML + DDL on all schemas (it dynamically creates per-customer schemas)
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP ON *.* TO 'audit_svc'@'%';

-- Grant SHOW DATABASES so audit_svc can list schemas
GRANT SHOW DATABASES ON *.* TO 'audit_svc'@'%';

-- Provision dummy audit schema for seed customer (Acme Corp)
-- Matches customer_guid from scripts/seed_s3_to_mongo.sql
CREATE SCHEMA IF NOT EXISTS `audit_cust_0001_aaaa_bbbb_000000000001`;

CREATE TABLE IF NOT EXISTS `audit_cust_0001_aaaa_bbbb_000000000001`.`audit_events`
    LIKE `audit_template`.`audit_events`;

FLUSH PRIVILEGES;

-- Log that initialization completed
SELECT 'MySQL initialization complete - Debezium CDC + Audit setup done' AS status;
