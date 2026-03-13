-- =============================================================================
-- Seed data for the S3-to-Mongo integration use case
-- Run against the control_plane MySQL database
--
-- Usage (Docker):
--   docker exec -i control-plane-mysql mysql -u control_plane -pcontrol_plane control_plane < scripts/seed_s3_to_mongo.sql
--
-- Usage (local):
--   mysql -u control_plane -pcontrol_plane -h 127.0.0.1 control_plane < scripts/seed_s3_to_mongo.sql
-- =============================================================================

-- ── 1. Customer ─────────────────────────────────────────────────────────────
INSERT INTO customers (customer_guid, name, max_integration)
VALUES ('cust-0001-aaaa-bbbb-000000000001', 'Acme Corp', 100)
ON DUPLICATE KEY UPDATE name = VALUES(name);

-- ── 2. Workspace ────────────────────────────────────────────────────────────
INSERT INTO workspaces (workspace_id, customer_guid)
VALUES ('ws-0001-aaaa-bbbb-000000000001', 'cust-0001-aaaa-bbbb-000000000001')
ON DUPLICATE KEY UPDATE customer_guid = VALUES(customer_guid);

-- ── 3. Auth (S3 + MongoDB credentials) ──────────────────────────────────────
INSERT INTO auths (auth_id, workspace_id, auth_type, json_data)
VALUES (
    1,
    'ws-0001-aaaa-bbbb-000000000001',
    's3_mongo',
    JSON_OBJECT(
        's3_access_key',    'minioadmin',
        's3_secret_key',    'minioadmin',
        's3_endpoint_url',  'http://minio:9000',
        'mongo_uri',        'mongodb://root:root@mongodb:27017',
        'mongo_db',         'test_database'
    )
)
ON DUPLICATE KEY UPDATE json_data = VALUES(json_data);

-- ── 4. Access Points (source = S3, destination = MongoDB) ───────────────────
INSERT INTO access_points (access_pt_id, ap_type)
VALUES (1, 's3')
ON DUPLICATE KEY UPDATE ap_type = VALUES(ap_type);

INSERT INTO access_points (access_pt_id, ap_type)
VALUES (2, 'mongodb')
ON DUPLICATE KEY UPDATE ap_type = VALUES(ap_type);

-- ── 5. Workflow ─────────────────────────────────────────────────────────────
INSERT INTO workflows (workflow_id, workflow_type)
VALUES (1, 's3_to_mongo')
ON DUPLICATE KEY UPDATE workflow_type = VALUES(workflow_type);

-- ── Verify ──────────────────────────────────────────────────────────────────
SELECT '✅ Seed data inserted successfully' AS status;

SELECT 'customers'     AS tbl, COUNT(*) AS cnt FROM customers
UNION ALL
SELECT 'workspaces',          COUNT(*)        FROM workspaces
UNION ALL
SELECT 'auths',               COUNT(*)        FROM auths
UNION ALL
SELECT 'access_points',       COUNT(*)        FROM access_points
UNION ALL
SELECT 'workflows',           COUNT(*)        FROM workflows;
