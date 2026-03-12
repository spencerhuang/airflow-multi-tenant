# MySQL CDC Setup Guide

This document explains the MySQL configuration for Debezium Change Data Capture (CDC) in both Docker and Kubernetes environments.

## Overview

Debezium requires specific MySQL permissions and configuration to capture database changes:

1. **Binary Logging** - Must be enabled with ROW format
2. **GTID Mode** - Recommended for replication consistency
3. **Replication Permissions** - User must have REPLICATION SLAVE and REPLICATION CLIENT
4. **Administrative Permissions** - User needs RELOAD and FLUSH_TABLES for snapshots

## Docker Setup

### Files Created

1. **[`docker/mysql.cnf`](docker/mysql.cnf)** - MySQL configuration file
2. **[`docker/mysql-init.sql`](docker/mysql-init.sql)** - Initialization script with Debezium permissions

### Configuration

The [`docker-compose.yml`](docker-compose.yml) has been updated to mount these files:

```yaml
mysql:
  image: mysql:8.0
  container_name: control-plane-mysql
  environment:
    MYSQL_ROOT_PASSWORD: root
    MYSQL_DATABASE: control_plane
    MYSQL_USER: control_plane
    MYSQL_PASSWORD: control_plane
  volumes:
    - mysql-data:/var/lib/mysql
    - ./docker/mysql.cnf:/etc/mysql/conf.d/mysql.cnf:ro
    - ./docker/mysql-init.sql:/docker-entrypoint-initdb.d/01-init.sql:ro
```

### MySQL Configuration (`mysql.cnf`)

```ini
[mysqld]
# Server ID (required for replication)
server-id = 1

# Enable binary logging (required for CDC)
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL

# Binary log retention (7 days)
binlog_expire_logs_seconds = 604800

# GTID mode (recommended)
gtid_mode = ON
enforce_gtid_consistency = ON

# Performance optimizations
max_connections = 200
innodb_buffer_pool_size = 256M

# Character set
character-set-server = utf8mb4
collation-server = utf8mb4_unicode_ci
```

### Initialization Script (`mysql-init.sql`)

The init script automatically grants required permissions when MySQL starts:

```sql
-- Grant Debezium CDC permissions
GRANT SELECT ON control_plane.* TO 'control_plane'@'%';
GRANT RELOAD ON *.* TO 'control_plane'@'%';
GRANT SHOW DATABASES ON *.* TO 'control_plane'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'control_plane'@'%';
GRANT REPLICATION CLIENT ON *.* TO 'control_plane'@'%';
GRANT FLUSH_TABLES ON *.* TO 'control_plane'@'%';
FLUSH PRIVILEGES;
```

### Setup Instructions (Docker)

#### For New Deployments

1. **Start Services** (permissions will be configured automatically):
   ```bash
   docker-compose up -d
   ```

2. **Verify Configuration**:
   ```bash
   # Check binlog is enabled
   docker exec control-plane-mysql mysql -uroot -proot \
     -e "SHOW VARIABLES LIKE 'log_bin'"

   # Check binlog format
   docker exec control-plane-mysql mysql -uroot -proot \
     -e "SHOW VARIABLES LIKE 'binlog_format'"

   # Verify permissions
   docker exec control-plane-mysql mysql -uroot -proot \
     -e "SHOW GRANTS FOR 'control_plane'@'%'"
   ```

#### For Existing Deployments

If you have an existing MySQL container with data:

**Option 1: Recreate MySQL container** (data will be lost):
```bash
# Stop services
docker-compose down

# Remove MySQL volume
docker volume rm airflow-multi-tenant_mysql-data

# Start services (will recreate with new config)
docker-compose up -d
```

**Option 2: Grant permissions manually** (preserves data):
```bash
# Grant permissions
docker exec -i control-plane-mysql mysql -uroot -proot <<'EOF'
GRANT SELECT ON control_plane.* TO 'control_plane'@'%';
GRANT RELOAD ON *.* TO 'control_plane'@'%';
GRANT SHOW DATABASES ON *.* TO 'control_plane'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'control_plane'@'%';
GRANT REPLICATION CLIENT ON *.* TO 'control_plane'@'%';
GRANT FLUSH_TABLES ON *.* TO 'control_plane'@'%';
FLUSH PRIVILEGES;
EOF

# Restart MySQL to apply config changes
docker-compose restart mysql

# Verify binlog is enabled
docker exec control-plane-mysql mysql -uroot -proot \
  -e "SHOW VARIABLES LIKE 'log_bin'"
```

**Note**: If binlog is not enabled after restart, you need to recreate the container (Option 1) since binlog configuration requires MySQL initialization.

## Kubernetes Setup

### Files Created

**[`k8s/deployments/mysql-statefulset.yaml`](k8s/deployments/mysql-statefulset.yaml)** - Complete MySQL deployment for Kubernetes with CDC support

This includes:
- ConfigMap with MySQL configuration and init script
- Secret for MySQL credentials
- Service (headless for StatefulSet)
- StatefulSet with persistent storage

### Setup Instructions (Kubernetes)

#### Deploy MySQL with CDC Support

1. **Apply MySQL StatefulSet**:
   ```bash
   kubectl apply -f k8s/deployments/mysql-statefulset.yaml
   ```

2. **Wait for MySQL to be Ready**:
   ```bash
   kubectl wait --for=condition=ready pod -l app=mysql -n airflow --timeout=300s
   ```

3. **Verify Configuration**:
   ```bash
   # Check binlog is enabled
   kubectl exec -n airflow mysql-0 -- \
     mysql -uroot -proot -e "SHOW VARIABLES LIKE 'log_bin'"

   # Check binlog format
   kubectl exec -n airflow mysql-0 -- \
     mysql -uroot -proot -e "SHOW VARIABLES LIKE 'binlog_format'"

   # Verify permissions
   kubectl exec -n airflow mysql-0 -- \
     mysql -uroot -proot -e "SHOW GRANTS FOR 'control_plane'@'%'"
   ```

#### Using External MySQL (Cloud SQL, RDS, etc.)

If using an external MySQL service:

1. **Enable Binary Logging**:
   - **AWS RDS**: Set `binlog_format = ROW` in parameter group
   - **Google Cloud SQL**: Enable binary logging in instance settings
   - **Azure Database**: Enable binary logging in server parameters

2. **Grant Permissions**:
   ```sql
   GRANT SELECT ON control_plane.* TO 'control_plane'@'%';
   GRANT RELOAD ON *.* TO 'control_plane'@'%';
   GRANT SHOW DATABASES ON *.* TO 'control_plane'@'%';
   GRANT REPLICATION SLAVE ON *.* TO 'control_plane'@'%';
   GRANT REPLICATION CLIENT ON *.* TO 'control_plane'@'%';
   GRANT FLUSH_TABLES ON *.* TO 'control_plane'@'%';
   FLUSH PRIVILEGES;
   ```

3. **Update ConfigMap**:
   Edit [`k8s/configmaps/control-plane-config.yaml`](k8s/configmaps/control-plane-config.yaml):
   ```yaml
   DATABASE_URL: "mysql+pymysql://control_plane:password@your-external-mysql-host:3306/control_plane"
   ```

4. **Update Debezium Connector Config**:
   Edit [`debezium/integration-connector.json`](debezium/integration-connector.json):
   ```json
   {
     "database.hostname": "your-external-mysql-host",
     "database.port": "3306",
     "database.user": "control_plane",
     "database.password": "your-password"
   }
   ```

## Required Permissions Explained

### SELECT
- **Purpose**: Read table data during initial snapshot
- **Scope**: `control_plane.*` (only on target database)

### RELOAD
- **Purpose**: Execute FLUSH commands for consistent snapshots
- **Scope**: `*.*` (global privilege)

### SHOW DATABASES
- **Purpose**: List databases for monitoring
- **Scope**: `*.*` (global privilege)

### REPLICATION SLAVE
- **Purpose**: Read binary log events
- **Scope**: `*.*` (global privilege)

### REPLICATION CLIENT
- **Purpose**: Query replication status
- **Scope**: `*.*` (global privilege)

### FLUSH_TABLES
- **Purpose**: Lock tables during snapshot
- **Scope**: `*.*` (global privilege)

## Binary Log Configuration Explained

### log_bin
- **Value**: `mysql-bin`
- **Purpose**: Enable binary logging with specified basename
- **Required**: Yes

### binlog_format
- **Value**: `ROW`
- **Purpose**: Log individual row changes (required for CDC)
- **Required**: Yes
- **Alternatives**: STATEMENT, MIXED (not supported by Debezium)

### binlog_row_image
- **Value**: `FULL`
- **Purpose**: Log complete before/after images of rows
- **Required**: Yes
- **Alternatives**: MINIMAL, NOBLOB (not recommended)

### binlog_expire_logs_seconds
- **Value**: `604800` (7 days)
- **Purpose**: Automatically purge old binary logs
- **Recommended**: Yes (prevents disk space issues)

### gtid_mode
- **Value**: `ON`
- **Purpose**: Enable Global Transaction Identifiers
- **Required**: No (but recommended for consistency)

### enforce_gtid_consistency
- **Value**: `ON`
- **Purpose**: Ensure GTID consistency
- **Required**: If gtid_mode is ON

## Verification Commands

### Check Binary Log Status

```bash
# Docker
docker exec control-plane-mysql mysql -uroot -proot \
  -e "SHOW MASTER STATUS"

# Kubernetes
kubectl exec -n airflow mysql-0 -- \
  mysql -uroot -proot -e "SHOW MASTER STATUS"
```

Expected output:
```
+------------------+----------+--------------+------------------+-------------------------------------------+
| File             | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set                         |
+------------------+----------+--------------+------------------+-------------------------------------------+
| mysql-bin.000001 |      156 |              |                  | 3e91fa47-5f4c-11e9-b584-0800200c9a66:1-10|
+------------------+----------+--------------+------------------+-------------------------------------------+
```

### Check User Permissions

```bash
# Docker
docker exec control-plane-mysql mysql -uroot -proot \
  -e "SHOW GRANTS FOR 'control_plane'@'%'"

# Kubernetes
kubectl exec -n airflow mysql-0 -- \
  mysql -uroot -proot -e "SHOW GRANTS FOR 'control_plane'@'%'"
```

Expected output:
```
GRANT RELOAD, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `control_plane`@`%`
GRANT FLUSH_TABLES ON *.* TO `control_plane`@`%`
GRANT SELECT, ALL PRIVILEGES ON `control_plane`.* TO `control_plane`@`%`
```

### Test CDC Connection

```bash
# Register Debezium connector
python debezium/register_connector.py

# Check connector status
curl -s http://localhost:8083/connectors/integration-cdc-connector/status | python3 -m json.tool
```

Expected status:
```json
{
    "name": "integration-cdc-connector",
    "connector": {
        "state": "RUNNING"
    },
    "tasks": [
        {
            "id": 0,
            "state": "RUNNING"
        }
    ]
}
```

## Troubleshooting

### Issue: "Access denied; you need (at least one of) the RELOAD privilege(s)"

**Solution**: Grant RELOAD permission:
```sql
GRANT RELOAD ON *.* TO 'control_plane'@'%';
FLUSH PRIVILEGES;
```

### Issue: "Binary logging is not enabled"

**Solution**:
1. Ensure `mysql.cnf` is mounted correctly
2. Restart MySQL container/pod
3. If still not enabled, recreate container/pod (binlog requires clean initialization)

### Issue: "Could not find first log file name in binary log index file"

**Solution**:
1. This happens when binary logs are corrupted or missing
2. Reset binary logs:
   ```sql
   RESET MASTER;
   ```
3. Restart Debezium connector

### Issue: Connector Status shows "FAILED"

**Check logs**:
```bash
# Docker
docker logs kafka-connect 2>&1 | tail -50

# Kubernetes
kubectl logs -n airflow -l app=kafka-connect --tail=50
```

**Common causes**:
- Missing permissions → Grant required permissions
- Binlog not enabled → Enable in MySQL configuration
- Wrong credentials → Update connector configuration
- Network connectivity → Check service/endpoint configuration

## Security Best Practices

### Production Considerations

1. **Use Secrets for Credentials**:
   - Docker: Use Docker secrets or environment files (not committed to git)
   - Kubernetes: Use Kubernetes Secrets with encryption at rest

2. **Restrict User Permissions**:
   - Grant only to specific database: `control_plane.*` instead of `*.*`
   - Use separate user for Debezium vs application

3. **Network Security**:
   - Use TLS for MySQL connections
   - Restrict MySQL port access using network policies

4. **Binary Log Security**:
   - Encrypt binary logs if supported
   - Set appropriate retention period
   - Monitor disk space usage

5. **Audit**:
   - Enable MySQL audit plugin
   - Monitor privilege changes
   - Log all Debezium connector activity

### Example Secure Setup (Kubernetes)

```yaml
# Use external secret management
apiVersion: v1
kind: Secret
metadata:
  name: mysql-secret
  namespace: airflow
  annotations:
    # Reference to external secret manager
    vault.hashicorp.com/agent-inject: "true"
    vault.hashicorp.com/role: "mysql-reader"
    vault.hashicorp.com/agent-inject-secret-password: "database/mysql/credentials"
type: Opaque
```

## Summary

✅ **Docker Setup**:
- `docker/mysql.cnf` - MySQL configuration with binlog enabled
- `docker/mysql-init.sql` - Initialization script with CDC permissions
- `docker-compose.yml` - Updated to mount configuration files

✅ **Kubernetes Setup**:
- `k8s/deployments/mysql-statefulset.yaml` - Complete MySQL deployment with CDC support
- Includes ConfigMap, Secret, Service, and StatefulSet
- Ready for production with persistent storage

✅ **Permissions**:
- RELOAD, REPLICATION SLAVE, REPLICATION CLIENT
- FLUSH_TABLES, SHOW DATABASES, SELECT

✅ **Binary Logging**:
- Enabled with ROW format
- FULL row image
- 7-day retention
- GTID mode enabled

🎉 **MySQL is now configured for Debezium CDC!**

For more information, see:
- [Debezium MySQL Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/mysql.html)
- [DEBEZIUM_CDC_IMPLEMENTATION.md](DEBEZIUM_CDC_IMPLEMENTATION.md) - Complete CDC implementation guide
- [IMPLEMENTATION_SUMMARY.md — Database Schema Management](IMPLEMENTATION_SUMMARY.md#database-schema-management-alembic-migrations) - Alembic migration workflow for schema changes
