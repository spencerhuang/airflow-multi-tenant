# Setup Guide - Complete

This guide covers the full setup of the Multi-Tenant Airflow Architecture, including general project setup and MySQL CDC configuration for both Docker and Kubernetes environments.

## Prerequisites

- Docker and Docker Compose (v2.0+)
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (fast Python package manager)
- Make (optional, for convenience commands)

### Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Quick Start

### 1. Clone and Setup

```bash
cd airflow-multi-tenant
make setup
```

This will:
- Create a `.env` file from `.env.example`
- Install Python dependencies via `uv`

Install the shared packages (required for local development):

```bash
uv pip install -e packages/shared_models -e packages/shared_utils
```

### 2. Start All Services

```bash
make docker-up
```

This starts:
- MySQL (Control Plane database) - Port 3306
- PostgreSQL (Airflow metadata) - Port 5432
- MongoDB (destination) - Port 27017
- MinIO (S3-compatible storage) - Ports 9000, 9001
- Kafka + Zookeeper (CDC) - Port 9092
- Airflow Webserver - Port 8080
- Airflow Scheduler
- Control Plane API - Port 8000

### 3. Verify Services

Wait 30-60 seconds for all services to start, then check:

```bash
# Check service status
docker-compose ps

# Check logs
make docker-logs
```

### 4. Access Web Interfaces

- **Control Plane API Docs**: http://localhost:8000/docs
- **Airflow UI**: http://localhost:8080 (username: `airflow`, password: `airflow`)
- **MinIO Console**: http://localhost:9001 (username: `minioadmin`, password: `minioadmin`)
- **Debezium UI**: http://localhost:8088
- **Kafka UI**: http://localhost:8081

## Database Setup

### Control Plane Database Schema (Alembic)

The control plane uses MySQL. Tables are managed via Alembic migrations.

```bash
# Run migrations (creates tables)
docker-compose exec control-plane python -m alembic upgrade head
```

### Seed Initial Data

You can seed initial data with a Python script:

```python
from sqlalchemy.orm import Session
from control_plane.app.core.database import SessionLocal
from control_plane.app.models import *

db = SessionLocal()

# Create customer
customer = Customer(customer_guid="cust-001", name="Demo Customer", max_integration=100)
db.add(customer)

# Create workspace
workspace = Workspace(workspace_id="ws-001", customer_guid="cust-001")
db.add(workspace)

# Create workflow
workflow = Workflow(workflow_id=1, workflow_type="S3ToMongo")
db.add(workflow)

# Create access points
s3_ap = AccessPoint(access_pt_id=1, ap_type="S3")
mongo_ap = AccessPoint(access_pt_id=2, ap_type="MongoDB")
db.add_all([s3_ap, mongo_ap])

db.commit()
```

## MySQL CDC Configuration

Debezium requires specific MySQL permissions and configuration to capture database changes:

1. **Binary Logging** - Must be enabled with ROW format
2. **GTID Mode** - Recommended for replication consistency
3. **Replication Permissions** - User must have REPLICATION SLAVE and REPLICATION CLIENT
4. **Administrative Permissions** - User needs RELOAD and FLUSH_TABLES for snapshots

### Docker Setup

The following files configure MySQL for CDC automatically on first startup:

- **[`docker/mysql.cnf`](docker/mysql.cnf)** - Enables binary logging, ROW format, GTID mode
- **[`docker/mysql-init.sql`](docker/mysql-init.sql)** - Grants Debezium CDC permissions
- **[`docker-compose.yml`](docker-compose.yml)** - Mounts config and init script into the MySQL container

Docker Compose MySQL service configuration:

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

#### MySQL Configuration (`mysql.cnf`)

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

#### Initialization Script (`mysql-init.sql`)

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

#### New Deployment Verification

```bash
# Verify MySQL configuration
docker exec control-plane-mysql mysql -uroot -proot \
  -e "SHOW VARIABLES LIKE 'log_bin'"

docker exec control-plane-mysql mysql -uroot -proot \
  -e "SHOW VARIABLES LIKE 'binlog_format'"

docker exec control-plane-mysql mysql -uroot -proot \
  -e "SHOW GRANTS FOR 'control_plane'@'%'"

# Register Debezium connector
python debezium/register_connector.py

# Verify connector is running
curl -s http://localhost:8083/connectors/integration-cdc-connector/status | python3 -m json.tool
```

#### Existing Deployment (Re-initialize)

**Option 1: Recreate** (data will be lost):
```bash
docker-compose down
docker volume rm airflow-multi-tenant_mysql-data
docker-compose up -d
```

**Option 2: Manual grants** (preserves data):
```bash
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

### Kubernetes Setup

**[`k8s/deployments/mysql-statefulset.yaml`](k8s/deployments/mysql-statefulset.yaml)** includes:
- ConfigMap with MySQL configuration and init script
- Secret for MySQL credentials
- Service (headless for StatefulSet)
- StatefulSet with persistent storage

```bash
# Deploy MySQL with CDC support
kubectl apply -f k8s/deployments/mysql-statefulset.yaml

# Wait for MySQL to be ready
kubectl wait --for=condition=ready pod -l app=mysql -n airflow --timeout=300s

# Verify configuration
kubectl exec -n airflow mysql-0 -- \
  mysql -uroot -proot -e "SHOW VARIABLES LIKE 'log_bin'"

kubectl exec -n airflow mysql-0 -- \
  mysql -uroot -proot -e "SHOW VARIABLES LIKE 'binlog_format'"

# Verify permissions
kubectl exec -n airflow mysql-0 -- \
  mysql -uroot -proot -e "SHOW GRANTS FOR 'control_plane'@'%'"
```

### External MySQL (Cloud SQL, RDS, etc.)

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

### Required Permissions Explained

| Permission | Purpose | Scope |
|---|---|---|
| **SELECT** | Read table data during initial snapshot | `control_plane.*` |
| **RELOAD** | Execute FLUSH commands for consistent snapshots | `*.*` (global) |
| **SHOW DATABASES** | List databases for monitoring | `*.*` (global) |
| **REPLICATION SLAVE** | Read binary log events | `*.*` (global) |
| **REPLICATION CLIENT** | Query replication status | `*.*` (global) |
| **FLUSH_TABLES** | Lock tables during snapshot | `*.*` (global) |

### Binary Log Configuration Explained

| Setting | Value | Purpose | Required |
|---|---|---|---|
| **log_bin** | `mysql-bin` | Enable binary logging with specified basename | Yes |
| **binlog_format** | `ROW` | Log individual row changes (required for CDC) | Yes |
| **binlog_row_image** | `FULL` | Log complete before/after images of rows | Yes |
| **binlog_expire_logs_seconds** | `604800` (7 days) | Automatically purge old binary logs | Recommended |
| **gtid_mode** | `ON` | Enable Global Transaction Identifiers | Recommended |
| **enforce_gtid_consistency** | `ON` | Ensure GTID consistency | If gtid_mode is ON |

### Verification Commands

#### Check Binary Log Status

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

#### Check User Permissions

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

#### Test CDC Connection

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

## MinIO (S3) Setup

### Create Test Bucket

1. Go to http://localhost:9001
2. Login with `minioadmin` / `minioadmin`
3. Create a bucket named `test-bucket`
4. Upload some test JSON files to `data/` prefix

Or use the MinIO CLI:

```bash
docker run --rm -it --network airflow-multi-tenant_default \
  minio/mc alias set local http://minio:9000 minioadmin minioadmin

docker run --rm -it --network airflow-multi-tenant_default \
  minio/mc mb local/test-bucket
```

## Running Tests

### Run All Tests

```bash
make test
```

### Run Tests with Coverage

```bash
make test-coverage
```

### Run Specific Test Suites

```bash
# Connector tests only
pytest connectors/tests/

# Control plane tests only
pytest control_plane/tests/

# Airflow DAG tests only
pytest airflow/tests/
```

### CDC Tests

```bash
# Test Debezium CDC
pytest control_plane/tests/test_debezium_cdc.py -v -s

# Test E2E with real CDC
pytest control_plane/tests/test_s3_to_mongo_e2e.py -v -s
```

## Development Workflow

### 1. Start Infrastructure Only (for local development)

```bash
make dev
```

This starts databases and services but not the control plane, so you can run it locally:

```bash
cd control_plane
uvicorn app.main:app --reload
```

### 2. Create Sample Data

Use the API to create sample integrations:

```bash
curl -X POST "http://localhost:8000/api/v1/integrations/" \
  -H "Content-Type: application/json" \
  -d '{
    "workspace_id": "workspace-123",
    "workflow_id": 1,
    "auth_id": 1,
    "source_access_pt_id": 1,
    "dest_access_pt_id": 2,
    "integration_type": "S3ToMongo",
    "usr_sch_cron": "0 2 * * *",
    "usr_timezone": "America/New_York",
    "schedule_type": "daily",
    "json_data": "{\"s3_bucket\": \"test-bucket\", \"s3_prefix\": \"data/\", \"mongo_collection\": \"test_collection\"}"
  }'
```

### 3. Trigger a DAG Run

```bash
curl -X POST "http://localhost:8000/api/v1/integrations/trigger" \
  -H "Content-Type: application/json" \
  -d '{
    "integration_id": 1,
    "execution_config": {}
  }'
```

### 4. View DAG Run in Airflow UI

Go to http://localhost:8080 and check the `s3_to_mongo_ondemand` DAG.

### 5. Verify CDC Event

```bash
# Check Kafka topic for CDC event
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.integration.events \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 5000
```

## CDC Architecture

```
1. API Request
   POST /api/v1/integrations/
   |
2. MySQL Database
   INSERT INTO integrations (...)
   [Binlog: ROW format, FULL image]
   |
3. Debezium Connector
   [Permissions: REPLICATION SLAVE, REPLICATION CLIENT, etc.]
   Reads binlog -> Publishes to Kafka
   |
4. Kafka Topic: cdc.integration.events
   {
     "integration_id": 123,
     "__op": "c",  // create
     "__source_ts_ms": 1234567890,
     ...all columns...
   }
   |
5. Kafka Consumer Service (Control Plane)
   Detects Debezium event -> Queries DB for json_data
   |
6. Airflow DAG Trigger
   trigger_dag_run(execution_config={...credentials...})
   |
7. Data Transfer
   S3 -> MongoDB
```

## Monitoring

### Debezium UI
- **URL**: http://localhost:8088
- View connector status, configuration, and metrics

### Kafka UI
- **URL**: http://localhost:8081
- Monitor topics, messages, consumer groups
- Check `cdc.integration.events` topic

### Logs

```bash
# Watch CDC event processing
docker-compose logs -f control-plane | grep "Processing Debezium CDC event"

# Check binary log events
docker exec control-plane-mysql mysqlbinlog /var/lib/mysql/mysql-bin.000003
```

## Troubleshooting

### Services Not Starting

```bash
# Check logs
docker-compose logs -f

# Restart specific service
docker-compose restart control-plane

# Rebuild and restart
docker-compose up -d --build
```

### Airflow Scheduler Not Picking Up DAGs

```bash
# Check DAG folder
docker-compose exec airflow-scheduler ls -la /opt/airflow/dags

# Trigger DAG parse
docker-compose exec airflow-scheduler airflow dags list
```

### Database Connection Issues

```bash
# Check MySQL
docker-compose exec mysql mysql -u root -proot -e "SHOW DATABASES;"

# Check PostgreSQL
docker-compose exec postgres-airflow psql -U airflow -c "\l"
```

### CDC: "Access denied; you need (at least one of) the RELOAD privilege(s)"

Grant RELOAD permission:
```sql
GRANT RELOAD ON *.* TO 'control_plane'@'%';
FLUSH PRIVILEGES;
```

### CDC: "Binary logging is not enabled"

1. Ensure `mysql.cnf` is mounted correctly
2. Restart MySQL container/pod
3. If still not enabled, recreate container/pod (binlog requires clean initialization)

### CDC: "Could not find first log file name in binary log index file"

Binary logs are corrupted or missing. Reset and restart:
```sql
RESET MASTER;
```
Then restart the Debezium connector.

### CDC: Connector Status shows "FAILED"

Check logs:
```bash
# Docker
docker logs kafka-connect 2>&1 | tail -50

# Kubernetes
kubectl logs -n airflow -l app=kafka-connect --tail=50
```

Common causes:
- Missing permissions - Grant required permissions
- Binlog not enabled - Enable in MySQL configuration
- Wrong credentials - Update connector configuration
- Network connectivity - Check service/endpoint configuration

### Reset Everything

```bash
make docker-down
docker volume prune
make docker-up
```

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
    vault.hashicorp.com/agent-inject: "true"
    vault.hashicorp.com/role: "mysql-reader"
    vault.hashicorp.com/agent-inject-secret-password: "database/mysql/credentials"
type: Opaque
```

## Production Checklist

Before deploying to production:

- [ ] Change all default passwords in `.env` file
- [ ] Enable authentication (`AUTH_ENABLED=true`)
- [ ] Use proper secret management (AWS Secrets Manager, HashiCorp Vault)
- [ ] Use Kubernetes Secrets for credentials (not ConfigMaps)
- [ ] Set up SSL/TLS for all services, including MySQL connections
- [ ] Configure CORS properly in FastAPI
- [ ] Set up monitoring and alerting (Prometheus, Grafana)
- [ ] Enable OpenTelemetry for distributed tracing
- [ ] Use managed services (RDS, DocumentDB, MSK) instead of Docker containers
- [ ] Set up backups for databases
- [ ] Implement rate limiting on API endpoints
- [ ] Configure log retention policies
- [ ] Review and adjust binlog retention (currently 7 days)
- [ ] Set up binary log backup strategy
- [ ] Enable MySQL audit plugin for compliance
- [ ] Configure network policies for security
- [ ] Test disaster recovery procedures

## Files Summary

### Configuration
- `docker/mysql.cnf` - MySQL binlog configuration
- `docker/mysql-init.sql` - CDC permissions initialization
- `debezium/integration-connector.json` - Debezium connector config
- `k8s/deployments/mysql-statefulset.yaml` - Kubernetes MySQL deployment

### Scripts
- `debezium/register_connector.py` - Register Debezium connector
- `control_plane/app/services/kafka_consumer_service.py` - CDC event processor

### Tests
- `control_plane/tests/test_debezium_cdc.py` - CDC integration tests
- `control_plane/tests/test_s3_to_mongo_e2e.py` - E2E test with real CDC

### Documentation
- `DEBEZIUM_CDC_IMPLEMENTATION.md` - Complete CDC implementation guide

## Next Steps

- [API Documentation](http://localhost:8000/docs)

## Support

For issues or questions:
- Check Kafka Connect logs: `docker logs kafka-connect`
- Verify connector status: `curl http://localhost:8083/connectors/integration-cdc-connector/status`
- Review Debezium documentation: https://debezium.io/documentation/
- [IMPLEMENTATION_SUMMARY.md — Database Schema Management](IMPLEMENTATION_SUMMARY.md#database-schema-management-alembic-migrations) - Alembic migration workflow
