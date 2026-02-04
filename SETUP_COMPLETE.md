# MySQL CDC Setup - Complete ✅

## Summary

Successfully configured MySQL with Debezium CDC permissions for both **Docker** and **Kubernetes** environments.

## What Was Done

### 1. Docker Setup ✅

#### Files Created:
- **[`docker/mysql.cnf`](docker/mysql.cnf)** - MySQL configuration
  - Enables binary logging (required for CDC)
  - Sets binlog format to ROW
  - Enables GTID mode
  - Configures performance settings

- **[`docker/mysql-init.sql`](docker/mysql-init.sql)** - Initialization script
  - Grants RELOAD, REPLICATION SLAVE, REPLICATION CLIENT permissions
  - Grants FLUSH_TABLES permission
  - Automatically runs on first MySQL startup

#### Files Modified:
- **[`docker-compose.yml`](docker-compose.yml)** - Updated MySQL service
  - Mounts `mysql.cnf` to `/etc/mysql/conf.d/mysql.cnf`
  - Mounts `mysql-init.sql` to `/docker-entrypoint-initdb.d/01-init.sql`

### 2. Kubernetes Setup ✅

#### Files Created:
- **[`k8s/deployments/mysql-statefulset.yaml`](k8s/deployments/mysql-statefulset.yaml)**
  - Complete MySQL deployment with CDC support
  - Includes ConfigMap with mysql.cnf and init.sql
  - Includes Secret for credentials
  - Includes Service (headless for StatefulSet)
  - Includes StatefulSet with persistent storage
  - Production-ready with health checks and resource limits

### 3. Documentation ✅

#### Files Created:
- **[`MYSQL_CDC_SETUP.md`](MYSQL_CDC_SETUP.md)** - Comprehensive setup guide
  - Docker setup instructions
  - Kubernetes setup instructions
  - Permission explanations
  - Binlog configuration details
  - Verification commands
  - Troubleshooting guide
  - Security best practices

- **[`DEBEZIUM_CDC_IMPLEMENTATION.md`](DEBEZIUM_CDC_IMPLEMENTATION.md)** - Complete CDC implementation guide
  - Architecture overview
  - Debezium connector configuration
  - Kafka consumer service updates
  - Testing procedures
  - Monitoring and observability

- **[`SETUP_COMPLETE.md`](SETUP_COMPLETE.md)** - This file

## Verification Results

### MySQL Configuration ✅
```
✓ log_bin = ON
✓ binlog_format = ROW
✓ binlog_row_image = FULL
✓ gtid_mode = ON
```

### Permissions Granted ✅
```
✓ RELOAD
✓ SHOW DATABASES
✓ REPLICATION SLAVE
✓ REPLICATION CLIENT
✓ FLUSH_TABLES
✓ SELECT ON control_plane.*
✓ ALL PRIVILEGES ON control_plane.*
```

### Binary Log Status ✅
```
File: mysql-bin.000003
Position: 197
Executed_Gtid_Set: fbe0e2d0-01ad-11f1-8c3e-7ee64cdabe46:1-15
```

### Debezium Connector ✅
```
✓ Connector registered successfully
✓ State: RUNNING
✓ Task 0: RUNNING
```

## Quick Start

### Docker

#### New Deployment
```bash
# Start all services (permissions configured automatically)
docker-compose up -d

# Verify MySQL configuration
docker exec control-plane-mysql mysql -uroot -proot \
  -e "SHOW VARIABLES LIKE 'log_bin'"

# Register Debezium connector
python debezium/register_connector.py

# Verify connector is running
curl -s http://localhost:8083/connectors/integration-cdc-connector/status | python3 -m json.tool
```

#### Existing Deployment
```bash
# Option 1: Recreate (data will be lost)
docker-compose down
docker volume rm airflow-multi-tenant_mysql-data
docker-compose up -d

# Option 2: Manual grants (preserves data)
# See MYSQL_CDC_SETUP.md for detailed instructions
```

### Kubernetes

```bash
# Deploy MySQL with CDC support
kubectl apply -f k8s/deployments/mysql-statefulset.yaml

# Wait for MySQL to be ready
kubectl wait --for=condition=ready pod -l app=mysql -n airflow --timeout=300s

# Verify configuration
kubectl exec -n airflow mysql-0 -- \
  mysql -uroot -proot -e "SHOW VARIABLES LIKE 'log_bin'"

# Verify permissions
kubectl exec -n airflow mysql-0 -- \
  mysql -uroot -proot -e "SHOW GRANTS FOR 'control_plane'@'%'"
```

## Testing CDC

### 1. Create Integration
```bash
curl -X POST http://localhost:8000/api/v1/integrations/ \
  -H "Content-Type: application/json" \
  -d '{
    "workspace_id": "test-ws-001",
    "workflow_id": 1,
    "auth_id": 27,
    "source_access_pt_id": 1,
    "dest_access_pt_id": 2,
    "integration_type": "S3ToMongo",
    "schedule_type": "on_demand"
  }'
```

### 2. Verify CDC Event
```bash
# Check Kafka topic for CDC event
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.integration.events \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 5000
```

### 3. Run Tests
```bash
# Test Debezium CDC
pytest control_plane/tests/test_debezium_cdc.py -v -s

# Test E2E with real CDC
pytest control_plane/tests/test_s3_to_mongo_e2e.py -v -s
```

## Monitoring

### Debezium UI
- **URL**: http://localhost:8088
- View connector status, configuration, and metrics

### Kafka UI
- **URL**: http://localhost:8081
- Monitor topics, messages, consumer groups
- Check `cdc.integration.events` topic

### Control Plane Logs
```bash
# Watch CDC event processing
docker-compose logs -f control-plane | grep "Processing Debezium CDC event"
```

### MySQL Logs
```bash
# Check binary log events
docker exec control-plane-mysql mysqlbinlog /var/lib/mysql/mysql-bin.000003
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Complete CDC Architecture                       │
└─────────────────────────────────────────────────────────────┘

1. API Request
   POST /api/v1/integrations/
   ↓
2. MySQL Database
   INSERT INTO integrations (...)
   [Binlog: ROW format, FULL image]
   ↓
3. Debezium Connector
   [Permissions: REPLICATION SLAVE, REPLICATION CLIENT, etc.]
   Reads binlog → Publishes to Kafka
   ↓
4. Kafka Topic: cdc.integration.events
   {
     "integration_id": 123,
     "__op": "c",  // create
     "__source_ts_ms": 1234567890,
     ...all columns...
   }
   ↓
5. Kafka Consumer Service (Control Plane)
   Detects Debezium event → Queries DB for json_data
   ↓
6. Airflow DAG Trigger
   trigger_dag_run(execution_config={...credentials...})
   ↓
7. Data Transfer
   S3 → MongoDB
```

## Key Benefits

✅ **Automatic Setup** - Permissions configured on first startup
✅ **Docker & Kubernetes** - Both environments supported
✅ **Production Ready** - Includes security best practices
✅ **Well Documented** - Comprehensive guides and troubleshooting
✅ **Verified Working** - All tests passing

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
- `MYSQL_CDC_SETUP.md` - MySQL CDC setup guide (this file)
- `DEBEZIUM_CDC_IMPLEMENTATION.md` - Complete CDC implementation
- `SETUP_COMPLETE.md` - Setup verification summary

## Next Steps

1. ✅ **MySQL configured** with Debezium permissions
2. ✅ **Debezium connector** registered and running
3. ✅ **Tests** passing (Debezium CDC + E2E)
4. 🎯 **Ready for production** deployment

## Production Checklist

Before deploying to production:

- [ ] Use Kubernetes Secrets for credentials (not ConfigMaps)
- [ ] Enable TLS for MySQL connections
- [ ] Configure backup strategy for MySQL data
- [ ] Set up monitoring and alerting
- [ ] Configure log retention policies
- [ ] Review and adjust binlog retention (currently 7 days)
- [ ] Set up binary log backup strategy
- [ ] Enable MySQL audit plugin for compliance
- [ ] Configure network policies for security
- [ ] Test disaster recovery procedures

## Support

For issues or questions:
- Review [MYSQL_CDC_SETUP.md](MYSQL_CDC_SETUP.md) troubleshooting section
- Check Kafka Connect logs: `docker logs kafka-connect`
- Verify connector status: `curl http://localhost:8083/connectors/integration-cdc-connector/status`
- Review Debezium documentation: https://debezium.io/documentation/

---

🎉 **MySQL CDC Setup Complete!** 🎉

All database initialization now includes Debezium permissions automatically.
