# Setup Guide

This guide will help you set up the Multi-Tenant Airflow Architecture locally.

## Prerequisites

- Docker and Docker Compose (v2.0+)
- Python 3.11+
- Make (optional, for convenience commands)

## Quick Start

### 1. Clone and Setup

```bash
cd airflow-multi-tenant
make setup
```

This will:
- Create a `.env` file from `.env.example`
- Install Python dependencies

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

## Database Setup

### Control Plane Database Schema

The control plane uses MySQL. Tables are managed via Alembic migrations.

To create tables and seed initial data:

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

### Reset Everything

```bash
make docker-down
docker volume prune
make docker-up
```

## Production Considerations

Before deploying to production:

1. **Change all default passwords** in `.env` file
2. **Enable authentication** (`AUTH_ENABLED=true`)
3. **Use proper secret management** (AWS Secrets Manager, HashiCorp Vault)
4. **Set up SSL/TLS** for all services
5. **Configure CORS** properly in FastAPI
6. **Set up monitoring** (Prometheus, Grafana)
7. **Enable OpenTelemetry** for distributed tracing
8. **Use managed services** (RDS, DocumentDB, MSK) instead of Docker containers
9. **Set up backups** for databases
10. **Implement rate limiting** on API endpoints

## Next Steps

- [Architecture Documentation](docs/ARCHITECTURE.md)
- [API Documentation](http://localhost:8000/docs)
- [Adding New Workflows](docs/ADDING_WORKFLOWS.md)
- [CDC Setup Guide](docs/CDC_SETUP.md)
