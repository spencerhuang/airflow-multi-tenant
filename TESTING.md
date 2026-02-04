# Testing Guide

This guide explains how to run all tests for the Multi-Tenant Airflow Architecture project.

## Prerequisites

1. **Docker Desktop or OrbStack** must be installed and running
2. **Python 3.11+** with virtual environment set up
3. **Dependencies installed**: Run `pip install -r requirements-dev.txt`

## Quick Start

### Option 1: Run All Tests (Recommended)

The easiest way to run all tests including integration and CDC Kafka tests:

```bash
# Make sure Docker is running first!
./run_all_tests.sh
```

This script will:
1. Check if Docker is running
2. Start all Docker services if not already running
3. Wait for services to be healthy
4. Run unit tests
5. Run integration tests with live services
6. Run CDC Kafka tests
7. Generate coverage reports

### Option 2: Step-by-Step Manual Testing

#### Step 1: Start Docker Services

```bash
# Start all services
docker-compose up -d

# Wait for services to be ready (about 60 seconds)
docker-compose ps

# Check logs if needed
docker-compose logs -f
```

#### Step 2: Run Unit Tests

These tests use mocking and don't require Docker services:

```bash
source venv/bin/activate

# Run all unit tests
pytest connectors/tests/test_s3_client.py \
       connectors/tests/test_s3_reader.py \
       connectors/tests/test_mongo_client.py \
       connectors/tests/test_azure_blob_client.py \
       control_plane/tests/test_integration_service.py \
       -v

# With coverage
pytest connectors/tests/ \
       control_plane/tests/test_integration_service.py \
       -v --cov=connectors --cov=control_plane/app/services \
       --cov-report=term-missing
```

#### Step 3: Run Integration Tests

These tests require Docker services to be running:

```bash
# Run integration API tests
pytest control_plane/tests/test_integration_api_live.py -v -s

# Or run the original integration tests
pytest control_plane/tests/test_integration_api.py -v
```

#### Step 4: Run CDC Kafka Tests

These tests verify Kafka functionality:

```bash
# Run CDC Kafka tests
pytest control_plane/tests/test_cdc_kafka.py -v -s
```

## Test Suites

### Unit Tests (73 tests)

Located in:
- `connectors/tests/test_s3_client.py` (10 tests)
- `connectors/tests/test_s3_reader.py` (15 tests)
- `connectors/tests/test_mongo_client.py` (17 tests)
- `connectors/tests/test_azure_blob_client.py` (15 tests)
- `control_plane/tests/test_integration_service.py` (16 tests)

**Coverage:** ~94%

Run with:
```bash
pytest connectors/tests/ control_plane/tests/test_integration_service.py -v
```

### Integration Tests

Located in:
- `control_plane/tests/test_integration_api.py` (SQLite-based)
- `control_plane/tests/test_integration_api_live.py` (MySQL-based, requires Docker)

Run with:
```bash
# Start Docker first
docker-compose up -d

# Run tests
pytest control_plane/tests/test_integration_api_live.py -v -s
```

### CDC Kafka Tests

Located in:
- `control_plane/tests/test_cdc_kafka.py`

Tests:
- Kafka connectivity
- Event publishing and consuming
- Multiple event types (integration.created, integration.updated, etc.)
- Event ordering
- Error handling

Run with:
```bash
# Start Docker first
docker-compose up -d

# Run tests
pytest control_plane/tests/test_cdc_kafka.py -v -s
```

## Docker Services

The test environment uses these services (defined in `docker-compose.yml`):

| Service | Port | Description |
|---------|------|-------------|
| MySQL | 3306 | Control Plane database |
| PostgreSQL | 5432 | Airflow metadata database |
| MongoDB | 27017 | Destination database |
| MinIO | 9000, 9001 | S3-compatible storage |
| Kafka | 9092 | CDC message broker |
| Zookeeper | 2181 | Kafka coordination |
| Kafka Connect | 8083 | Debezium CDC connectors |
| Kafka UI | 8081 | Kafka monitoring UI |
| Debezium UI | 8088 | CDC connector management UI |
| Airflow Webserver | 8080 | Airflow UI |
| Airflow Scheduler | - | DAG scheduler |
| Control Plane API | 8000 | REST API |

## UI Access

After starting Docker services with `docker-compose up -d`, you can access these web interfaces:

| Service | URL | Credentials | Description |
|---------|-----|-------------|-------------|
| **Airflow UI** | http://localhost:8080 | Username: `airflow`<br>Password: `airflow` | Manage and monitor DAGs, tasks, and workflow runs |
| **Control Plane API** | http://localhost:8000/docs | None (Swagger UI) | REST API documentation and testing |
| **Kafka UI** | http://localhost:8081 | None | Monitor Kafka topics, messages, consumer groups, and connectors |
| **Debezium UI** | http://localhost:8088 | None | Create and manage CDC connectors for MySQL and MongoDB |
| **MinIO Console** | http://localhost:9001 | Username: `minioadmin`<br>Password: `minioadmin` | Manage S3-compatible object storage buckets |
| **Kafka Connect API** | http://localhost:8083 | None (REST API) | Debezium connector REST API |

### Quick Access Guide

**For Airflow Development:**
- Open http://localhost:8080 (airflow/airflow)
- View DAG runs, task logs, and XCom data

**For Kafka/CDC Monitoring:**
- Open http://localhost:8081 for Kafka UI
  - Browse topics and messages
  - Monitor consumer lag
  - View connector status
- Open http://localhost:8088 for Debezium UI
  - Create MySQL/MongoDB CDC connectors
  - Monitor connector health and metrics

**For API Testing:**
- Open http://localhost:8000/docs for interactive API docs
- Test integration endpoints directly from browser

**For Storage Management:**
- Open http://localhost:9001 (minioadmin/minioadmin)
- Create buckets and upload test data

### Service Health Checks

Check if services are healthy:

```bash
# All services
docker-compose ps

# Specific service logs
docker-compose logs -f control-plane
docker-compose logs -f kafka
docker-compose logs -f airflow-webserver

# API health check
curl http://localhost:8000/api/v1/health
```

## Coverage Reports

### View Coverage Report

After running tests with coverage:

```bash
# Terminal output
pytest --cov=connectors --cov=control_plane --cov-report=term-missing

# HTML report
pytest --cov=connectors --cov=control_plane --cov-report=html
open htmlcov/index.html
```

### Current Coverage

| Component | Coverage |
|-----------|----------|
| **Overall** | **94%** |
| connectors/azure_blob/ | 100% |
| connectors/mongo/auth.py | 100% |
| connectors/mongo/client.py | 82% |
| connectors/s3/ | 100% |
| control_plane/app/services/ | 95% |

## Troubleshooting

### Docker Issues

**Problem:** `Cannot connect to Docker daemon`
```bash
# Solution: Start Docker Desktop/OrbStack
# On macOS, open Docker Desktop application
```

**Problem:** Services not starting
```bash
# Check logs
docker-compose logs

# Restart all services
docker-compose down
docker-compose up -d

# Rebuild if needed
docker-compose up -d --build
```

### Test Failures

**Problem:** Integration tests fail with connection errors
```bash
# Wait longer for services to be ready
sleep 60

# Check service health
docker-compose ps
curl http://localhost:8000/api/v1/health
```

**Problem:** Kafka tests timeout
```bash
# Check Kafka logs
docker-compose logs kafka

# Verify Kafka is accessible
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**Problem:** MySQL connection refused
```bash
# Check MySQL is running
docker exec control-plane-mysql mysqladmin ping -h localhost -proot

# Check database exists
docker exec control-plane-mysql mysql -u root -proot -e "SHOW DATABASES;"
```

### Clean Up

```bash
# Stop all services
docker-compose down

# Remove volumes (clean slate)
docker-compose down -v

# Remove all data and restart
docker-compose down -v
docker volume prune -f
docker-compose up -d
```

## CI/CD Integration

For CI/CD pipelines, use this approach:

```yaml
# Example GitHub Actions workflow
- name: Start Docker services
  run: docker-compose up -d

- name: Wait for services
  run: sleep 60

- name: Run tests
  run: |
    source venv/bin/activate
    ./run_all_tests.sh

- name: Upload coverage
  uses: codecov/codecov-action@v3
  with:
    files: ./coverage.xml
```

## Test Development

### Adding New Unit Tests

1. Create test file in appropriate directory:
   - Connector tests: `connectors/tests/test_*.py`
   - Service tests: `control_plane/tests/test_*.py`

2. Use mocking to avoid external dependencies:
   ```python
   from unittest.mock import Mock, patch

   @patch("module.external_dependency")
   def test_something(mock_dependency):
       # Test implementation
   ```

3. Run your new tests:
   ```bash
   pytest path/to/test_file.py -v
   ```

### Adding New Integration Tests

1. Add to `test_integration_api_live.py` or create new file
2. Use `wait_for_services` fixture
3. Clean up test data in teardown
4. Run with Docker services

### Adding New CDC Tests

1. Add to `test_cdc_kafka.py`
2. Use `kafka_producer` and `kafka_consumer` fixtures
3. Test event structure and ordering
4. Verify message delivery

## Additional Commands

```bash
# Run specific test file
pytest control_plane/tests/test_cdc_kafka.py::TestKafkaConnectivity -v

# Run tests matching pattern
pytest -k "kafka" -v

# Run with verbose output
pytest -v -s

# Run only failed tests from last run
pytest --lf

# Run tests in parallel (requires pytest-xdist)
pytest -n auto
```

## Next Steps

- [Architecture Documentation](docs/ARCHITECTURE.md)
- [Setup Guide](SETUP.md)
- [API Documentation](http://localhost:8000/docs)
- [Contributing Guidelines](CONTRIBUTING.md)
