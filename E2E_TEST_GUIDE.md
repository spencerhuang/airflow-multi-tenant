# End-to-End Test Guide: S3 to MongoDB Data Transfer

## Overview

This guide explains the complete end-to-end test that verifies actual data transfer from MinIO (S3) to MongoDB through the Airflow workflow.

## What Was Implemented

### 1. MongoDB Initialization Script
**File:** `docker/mongodb-init.js`

Automatically creates:
- Database: `test_database`
- Collection: `test_s3_data` with JSON schema validation
- Collection: `s3_imports` for import tracking
- Indexes for efficient querying

Runs automatically when MongoDB container starts.

### 2. Actual Data Transfer Implementation
**File:** `airflow/plugins/operators/s3_to_mongo_operators.py`

The `ExecuteS3ToMongoTask` now performs real data transfer:
- Connects to S3/MinIO using boto3
- Lists and reads JSON files from specified bucket/prefix
- Parses JSON data (handles both objects and arrays)
- Connects to MongoDB using pymongo
- Inserts data with metadata (_import_timestamp, _source_bucket, _source_key)
- Returns detailed statistics (files_processed, records_read, records_written, errors)

### 3. Kafka Consumer Service
**File:** `kafka_consumer/app/services/kafka_consumer_service.py`

Standalone FastAPI microservice (port 8001) that:
- Subscribes to Kafka topic `cdc.integration.events`
- Listens for CDC events (integration.created, updated, deleted, etc.)
- Automatically triggers Airflow DAGs when integration.created events are received
- Runs independently from the Control Plane with its own lifecycle
- Provides health endpoints (`/health`, `/health/ready`, `/health/detailed`)

### 4. Complete E2E Test with Event-Driven Architecture
**File:** `control_plane/tests/test_s3_to_mongo_e2e.py`

The test now validates the complete event-driven pipeline:
1. **test_01**: Uploads test JSON data to MinIO
2. **test_02**: Verifies data exists in MinIO
3. **test_03**: Creates integration via Control Plane API
4. **test_04**: Publishes `integration.created` event to Kafka (simulating CDC)
5. **test_05**: Kafka consumer picks up event, triggers DAG, waits for completion, verifies data in MongoDB
6. **test_06**: Provides summary of entire event-driven test

## Prerequisites

### 1. Install Python Packages

The test requires additional packages:

```bash
uv pip install minio boto3 pymongo kafka-python
```

Or update requirements:

```bash
echo "minio" >> requirements-dev.txt
echo "boto3" >> requirements-dev.txt
echo "pymongo" >> requirements-dev.txt
echo "kafka-python" >> requirements-dev.txt
uv pip install -r requirements-dev.txt
```

### 2. Start All Docker Services

```bash
# Stop and remove existing containers (to apply MongoDB init script)
docker-compose down -v

# Start all services
docker-compose up -d

# Wait for services to be ready (about 60 seconds)
sleep 60

# Check service health
docker-compose ps
```

### 3. Verify Airflow DAG is Available

1. Open Airflow UI: http://localhost:8080 (airflow/airflow)
2. Verify `s3_to_mongo_ondemand` DAG is visible
3. Unpause the DAG if it's paused (toggle switch)

## Running the E2E Test

### Full Test Suite

```bash
# Run all E2E tests in order
pytest control_plane/tests/test_s3_to_mongo_e2e.py -v -s
```

Expected output:
```
test_01_upload_test_data_to_minio PASSED
test_02_verify_minio_data PASSED
test_03_create_integration PASSED
test_04_trigger_workflow PASSED
test_05_verify_mongodb_data PASSED
test_06_data_pipeline_summary PASSED
```

### Run Individual Tests

```bash
# Just upload data
pytest control_plane/tests/test_s3_to_mongo_e2e.py::TestS3ToMongoEndToEnd::test_01_upload_test_data_to_minio -v -s

# Full workflow (tests 4-5)
pytest control_plane/tests/test_s3_to_mongo_e2e.py::TestS3ToMongoEndToEnd::test_04_trigger_workflow -v -s
pytest control_plane/tests/test_s3_to_mongo_e2e.py::TestS3ToMongoEndToEnd::test_05_verify_mongodb_data -v -s
```

## What the Test Does

### Test Flow

```
┌──────────────┐
│   MinIO/S3   │  ← Step 1: Upload test JSON files
│ (test bucket)│
└──────────────┘

       ↓  Step 3: Create integration

┌──────────────┐
│ Control Plane│  ← Step 3: Integration created
│     API      │
└──────┬───────┘
       │
       ↓  Step 4: Publish integration.created event

┌──────────────┐
│    Kafka     │  ← Step 4: CDC event published
│  (cdc topic) │     Event: integration.created
└──────┬───────┘
       │
       ↓  Step 5: Consumer processes event

┌──────────────┐
│    Kafka     │  ← Step 5: Background consumer
│  Consumer    │     Triggers Airflow DAG
│  Service     │
└──────┬───────┘
       │
       ↓  Step 5: DAG triggered

┌──────────────┐
│   Airflow    │  ← Step 5: DAG executes tasks
│  (on-demand  │     - Prepare: Extract config
│     DAG)     │     - Validate: Check connectivity
└──────┬───────┘     - Execute: Transfer data
       │             - Cleanup: Finalize
       │
       ↓  Data transfer from MinIO

┌──────────────┐
│   MongoDB    │  ← Step 5: Verify data appears!
│(test_database│
│collection)   │
└──────────────┘
```

### Test Data

The test creates 3 JSON files in MinIO:

**data/record_1.json:**
```json
{
  "id": 1,
  "name": "Alice",
  "email": "alice@example.com",
  "timestamp": "2026-02-04T07:35:00.000000"
}
```

**data/record_2.json:**
```json
{
  "id": 2,
  "name": "Bob",
  "email": "bob@example.com",
  "timestamp": "2026-02-04T07:35:00.000000"
}
```

**data/record_3.json:**
```json
{
  "id": 3,
  "name": "Charlie",
  "email": "charlie@example.com",
  "timestamp": "2026-02-04T07:35:00.000000"
}
```

### MongoDB Result

After the DAG completes, MongoDB contains:

```javascript
db.test_s3_data.find()

// Result:
[
  {
    "_id": ObjectId("..."),
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com",
    "timestamp": "2026-02-04T07:35:00.000000",
    "_import_timestamp": ISODate("2026-02-04T07:40:12.345Z"),
    "_source_bucket": "test-s3-to-mongo",
    "_source_key": "data/record_1.json"
  },
  // ... Alice, Bob, Charlie
]
```

## Verifying Manually

### 1. Check MinIO

```bash
# Using MinIO Console
open http://localhost:9001
# Login: minioadmin / minioadmin
# Navigate to bucket: test-s3-to-mongo
# Check files in data/ folder
```

### 2. Check MongoDB

```bash
# Connect to MongoDB
docker exec -it mongodb mongosh -u root -p root

# Use database
use test_database

# Count documents
db.test_s3_data.countDocuments()

# View documents
db.test_s3_data.find().pretty()

# View with import metadata
db.test_s3_data.find({}, {
  name: 1,
  email: 1,
  _import_timestamp: 1,
  _source_key: 1
})
```

### 3. Check Airflow DAG Run

```bash
# Open Airflow UI
open http://localhost:8080

# View DAG runs
# Click on s3_to_mongo_ondemand DAG
# Check recent runs
# View task logs for each task
```

### 4. Check Control Plane API

```bash
# Get all integrations
curl http://localhost:8000/api/v1/integrations/

# Get specific integration
curl http://localhost:8000/api/v1/integrations/1
```

## Troubleshooting

### Issue: "minio package not installed"

**Solution:**
```bash
uv pip install minio boto3 pymongo
```

### Issue: "Could not connect to Airflow API"

**Check:**
```bash
# Is Airflow webserver running?
curl http://localhost:8080/health

# Check logs
docker-compose logs airflow-webserver
```

**Solution:**
```bash
# Restart Airflow
docker-compose restart airflow-webserver airflow-scheduler
```

### Issue: "DAG s3_to_mongo_ondemand not found"

**Check:**
```bash
# List DAG files
ls -la airflow/dags/

# Check Airflow logs for errors
docker-compose logs airflow-scheduler | grep -i error
```

**Solution:**
```bash
# Restart scheduler to reload DAGs
docker-compose restart airflow-scheduler

# Wait 30 seconds
sleep 30

# Check Airflow UI
open http://localhost:8080
```

### Issue: "DAG failed"

**Check Airflow logs:**
```bash
# View task logs in Airflow UI
# Click on failed task → View Logs

# Or check container logs
docker-compose logs airflow-scheduler
```

**Common causes:**
- Import errors in operator code
- Missing boto3 or pymongo in Airflow container
- Incorrect connection configuration
- MongoDB or MinIO not accessible from Airflow

### Issue: "No data in MongoDB after DAG succeeds"

**Debug:**
```bash
# Check if files exist in MinIO
docker exec -it minio mc ls local/test-s3-to-mongo/data/

# Check MongoDB directly
docker exec -it mongodb mongosh -u root -p root --eval "use test_database; db.test_s3_data.countDocuments()"

# Check Airflow execute task logs
# Look for "records_written" in logs
```

### Issue: "Foreign key constraint error in cleanup"

This has been fixed in the latest version. If you still see it:

```bash
# Pull latest test file
git pull origin main

# The cleanup now deletes integration_runs before integrations
```

## Architecture

### Component Interaction

```
┌─────────────────┐
│   E2E Test      │ ──────┐
│   (pytest)      │       │
└─────────────────┘       │
                          │ 1. Create Integration
                          ↓
                   ┌──────────────┐
                   │ Control Plane│
                   │     API      │
                   └──────┬───────┘
                          │ 2. Store in DB
                          ↓
                   ┌──────────────┐
                   │    MySQL     │
                   └──────────────┘

┌─────────────────┐
│   E2E Test      │ ──────┐
│   (pytest)      │       │ 3. Trigger DAG
└─────────────────┘       │ via REST API
                          ↓
                   ┌──────────────┐
                   │   Airflow    │
                   │   REST API   │
                   └──────┬───────┘
                          │ 4. Create DAG Run
                          ↓
                   ┌──────────────┐
                   │   Scheduler  │
                   └──────┬───────┘
                          │ 5. Execute Tasks
                          ↓
            ┌─────────────────────────┐
            │   s3_to_mongo_ondemand  │
            │         (DAG)           │
            └─────┬───────────────┬───┘
                  │               │
      ┌───────────┤   ┌──────────┤
      │           │   │          │
      ↓           ↓   ↓          ↓
  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
  │Prepare │→│Validate│→│Execute │→│Cleanup │
  └────────┘ └────────┘ └───┬────┘ └────────┘
                            │
                            │ 6. Transfer Data
              ┌─────────────┼─────────────┐
              │             │             │
              ↓             ↓             ↓
         ┌────────┐    ┌────────┐   ┌────────┐
         │ MinIO  │    │ boto3  │   │MongoDB │
         │  (S3)  │    │        │   │        │
         └────────┘    └────────┘   └────────┘
```

## Next Steps

1. **Extend to other workflows:**
   - Azure Blob to MongoDB
   - S3 to PostgreSQL
   - MongoDB to S3 (reverse)

2. **Add CDC testing:**
   - Test Kafka event publishing
   - Verify Debezium connectors
   - Test event-driven triggers

3. **Performance testing:**
   - Large file sizes (MB, GB)
   - Many files (100s, 1000s)
   - Concurrent DAG runs

4. **Error handling:**
   - Invalid JSON files
   - Missing files
   - Connection failures
   - Partial failures

## Summary

The E2E test now provides complete validation of the S3 to MongoDB data pipeline:

✅ Real data upload to MinIO
✅ Integration creation via API
✅ DAG triggering via Airflow API
✅ Actual data transfer (S3 → MongoDB)
✅ Data verification in MongoDB
✅ Comprehensive test reporting

This ensures the entire stack works correctly from end to end!
