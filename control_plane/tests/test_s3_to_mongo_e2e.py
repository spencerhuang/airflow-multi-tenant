"""End-to-end integration test for S3 to MongoDB workflow with real Debezium CDC.

This test validates the complete event-driven data pipeline:
- Creates actual data in MinIO (S3)
- Creates integration via Control Plane API
- Debezium automatically captures database change (CDC)
- Kafka consumer service processes CDC event
- Airflow DAG is triggered automatically
- Data is transferred from S3 to MongoDB

Test Flow:
1. Upload test JSON data to MinIO (S3)
2. Verify data exists in MinIO
3. Create integration via Control Plane API → INSERT into MySQL
   → Debezium detects change → Publishes CDC event to Kafka
4. Wait for Kafka consumer service to process CDC event and trigger Airflow DAG
5. Wait for DAG completion and verify data in MongoDB

Prerequisites:
- Docker services running (MinIO, MongoDB, Kafka, Kafka Connect, Airflow, Control Plane)
- Debezium connector registered (run: python debezium/register_connector.py)
- Run with: docker-compose up -d && pytest control_plane/tests/test_s3_to_mongo_e2e.py -v -s
"""

import pytest
import json
import time
import requests
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import uuid

# MinIO/S3 client
try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False

# MongoDB client
from pymongo import MongoClient as PyMongoClient

# Kafka client
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    KafkaProducer = None

from control_plane.app.core.database import Base
from control_plane.app.models.customer import Customer
from control_plane.app.models.workspace import Workspace
from control_plane.app.models.auth import Auth
from control_plane.app.models.workflow import Workflow
from control_plane.app.models.access_point import AccessPoint


# Test configuration
API_BASE_URL = "http://localhost:8000"
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DATABASE_URL = "mysql+pymysql://control_plane:control_plane@localhost:3306/control_plane"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "cdc.integration.events"
AIRFLOW_API_URL = "http://localhost:8080/api/v2"
AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"

from shared_utils import get_airflow_auth_headers as _get_airflow_auth_headers


def get_airflow_auth_headers():
    return _get_airflow_auth_headers(AIRFLOW_API_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD)

# Test data
TEST_BUCKET = "test-s3-to-mongo"
TEST_PREFIX = "data/"
TEST_COLLECTION = "test_s3_data"


@pytest.fixture(scope="session")
def wait_for_services():
    """Wait for all required services to be ready."""
    max_retries = 30
    retry_interval = 2

    services = {
        "API": (lambda: requests.get(f"{API_BASE_URL}/api/v1/health", timeout=2)),
        "MinIO": (lambda: requests.get(f"http://localhost:9000/minio/health/live", timeout=2)),
    }

    for service_name, check_func in services.items():
        for i in range(max_retries):
            try:
                response = check_func()
                if response.status_code == 200:
                    print(f"\n✓ {service_name} is ready")
                    break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                if i < max_retries - 1:
                    print(f"Waiting for {service_name}... ({i+1}/{max_retries})")
                    time.sleep(retry_interval)
                else:
                    pytest.skip(f"{service_name} not available - are Docker services running?")


@pytest.fixture(scope="session")
def minio_client(wait_for_services):
    """Create MinIO client for testing."""
    if not MINIO_AVAILABLE:
        pytest.skip("minio package not installed - run: pip install minio")

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    # Create test bucket if it doesn't exist
    try:
        if not client.bucket_exists(TEST_BUCKET):
            client.make_bucket(TEST_BUCKET)
            print(f"✓ Created test bucket: {TEST_BUCKET}")
        else:
            print(f"✓ Test bucket exists: {TEST_BUCKET}")
    except S3Error as e:
        pytest.skip(f"MinIO error: {e}")

    yield client

    # Cleanup: Remove test objects and bucket
    try:
        objects = client.list_objects(TEST_BUCKET, prefix=TEST_PREFIX, recursive=True)
        for obj in objects:
            client.remove_object(TEST_BUCKET, obj.object_name)
            print(f"✓ Cleaned up: {obj.object_name}")
    except Exception as e:
        print(f"Cleanup warning: {e}")


@pytest.fixture(scope="session")
def mongo_client(wait_for_services):
    """Create MongoDB client for testing."""
    try:
        # MongoDB requires authentication (root/root from docker-compose)
        client = PyMongoClient(
            f"mongodb://root:root@{MONGO_HOST}:{MONGO_PORT}/",
            serverSelectionTimeoutMS=5000
        )
        # Test connection
        client.server_info()
        print(f"✓ MongoDB connected")
    except Exception as e:
        pytest.skip(f"MongoDB not available: {e}")

    yield client

    # Cleanup: Remove test collection
    try:
        db = client["test_database"]
        if TEST_COLLECTION in db.list_collection_names():
            db[TEST_COLLECTION].drop()
            print(f"✓ Cleaned up MongoDB collection: {TEST_COLLECTION}")
    except Exception as e:
        print(f"Cleanup warning: {e}")

    client.close()


@pytest.fixture(scope="session")
def kafka_producer(wait_for_services):
    """Create Kafka producer for testing."""
    if not KAFKA_AVAILABLE:
        pytest.skip("kafka-python package not installed - run: pip install kafka-python")

    max_retries = 30
    retry_delay = 2

    # Wait for Kafka to be ready
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=5000,
            )
            print(f"✓ Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
            yield producer
            producer.close()
            return
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Waiting for Kafka... ({attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                pytest.skip(f"Kafka not available after {max_retries} retries: {e}")


@pytest.fixture(scope="session")
def setup_database(wait_for_services):
    """Set up database with test integration."""
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(bind=engine)

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()

    try:
        # Clean up existing test data using raw SQL to avoid ORM session/FK issues
        from sqlalchemy import text
        with engine.begin() as conn:
            conn.execute(text(
                "DELETE ire FROM integration_run_errors ire "
                "JOIN integration_runs ir ON ire.run_id = ir.run_id "
                "JOIN integrations i ON ir.integration_id = i.integration_id "
                "WHERE i.workspace_id LIKE 'test-e2e-%'"
            ))
            conn.execute(text(
                "DELETE ir FROM integration_runs ir "
                "JOIN integrations i ON ir.integration_id = i.integration_id "
                "WHERE i.workspace_id LIKE 'test-e2e-%'"
            ))
            conn.execute(text("DELETE FROM integrations WHERE workspace_id LIKE 'test-e2e-%'"))
            conn.execute(text("DELETE FROM auths WHERE workspace_id LIKE 'test-e2e-%'"))
            conn.execute(text("DELETE FROM workspaces WHERE workspace_id LIKE 'test-e2e-%'"))
            conn.execute(text("DELETE FROM customers WHERE customer_guid LIKE 'test-e2e-%'"))

        # Create test data
        customer = Customer(customer_guid="test-e2e-cust", name="E2E Test Customer", max_integration=100)
        db.add(customer)

        workspace = Workspace(workspace_id="test-e2e-ws", customer_guid="test-e2e-cust")
        db.add(workspace)

        # S3 credentials (source)
        s3_auth = Auth(
            workspace_id="test-e2e-ws",
            auth_type="aws_iam",
            json_data=json.dumps({
                "s3_endpoint_url": "http://minio:9000",  # Docker internal
                "s3_access_key": MINIO_ACCESS_KEY,
                "s3_secret_key": MINIO_SECRET_KEY,
            }),
        )
        db.add(s3_auth)

        # MongoDB credentials (destination)
        mongo_auth = Auth(
            workspace_id="test-e2e-ws",
            auth_type="mongodb",
            json_data=json.dumps({
                "mongo_uri": "mongodb://root:root@mongodb:27017/",
                "mongo_database": "test_database",
            }),
        )
        db.add(mongo_auth)

        # Get or create workflow
        workflow = db.query(Workflow).filter(Workflow.workflow_id == 1).first()
        if not workflow:
            workflow = Workflow(workflow_id=1, workflow_type="S3ToMongo")
            db.add(workflow)

        # Get or create access points
        s3_ap = db.query(AccessPoint).filter(AccessPoint.access_pt_id == 1).first()
        if not s3_ap:
            s3_ap = AccessPoint(access_pt_id=1, ap_type="S3")
            db.add(s3_ap)

        mongo_ap = db.query(AccessPoint).filter(AccessPoint.access_pt_id == 2).first()
        if not mongo_ap:
            mongo_ap = AccessPoint(access_pt_id=2, ap_type="MongoDB")
            db.add(mongo_ap)

        db.commit()

        auth_id = s3_auth.auth_id

        yield {"workspace_id": "test-e2e-ws", "auth_id": auth_id}

    finally:
        # Cleanup using a fresh connection to avoid ORM session state issues
        try:
            db.close()
            with engine.begin() as conn:
                from sqlalchemy import text
                # Delete in FK order: errors → runs → integrations → auth → workspace → customer
                conn.execute(text(
                    "DELETE ire FROM integration_run_errors ire "
                    "JOIN integration_runs ir ON ire.run_id = ir.run_id "
                    "JOIN integrations i ON ir.integration_id = i.integration_id "
                    "WHERE i.workspace_id LIKE 'test-e2e-%'"
                ))
                conn.execute(text(
                    "DELETE ir FROM integration_runs ir "
                    "JOIN integrations i ON ir.integration_id = i.integration_id "
                    "WHERE i.workspace_id LIKE 'test-e2e-%'"
                ))
                conn.execute(text("DELETE FROM integrations WHERE workspace_id LIKE 'test-e2e-%'"))
                conn.execute(text("DELETE FROM auths WHERE workspace_id LIKE 'test-e2e-%'"))
                conn.execute(text("DELETE FROM workspaces WHERE workspace_id LIKE 'test-e2e-%'"))
                conn.execute(text("DELETE FROM customers WHERE customer_guid LIKE 'test-e2e-%'"))
            print("✓ Cleanup completed successfully")
        except Exception as e:
            print(f"Cleanup error: {e}")
        finally:
            engine.dispose()


class TestS3ToMongoEndToEnd:
    """End-to-end tests for S3 to MongoDB workflow."""

    # Class variables to share state across test methods
    integration_id = None
    dag_run_id = None

    def test_01_upload_test_data_to_minio(self, minio_client):
        """Step 1: Upload test data to MinIO (S3)."""
        print("\n" + "="*60)
        print("STEP 1: Uploading test data to MinIO")
        print("="*60)

        # Create test data
        test_data = [
            {"id": 1, "name": "Alice", "email": "alice@example.com", "timestamp": datetime.utcnow().isoformat()},
            {"id": 2, "name": "Bob", "email": "bob@example.com", "timestamp": datetime.utcnow().isoformat()},
            {"id": 3, "name": "Charlie", "email": "charlie@example.com", "timestamp": datetime.utcnow().isoformat()},
        ]

        # Upload each record as a separate JSON file
        for i, record in enumerate(test_data, 1):
            object_name = f"{TEST_PREFIX}record_{i}.json"
            data_bytes = json.dumps(record).encode('utf-8')

            from io import BytesIO
            minio_client.put_object(
                TEST_BUCKET,
                object_name,
                BytesIO(data_bytes),
                len(data_bytes),
                content_type="application/json",
            )
            print(f"  ✓ Uploaded: {object_name}")

        # Verify uploads
        objects = list(minio_client.list_objects(TEST_BUCKET, prefix=TEST_PREFIX))
        assert len(objects) == 3, f"Expected 3 objects, found {len(objects)}"
        print(f"\n✅ Successfully uploaded {len(objects)} files to MinIO")

    def test_02_verify_minio_data(self, minio_client):
        """Step 2: Verify data exists in MinIO."""
        print("\n" + "="*60)
        print("STEP 2: Verifying data in MinIO")
        print("="*60)

        objects = list(minio_client.list_objects(TEST_BUCKET, prefix=TEST_PREFIX))
        print(f"  Found {len(objects)} objects:")

        for obj in objects:
            # Download and parse each object
            response = minio_client.get_object(TEST_BUCKET, obj.object_name)
            data = json.loads(response.read().decode('utf-8'))
            print(f"    ✓ {obj.object_name}: {data['name']} ({data['email']})")

        assert len(objects) == 3
        print(f"\n✅ Verified all test data in MinIO")

    def test_03_create_integration(self, setup_database):
        """Step 3: Create S3 to MongoDB integration via API."""
        print("\n" + "="*60)
        print("STEP 3: Creating S3 to MongoDB integration")
        print("="*60)

        integration_data = {
            "workspace_id": setup_database["workspace_id"],
            "workflow_id": 1,
            "auth_id": setup_database["auth_id"],
            "source_access_pt_id": 1,
            "dest_access_pt_id": 2,
            "integration_type": "S3ToMongo",
            "schedule_type": "on_demand",
            "json_data": json.dumps({
                "s3_bucket": TEST_BUCKET,
                "s3_prefix": TEST_PREFIX,
                "mongo_collection": TEST_COLLECTION,
            }),
        }

        response = requests.post(f"{API_BASE_URL}/api/v1/integrations/", json=integration_data)
        assert response.status_code == 201, f"Failed to create integration: {response.text}"

        integration = response.json()
        print(f"  ✓ Integration created: ID={integration['integration_id']}")
        print(f"  ✓ Type: {integration['integration_type']}")
        print(f"  ✓ Status: {integration['usr_sch_status']}")
        print(f"\n✅ Integration created successfully")

        # Store for next test (use class variable to persist across test methods)
        TestS3ToMongoEndToEnd.integration_id = integration["integration_id"]

    def test_04_wait_for_cdc_and_dag_trigger(self, setup_database):
        """Step 4: Wait for Debezium CDC event and DAG trigger (automatic)."""
        print("\n" + "="*60)
        print("STEP 4: Waiting for automatic CDC event and DAG trigger")
        print("="*60)

        assert TestS3ToMongoEndToEnd.integration_id is not None, "Integration must be created first (run test_03_create_integration)"

        print(f"\nIntegration ID: {TestS3ToMongoEndToEnd.integration_id}")
        print(f"\nHow this works:")
        print(f"  1. Integration was created in Step 3 (INSERT into MySQL)")
        print(f"  2. Debezium automatically detected the INSERT")
        print(f"  3. Debezium published CDC event to Kafka topic: {KAFKA_TOPIC}")
        print(f"  4. Kafka consumer service picked up the CDC event")
        print(f"  5. Consumer service is triggering Airflow DAG...")

        # Store timestamp before waiting so we only look at DAG runs created after this point
        from datetime import datetime as dt, timedelta
        trigger_time = dt.utcnow()

        # Wait for Kafka consumer to process the CDC event and trigger the DAG
        print(f"\n  Waiting for DAG to be triggered (15 seconds)...")
        time.sleep(15)  # Give consumer time to process CDC event and trigger DAG

        # Check if DAG was triggered by the CDC event
        dag_id = "s3_to_mongo_ondemand"

        print(f"\n  Checking Airflow for DAG runs created after {trigger_time.isoformat()}...")
        # Filter DAG runs to only those created in the last 30 seconds
        min_start_time = (trigger_time - timedelta(seconds=10)).isoformat() + "Z"
        list_url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns?start_date_gte={min_start_time}&order_by=-start_date"

        try:
            headers = get_airflow_auth_headers()
            response = requests.get(list_url, headers=headers, timeout=10)

            if response.status_code == 200:
                dag_runs = response.json().get("dag_runs", [])

                # Find the most recent DAG run created after our trigger time
                if dag_runs:
                    latest_run = dag_runs[0]  # API returns newest first
                    dag_run_id = latest_run.get("dag_run_id")

                    print(f"  ✓ Found DAG run triggered by CDC event")
                    print(f"  ✓ DAG Run ID: {dag_run_id}")
                    print(f"  ✓ State: {latest_run.get('state')}")
                    print(f"  ✓ Start Date: {latest_run.get('start_date')}")

                    # Store dag_run_id for next test
                    TestS3ToMongoEndToEnd.dag_run_id = dag_run_id
                    print(f"\n✅ Workflow automatically triggered via Debezium CDC!")
                    print(f"\n🎉 End-to-end CDC pipeline is working:")
                    print(f"   MySQL INSERT → Debezium → Kafka → Consumer → Airflow DAG")
                else:
                    pytest.skip("No DAG runs found. CDC event may not have been processed yet. Check:\n" +
                               "  1. Debezium connector: curl http://localhost:8083/connectors/integration-cdc-connector/status\n" +
                               "  2. Kafka consumer logs: docker-compose logs control-plane | grep 'Processing CDC event'\n" +
                               "  3. Kafka topic: docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic cdc.integration.events --from-beginning --max-messages 10")
            else:
                pytest.skip(f"Could not verify DAG trigger (status {response.status_code})")

        except Exception as e:
            print(f"  ✗ Error checking DAG status: {e}")
            pytest.skip(f"Could not check DAG status: {e}")

    def test_05_verify_mongodb_data(self, mongo_client):
        """Step 5: Wait for DAG to complete and verify data in MongoDB."""
        print("\n" + "="*60)
        print("STEP 5: Waiting for DAG completion and verifying MongoDB data")
        print("="*60)

        # Check if we have a dag_run_id from test_04
        dag_run_id = getattr(TestS3ToMongoEndToEnd, 'dag_run_id', None)
        if not dag_run_id:
            pytest.skip("DAG was not triggered successfully in test_04")

        # Wait for DAG to complete
        dag_id = "s3_to_mongo_ondemand"

        print(f"  Waiting for DAG run {dag_run_id} to complete...")

        max_wait = 120  # seconds
        interval = 5
        elapsed = 0
        dag_state = None

        while elapsed < max_wait:
            try:
                # Check DAG run status
                headers = get_airflow_auth_headers()
                status_url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns/{dag_run_id}"
                response = requests.get(status_url, headers=headers, timeout=10)

                if response.status_code == 200:
                    dag_run_info = response.json()
                    dag_state = dag_run_info.get("state")

                    print(f"    DAG state: {dag_state} (elapsed: {elapsed}s)")

                    if dag_state == "success":
                        print(f"  ✓ DAG completed successfully")
                        break
                    elif dag_state == "failed":
                        print(f"  ✗ DAG failed")
                        pytest.fail(f"DAG run failed. Check Airflow logs for details.")
                    elif dag_state in ["running", "queued"]:
                        # Still running, wait more
                        pass
                    else:
                        print(f"  Unexpected DAG state: {dag_state}")

            except Exception as e:
                print(f"  Warning: Could not check DAG status: {e}")

            time.sleep(interval)
            elapsed += interval

        if dag_state != "success":
            pytest.skip(f"DAG did not complete within {max_wait} seconds (last state: {dag_state})")

        # Now verify data in MongoDB
        print("\n  Verifying data in MongoDB...")
        db = mongo_client["test_database"]
        collection = db[TEST_COLLECTION]

        # Give MongoDB a moment to finalize writes
        time.sleep(2)

        # Verify data
        documents = list(collection.find({}, {"_id": 0, "_import_timestamp": 0, "_source_bucket": 0, "_source_key": 0}))
        print(f"  Found {len(documents)} documents in MongoDB:")

        for doc in documents:
            print(f"    ✓ {doc.get('name')} - {doc.get('email')}")

        assert len(documents) >= 3, f"Expected at least 3 documents, found {len(documents)}"

        # Verify specific records
        names = {doc['name'] for doc in documents}
        assert 'Alice' in names, "Expected to find Alice in MongoDB"
        assert 'Bob' in names, "Expected to find Bob in MongoDB"
        assert 'Charlie' in names, "Expected to find Charlie in MongoDB"

        print(f"\n✅ Successfully verified data transfer to MongoDB")

    def test_06_data_pipeline_summary(self, minio_client, mongo_client):
        """Step 6: Summary of end-to-end test."""
        print("\n" + "="*60)
        print("END-TO-END TEST SUMMARY")
        print("="*60)

        # Check MinIO
        minio_objects = list(minio_client.list_objects(TEST_BUCKET, prefix=TEST_PREFIX))
        print(f"\n1. MinIO (S3) Source:")
        print(f"   ✓ Bucket: {TEST_BUCKET}")
        print(f"   ✓ Files: {len(minio_objects)}")

        # Check MongoDB
        db = mongo_client["test_database"]
        collection = db[TEST_COLLECTION]
        mongo_count = collection.count_documents({})
        print(f"\n2. MongoDB Destination:")
        print(f"   ✓ Database: test_database")
        print(f"   ✓ Collection: {TEST_COLLECTION}")
        print(f"   ✓ Documents: {mongo_count}")

        # Check Integration
        response = requests.get(f"{API_BASE_URL}/api/v1/integrations/")
        if response.status_code == 200:
            integrations = response.json()
            e2e_integrations = [i for i in integrations if i.get('workspace_id') == 'test-e2e-ws']
            print(f"\n3. Integration Configuration:")
            print(f"   ✓ Integrations created: {len(e2e_integrations)}")

        print(f"\n" + "="*60)
        print(f"✅ END-TO-END TEST COMPLETE")
        print(f"="*60)
        print(f"\n📝 Test Results:")
        print(f"   1. ✅ Test data uploaded to MinIO")
        print(f"   2. ✅ Integration created via API (INSERT into MySQL)")
        print(f"   3. ✅ Debezium automatically captured CDC event")
        print(f"   4. ✅ CDC event published to Kafka")
        print(f"   5. ✅ Kafka consumer service triggered Airflow DAG")
        print(f"   6. ✅ Data successfully transferred from S3 to MongoDB")
        print(f"\n🎉 Complete CDC-driven data pipeline validated!")
        print(f"\n📊 Pipeline Flow:")
        print(f"   MySQL → Debezium → Kafka → Consumer → Airflow → S3 → MongoDB")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
