"""End-to-end test for Airflow cron-scheduled DAG execution via Controller DAG.

This test validates the full scheduling pipeline:
1. Airflow's scheduler triggers the Controller DAG on cron schedule
2. The controller queries the control plane DB for due integrations (utc_next_run <= now)
3. For each due integration, it triggers s3_to_mongo_ondemand via TriggerDagRunOperator (DTM)
4. The ondemand DAG processes S3 data into MongoDB

Test Flow:
1. Seed control plane MySQL with test customer, workspace, auth, integration (with utc_next_run in the past)
2. Upload test data to MinIO (S3)
3. Deploy a test controller DAG with schedule="* * * * *"
4. Wait for Airflow's scheduler to trigger the controller
5. Wait for the dispatched ondemand DAG to complete
6. Verify data in MongoDB
7. Clean up all test state

Prerequisites:
- Docker services running (MinIO, MongoDB, MySQL, Airflow)
- Run with: pytest control_plane/tests/test_cron_scheduled_e2e.py -v -s
"""

import atexit
import json
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO

import pymysql
import pytest
import requests
from pymongo import MongoClient as PyMongoClient

try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False

from shared_utils import get_airflow_auth_headers

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AIRFLOW_API_URL = "http://localhost:8080/api/v2"
AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MONGO_HOST = "localhost"
MONGO_PORT = 27017

# MySQL control plane (host-accessible)
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "control_plane"
MYSQL_PASSWORD = "control_plane"
MYSQL_DB = "control_plane"

# Test-specific constants
TEST_BUCKET = "test-cron-e2e"
TEST_PREFIX = "cron-data/"
TEST_COLLECTION = "test_cron_e2e"
CONTROLLER_DAG_ID = "s3_to_mongo_controller_e2e_test"
ONDEMAND_DAG_ID = "s3_to_mongo_ondemand"

# Unique IDs for test data (avoid collision with real data)
TEST_CUSTOMER_GUID = "e2e-test-cust-" + uuid.uuid4().hex[:8]
TEST_WORKSPACE_ID = "e2e-test-ws-" + uuid.uuid4().hex[:8]

DAG_FILE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "airflow", "dags", f"{CONTROLLER_DAG_ID}.py"
)

# ---------------------------------------------------------------------------
# The temporary controller DAG file content (DTM pattern)
# ---------------------------------------------------------------------------
DAG_FILE_CONTENT = f'''\
"""Temporary controller DAG for cron scheduling e2e test. Auto-generated."""
from datetime import datetime, timezone
from airflow.sdk import DAG
from airflow.sdk.definitions.decorators import task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="{CONTROLLER_DAG_ID}",
    description="Temporary controller DAG for cron scheduling e2e test",
    schedule="* * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["test", "e2e", "cron", "controller"],
) as dag:

    @task
    def find_due_integrations() -> list[dict]:
        from operators.dispatch_operators import find_and_prepare_due_integrations
        return find_and_prepare_due_integrations(integration_type="s3_to_mongo")

    due = find_due_integrations()

    TriggerDagRunOperator.partial(
        task_id="trigger_ondemand",
        trigger_dag_id="{ONDEMAND_DAG_ID}",
        wait_for_completion=False,
        reset_dag_run=False,
    ).expand_kwargs(due)
'''


def _airflow_headers():
    return get_airflow_auth_headers(AIRFLOW_API_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD)


def _cleanup_dag_file():
    try:
        if os.path.exists(DAG_FILE_PATH):
            os.remove(DAG_FILE_PATH)
    except OSError:
        pass


def _cleanup_airflow_dag(dag_id):
    """Pause and delete a DAG and its runs from Airflow via API."""
    try:
        headers = _airflow_headers()
        requests.patch(
            f"{AIRFLOW_API_URL}/dags/{dag_id}",
            headers=headers, json={"is_paused": True}, timeout=5,
        )
        r = requests.get(
            f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns",
            headers=headers, timeout=10,
        )
        if r.status_code == 200:
            for run in r.json().get("dag_runs", []):
                requests.delete(
                    f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns/{run['dag_run_id']}",
                    headers=headers, timeout=10,
                )
        requests.delete(
            f"{AIRFLOW_API_URL}/dags/{dag_id}",
            headers=headers, timeout=10,
        )
    except Exception:
        pass


atexit.register(_cleanup_dag_file)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def wait_for_services():
    """Wait for Airflow, MinIO, and MySQL to be reachable."""
    for name, url in [
        ("Airflow", f"{AIRFLOW_API_URL}/version"),
        ("MinIO", "http://localhost:9000/minio/health/live"),
    ]:
        for i in range(30):
            try:
                headers = _airflow_headers() if name == "Airflow" else {}
                r = requests.get(url, headers=headers, timeout=2)
                if r.status_code == 200:
                    print(f"\n  {name} is ready")
                    break
            except (requests.ConnectionError, requests.Timeout):
                pass
            if i == 29:
                pytest.skip(f"{name} not available")
            time.sleep(2)

    # Check MySQL
    for i in range(30):
        try:
            conn = pymysql.connect(
                host=MYSQL_HOST, port=MYSQL_PORT,
                user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB,
            )
            conn.close()
            print(f"  MySQL is ready")
            break
        except Exception:
            if i == 29:
                pytest.skip("MySQL not available")
            time.sleep(2)


@pytest.fixture(scope="session")
def seed_mysql(wait_for_services):
    """Seed control plane MySQL with test customer, workspace, auth, integration."""
    conn = pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB,
        autocommit=True,
    )
    cursor = conn.cursor()

    try:
        # 1. Customer
        cursor.execute(
            "INSERT INTO customers (customer_guid, name) VALUES (%s, %s)",
            (TEST_CUSTOMER_GUID, "E2E Test Customer"),
        )

        # 2. Workspace
        cursor.execute(
            "INSERT INTO workspaces (workspace_id, customer_guid) VALUES (%s, %s)",
            (TEST_WORKSPACE_ID, TEST_CUSTOMER_GUID),
        )

        # 3. Workflow (upsert — may already exist)
        cursor.execute(
            "INSERT IGNORE INTO workflows (workflow_type) VALUES (%s)",
            ("s3_to_mongo",),
        )
        cursor.execute(
            "SELECT workflow_id FROM workflows WHERE workflow_type = %s",
            ("s3_to_mongo",),
        )
        workflow_id = cursor.fetchone()[0]

        # 4. Access points (source=S3, dest=MongoDB)
        cursor.execute(
            "INSERT INTO access_points (ap_type) VALUES (%s)", ("S3",),
        )
        source_ap_id = cursor.lastrowid
        cursor.execute(
            "INSERT INTO access_points (ap_type) VALUES (%s)", ("MongoDB",),
        )
        dest_ap_id = cursor.lastrowid

        # 5. Auth records (S3 credentials + MongoDB credentials)
        s3_creds = json.dumps({
            "s3_endpoint_url": "http://minio:9000",
            "s3_access_key": "minioadmin",
            "s3_secret_key": "minioadmin",
        })
        cursor.execute(
            "INSERT INTO auths (workspace_id, auth_type, json_data) VALUES (%s, %s, %s)",
            (TEST_WORKSPACE_ID, "aws_iam", s3_creds),
        )
        auth_id = cursor.lastrowid

        mongo_creds = json.dumps({
            "mongo_uri": "mongodb://root:root@mongodb:27017/",
            "mongo_database": "test_database",
        })
        cursor.execute(
            "INSERT INTO auths (workspace_id, auth_type, json_data) VALUES (%s, %s, %s)",
            (TEST_WORKSPACE_ID, "mongodb", mongo_creds),
        )

        # 6. Integration
        integration_json = json.dumps({
            "s3_bucket": TEST_BUCKET,
            "s3_prefix": TEST_PREFIX,
            "mongo_collection": TEST_COLLECTION,
        })
        # Set utc_next_run to the past so the controller picks it up immediately
        cursor.execute(
            """INSERT INTO integrations
               (workspace_id, workflow_id, auth_id, source_access_pt_id, dest_access_pt_id,
                integration_type, schedule_type, usr_sch_status, utc_sch_cron, utc_next_run,
                json_data, created_at, updated_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())""",
            (TEST_WORKSPACE_ID, workflow_id, auth_id, source_ap_id, dest_ap_id,
             "s3_to_mongo", "daily", "active", "0 * * * *",
             datetime(2020, 1, 1, tzinfo=timezone.utc),
             integration_json),
        )
        integration_id = cursor.lastrowid

        seed_data = {
            "customer_guid": TEST_CUSTOMER_GUID,
            "workspace_id": TEST_WORKSPACE_ID,
            "workflow_id": workflow_id,
            "source_ap_id": source_ap_id,
            "dest_ap_id": dest_ap_id,
            "auth_id": auth_id,
            "integration_id": integration_id,
        }
        print(f"\n  Seeded MySQL: {seed_data}")

        yield seed_data

    finally:
        # Cleanup: delete in reverse FK order
        cursor.execute(
            "DELETE FROM integration_run_errors WHERE run_id IN "
            "(SELECT run_id FROM integration_runs WHERE integration_id IN "
            "(SELECT integration_id FROM integrations WHERE workspace_id = %s))",
            (TEST_WORKSPACE_ID,),
        )
        cursor.execute(
            "DELETE FROM integration_runs WHERE integration_id IN "
            "(SELECT integration_id FROM integrations WHERE workspace_id = %s)",
            (TEST_WORKSPACE_ID,),
        )
        cursor.execute(
            "DELETE FROM integrations WHERE workspace_id = %s", (TEST_WORKSPACE_ID,),
        )
        cursor.execute(
            "DELETE FROM auths WHERE workspace_id = %s", (TEST_WORKSPACE_ID,),
        )
        cursor.execute(
            "DELETE FROM access_points WHERE access_pt_id IN (%s, %s)",
            (seed_data["source_ap_id"], seed_data["dest_ap_id"]),
        )
        cursor.execute(
            "DELETE FROM workspaces WHERE workspace_id = %s", (TEST_WORKSPACE_ID,),
        )
        cursor.execute(
            "DELETE FROM customers WHERE customer_guid = %s", (TEST_CUSTOMER_GUID,),
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"  Cleaned up MySQL test data")

        # Clean up CDC pipeline state (consumer group offsets, processor DAG runs)
        from control_plane.tests.e2e_helpers import cleanup_cdc_pipeline
        cleanup_cdc_pipeline(
            airflow_api_url=AIRFLOW_API_URL,
            get_airflow_headers=_airflow_headers,
        )


@pytest.fixture(scope="session")
def minio_client(wait_for_services):
    """Create MinIO client and test bucket."""
    if not MINIO_AVAILABLE:
        pytest.skip("minio package not installed")

    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                   secret_key=MINIO_SECRET_KEY, secure=False)
    try:
        if not client.bucket_exists(TEST_BUCKET):
            client.make_bucket(TEST_BUCKET)
    except S3Error as e:
        pytest.skip(f"MinIO error: {e}")

    yield client

    try:
        for obj in client.list_objects(TEST_BUCKET, prefix=TEST_PREFIX, recursive=True):
            client.remove_object(TEST_BUCKET, obj.object_name)
    except Exception as e:
        print(f"MinIO cleanup warning: {e}")


@pytest.fixture(scope="session")
def mongo_client(wait_for_services):
    """Create MongoDB client. Pre-cleans the test collection."""
    try:
        client = PyMongoClient(
            f"mongodb://root:root@{MONGO_HOST}:{MONGO_PORT}/",
            serverSelectionTimeoutMS=5000,
        )
        client.server_info()
    except Exception as e:
        pytest.skip(f"MongoDB not available: {e}")

    # Pre-clean stale collection
    try:
        db = client["test_database"]
        if TEST_COLLECTION in db.list_collection_names():
            db[TEST_COLLECTION].drop()
            print(f"\n  Pre-cleaned stale MongoDB collection: {TEST_COLLECTION}")
    except Exception as e:
        print(f"MongoDB pre-clean warning: {e}")

    yield client

    try:
        db = client["test_database"]
        if TEST_COLLECTION in db.list_collection_names():
            db[TEST_COLLECTION].drop()
            print(f"  Cleaned up MongoDB collection: {TEST_COLLECTION}")
    except Exception as e:
        print(f"MongoDB cleanup warning: {e}")
    client.close()


@pytest.fixture(scope="session")
def deploy_controller_dag(wait_for_services):
    """Deploy the test controller DAG and wait for Airflow to detect it."""
    _cleanup_dag_file()
    _cleanup_airflow_dag(CONTROLLER_DAG_ID)
    print("\n  Pre-cleaned stale Airflow controller DAG state")

    abs_path = os.path.abspath(DAG_FILE_PATH)
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    with open(abs_path, "w") as f:
        f.write(DAG_FILE_CONTENT)
    print(f"  Deployed controller DAG to {abs_path}")

    # The dag-processor has AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=30,
    # so it will detect new DAG files within ~30s. We try reserialize first
    # for faster detection, but fall back to the natural refresh cycle.
    repo_root = os.path.join(os.path.dirname(__file__), "..", "..")
    try:
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "airflow-dag-processor",
             "airflow", "dags", "reserialize"],
            cwd=repo_root,
            capture_output=True, timeout=60,
        )
        if result.returncode == 0:
            print("  DAG reserialize completed successfully")
        else:
            print(f"  Reserialize returned {result.returncode}, relying on 30s refresh cycle")
    except (subprocess.TimeoutExpired, Exception) as e:
        print(f"  Reserialize skipped ({e}), relying on 30s refresh cycle")
    time.sleep(5)

    # Poll for DAG detection via API
    detected = False
    max_attempts = 40
    for i in range(max_attempts):
        try:
            headers = _airflow_headers()
            r = requests.get(f"{AIRFLOW_API_URL}/dags/{CONTROLLER_DAG_ID}", headers=headers, timeout=5)
            if r.status_code == 200:
                detected = True
                print(f"  Controller DAG detected after ~{i * 3}s")
                break
        except Exception:
            pass
        if i % 10 == 0:
            print(f"  Waiting for DAG detection... ({i * 3}s)")
        time.sleep(3)

    if not detected:
        _cleanup_dag_file()
        pytest.skip("Airflow did not detect the test controller DAG within 120s")

    try:
        headers = _airflow_headers()
        requests.patch(
            f"{AIRFLOW_API_URL}/dags/{CONTROLLER_DAG_ID}",
            headers=headers, json={"is_paused": False}, timeout=5,
        )
    except Exception:
        pass

    deployed_at = datetime.now(timezone.utc).isoformat()
    print(f"  Controller DAG unpaused at {deployed_at}")

    yield {"dag_id": CONTROLLER_DAG_ID, "detected": detected, "deployed_at": deployed_at}

    print(f"\n  Tearing down controller DAG...")
    _cleanup_airflow_dag(CONTROLLER_DAG_ID)
    _cleanup_dag_file()
    print(f"  Removed controller DAG file and Airflow state")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestControllerScheduledDAG:
    """End-to-end tests for Controller DAG cron-scheduled execution via DTM."""

    controller_run_id = None
    ondemand_run_id = None

    def test_01_upload_test_data(self, minio_client, mongo_client, seed_mysql):
        """Step 1: Upload test data to MinIO and verify DB seed."""
        print("\n" + "=" * 60)
        print("STEP 1: Uploading test data to MinIO")
        print("=" * 60)

        # Verify MongoDB is clean
        db = mongo_client["test_database"]
        assert db[TEST_COLLECTION].count_documents({}) == 0, "MongoDB collection should be empty"
        print(f"  MongoDB collection '{TEST_COLLECTION}' is clean")

        # Verify MySQL seed
        print(f"  MySQL integration_id: {seed_mysql['integration_id']}")
        print(f"  MySQL workspace_id: {TEST_WORKSPACE_ID}")

        test_data = [
            {"id": 1, "name": "Alice", "email": "alice@example.com",
             "timestamp": datetime.now(timezone.utc).isoformat()},
            {"id": 2, "name": "Bob", "email": "bob@example.com",
             "timestamp": datetime.now(timezone.utc).isoformat()},
            {"id": 3, "name": "Charlie", "email": "charlie@example.com",
             "timestamp": datetime.now(timezone.utc).isoformat()},
        ]

        for i, record in enumerate(test_data, 1):
            obj_name = f"{TEST_PREFIX}record_{i}.json"
            data = json.dumps(record).encode("utf-8")
            minio_client.put_object(
                TEST_BUCKET, obj_name, BytesIO(data), len(data),
                content_type="application/json",
            )
            print(f"  Uploaded: {obj_name}")

        objects = list(minio_client.list_objects(TEST_BUCKET, prefix=TEST_PREFIX))
        assert len(objects) == 3
        print(f"\n  Uploaded {len(objects)} files to s3://{TEST_BUCKET}/{TEST_PREFIX}")

    def test_02_verify_dag_detected(self, deploy_controller_dag):
        """Step 2: Verify Airflow detected the controller DAG."""
        print("\n" + "=" * 60)
        print("STEP 2: Verifying controller DAG detection")
        print("=" * 60)

        assert deploy_controller_dag["detected"], "Controller DAG was not detected"

        headers = _airflow_headers()
        r = requests.get(f"{AIRFLOW_API_URL}/dags/{CONTROLLER_DAG_ID}", headers=headers, timeout=5)
        assert r.status_code == 200

        dag_info = r.json()
        print(f"  DAG ID: {dag_info['dag_id']}")
        print(f"  Is Paused: {dag_info.get('is_paused')}")
        print(f"  Schedule: {dag_info.get('timetable_summary')}")
        print(f"\n  Controller DAG is active and ready for scheduling")

    def test_03_wait_for_controller_run(self, deploy_controller_dag):
        """Step 3: Wait for scheduler to trigger the controller DAG."""
        print("\n" + "=" * 60)
        print("STEP 3: Waiting for scheduler-triggered controller run")
        print("=" * 60)
        print("  (schedule='* * * * *' — expecting a run within ~60-90s)")

        deployed_at = deploy_controller_dag["deployed_at"]
        print(f"  Only accepting runs created after: {deployed_at}")

        max_wait = 180
        interval = 5
        elapsed = 0

        while elapsed < max_wait:
            try:
                headers = _airflow_headers()
                r = requests.get(
                    f"{AIRFLOW_API_URL}/dags/{CONTROLLER_DAG_ID}/dagRuns",
                    headers=headers, timeout=10,
                )
                if r.status_code == 200:
                    runs = r.json().get("dag_runs", [])
                    new_scheduled = [
                        run for run in runs
                        if run.get("run_type") == "scheduled"
                        and run.get("queued_at", "") > deployed_at
                    ]
                    if new_scheduled:
                        latest = new_scheduled[0]
                        TestControllerScheduledDAG.controller_run_id = latest["dag_run_id"]
                        print(f"\n  Found NEW controller run after ~{elapsed}s")
                        print(f"  DAG Run ID: {latest['dag_run_id']}")
                        print(f"  State: {latest['state']}")
                        return
            except Exception as e:
                print(f"  Warning: {e}")

            if elapsed % 15 == 0:
                print(f"  Waiting... ({elapsed}s / {max_wait}s)")
            time.sleep(interval)
            elapsed += interval

        pytest.fail(f"No controller run appeared within {max_wait}s")

    def test_04_wait_for_controller_completion(self, deploy_controller_dag):
        """Step 4: Wait for the controller task to complete."""
        print("\n" + "=" * 60)
        print("STEP 4: Waiting for controller completion")
        print("=" * 60)

        dag_run_id = TestControllerScheduledDAG.controller_run_id
        if not dag_run_id:
            pytest.skip("No controller run from test_03")

        print(f"  Tracking controller: {dag_run_id}")

        max_wait = 60
        interval = 5
        elapsed = 0
        dag_state = None

        while elapsed < max_wait:
            try:
                headers = _airflow_headers()
                r = requests.get(
                    f"{AIRFLOW_API_URL}/dags/{CONTROLLER_DAG_ID}/dagRuns/{dag_run_id}",
                    headers=headers, timeout=10,
                )
                if r.status_code == 200:
                    dag_state = r.json().get("state")
                    print(f"    Controller state: {dag_state} ({elapsed}s)")

                    if dag_state == "success":
                        print(f"\n  Controller completed successfully!")
                        return
                    elif dag_state == "failed":
                        self._print_task_logs(CONTROLLER_DAG_ID, dag_run_id, "find_due_integrations")
                        pytest.fail("Controller DAG run failed. See logs above.")
            except Exception as e:
                print(f"    Warning: {e}")

            time.sleep(interval)
            elapsed += interval

        pytest.fail(f"Controller did not complete within {max_wait}s (last state: {dag_state})")

    def test_05_wait_for_ondemand_completion(self, deploy_controller_dag):
        """Step 5: Wait for the dispatched ondemand DAG to complete."""
        print("\n" + "=" * 60)
        print("STEP 5: Waiting for dispatched ondemand DAG completion")
        print("=" * 60)

        if not TestControllerScheduledDAG.controller_run_id:
            pytest.skip("No controller run")

        # Find the ondemand DAG run that was dispatched
        max_wait = 120
        interval = 5
        elapsed = 0
        dag_state = None

        while elapsed < max_wait:
            try:
                headers = _airflow_headers()
                r = requests.get(
                    f"{AIRFLOW_API_URL}/dags/{ONDEMAND_DAG_ID}/dagRuns",
                    headers=headers, timeout=10,
                    params={"limit": 100, "order_by": "-logical_date"},
                )
                if r.status_code == 200:
                    runs = r.json().get("dag_runs", [])
                    # Find runs with our test workspace in the run_id
                    test_runs = [
                        run for run in runs
                        if TEST_WORKSPACE_ID in run.get("dag_run_id", "")
                    ]
                    if test_runs:
                        latest = test_runs[-1]
                        TestControllerScheduledDAG.ondemand_run_id = latest["dag_run_id"]
                        dag_state = latest["state"]
                        print(f"    Ondemand run: {latest['dag_run_id']}")
                        print(f"    State: {dag_state} ({elapsed}s)")

                        if dag_state == "success":
                            print(f"\n  Ondemand DAG completed successfully!")
                            return
                        elif dag_state == "failed":
                            self._print_task_logs(ONDEMAND_DAG_ID, latest["dag_run_id"], "prepare")
                            self._print_task_logs(ONDEMAND_DAG_ID, latest["dag_run_id"], "execute")
                            pytest.fail("Ondemand DAG run failed. See logs above.")
            except Exception as e:
                print(f"    Warning: {e}")

            if elapsed % 15 == 0 and not TestControllerScheduledDAG.ondemand_run_id:
                print(f"  Waiting for ondemand run... ({elapsed}s / {max_wait}s)")
            time.sleep(interval)
            elapsed += interval

        pytest.fail(
            f"Ondemand DAG did not complete within {max_wait}s (last state: {dag_state})"
        )

    def test_06_verify_mongodb_data(self, mongo_client):
        """Step 6: Verify data landed in MongoDB."""
        print("\n" + "=" * 60)
        print("STEP 6: Verifying MongoDB data")
        print("=" * 60)

        if not TestControllerScheduledDAG.ondemand_run_id:
            pytest.skip("No completed ondemand DAG run")

        time.sleep(2)

        db = mongo_client["test_database"]
        collection = db[TEST_COLLECTION]
        documents = list(collection.find(
            {}, {"_id": 0, "_import_timestamp": 0, "_source_bucket": 0, "_source_key": 0}
        ))

        print(f"  Found {len(documents)} documents:")
        for doc in documents:
            print(f"    {doc.get('name')} - {doc.get('email')}")

        assert len(documents) >= 3, f"Expected >= 3 documents, found {len(documents)}"

        names = {doc["name"] for doc in documents}
        assert "Alice" in names
        assert "Bob" in names
        assert "Charlie" in names
        print(f"\n  Data verified in MongoDB!")

    def test_07_verify_integration_run_created(self, seed_mysql):
        """Step 7: Verify IntegrationRun was created in control plane DB."""
        print("\n" + "=" * 60)
        print("STEP 7: Verifying IntegrationRun record")
        print("=" * 60)

        if not TestControllerScheduledDAG.ondemand_run_id:
            pytest.skip("No completed ondemand DAG run")

        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB,
        )
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(
            "SELECT * FROM integration_runs WHERE integration_id = %s ORDER BY run_id DESC LIMIT 1",
            (seed_mysql["integration_id"],),
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        assert row is not None, "IntegrationRun record not found"
        print(f"  IntegrationRun found:")
        print(f"    run_id: {row['run_id']}")
        print(f"    dag_run_id: {row['dag_run_id']}")
        print(f"    integration_id: {row['integration_id']}")
        print(f"    started: {row['started']}")
        assert TEST_WORKSPACE_ID in row["dag_run_id"]
        print(f"\n  IntegrationRun tracking verified!")

    def test_08_summary(self, minio_client, mongo_client, seed_mysql):
        """Step 8: Summary."""
        print("\n" + "=" * 60)
        print("CONTROLLER SCHEDULING E2E TEST SUMMARY")
        print("=" * 60)

        minio_objects = list(minio_client.list_objects(TEST_BUCKET, prefix=TEST_PREFIX))
        db = mongo_client["test_database"]
        mongo_count = db[TEST_COLLECTION].count_documents({})

        print(f"\n  1. MinIO Source: {len(minio_objects)} files in s3://{TEST_BUCKET}/{TEST_PREFIX}")
        print(f"  2. MongoDB Dest: {mongo_count} documents in {TEST_COLLECTION}")
        print(f"  3. Controller DAG: {CONTROLLER_DAG_ID}")
        print(f"  4. Ondemand DAG: {ONDEMAND_DAG_ID}")
        print(f"  5. Controller Run: {TestControllerScheduledDAG.controller_run_id}")
        print(f"  6. Ondemand Run: {TestControllerScheduledDAG.ondemand_run_id}")
        print(f"  7. Integration ID: {seed_mysql['integration_id']}")
        print(f"\n  Pipeline: Cron → Controller → DB Query → DTM → Ondemand DAG → S3 → MongoDB")
        print(f"\n  Full controller scheduling pipeline is working!")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _print_task_logs(self, dag_id, dag_run_id, task_id):
        """Print task logs for debugging failures."""
        try:
            headers = _airflow_headers()
            r = requests.get(
                f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/1",
                headers=headers, timeout=10,
            )
            if r.status_code == 200:
                data = r.json()
                print(f"\n  === Logs for {dag_id}/{task_id} ===")
                for entry in data.get("content", []):
                    if "event" in entry:
                        print(f"    [{entry.get('level', '')}] {entry['event'][:300]}")
        except Exception:
            pass
