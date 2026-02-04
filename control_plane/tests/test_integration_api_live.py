"""Integration tests for API endpoints with live database.

These tests require Docker services to be running.
Run with: docker-compose up -d && pytest control_plane/tests/test_integration_api_live.py
"""

import pytest
import requests
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from control_plane.app.core.database import Base
from control_plane.app.models.customer import Customer
from control_plane.app.models.workspace import Workspace
from control_plane.app.models.auth import Auth
from control_plane.app.models.workflow import Workflow
from control_plane.app.models.access_point import AccessPoint


# Test configuration
API_BASE_URL = "http://localhost:8000"
DATABASE_URL = "mysql+pymysql://control_plane:control_plane@localhost:3306/control_plane"


@pytest.fixture(scope="session")
def wait_for_services():
    """Wait for services to be ready."""
    max_retries = 30
    retry_interval = 2

    # Wait for API
    for i in range(max_retries):
        try:
            response = requests.get(f"{API_BASE_URL}/api/v1/health", timeout=2)
            if response.status_code == 200:
                print(f"\n✓ API is ready")
                break
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            if i < max_retries - 1:
                print(f"Waiting for API... ({i+1}/{max_retries})")
                time.sleep(retry_interval)
            else:
                pytest.skip("API not available - are Docker services running?")

    # Wait for database
    for i in range(max_retries):
        try:
            engine = create_engine(DATABASE_URL)
            engine.connect()
            print(f"✓ Database is ready")
            engine.dispose()
            break
        except Exception:
            if i < max_retries - 1:
                print(f"Waiting for database... ({i+1}/{max_retries})")
                time.sleep(retry_interval)
            else:
                pytest.skip("Database not available - are Docker services running?")


@pytest.fixture(scope="session")
def setup_database(wait_for_services):
    """Set up database with test data."""
    engine = create_engine(DATABASE_URL)

    # Create tables
    Base.metadata.create_all(bind=engine)

    # Create session
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()

    try:
        # Clean up existing test data (in correct order due to foreign keys)
        from control_plane.app.models.integration import Integration
        from control_plane.app.models.integration_run import IntegrationRun

        # Delete in correct order: children first, parents last
        # First, get all test integrations
        test_integrations = db.query(Integration).filter(Integration.workspace_id.like("test-%")).all()
        test_integration_ids = [integration.integration_id for integration in test_integrations]

        # Delete integration_runs first (child table)
        if test_integration_ids:
            db.query(IntegrationRun).filter(IntegrationRun.integration_id.in_(test_integration_ids)).delete(synchronize_session=False)

        # Then delete integrations
        db.query(Integration).filter(Integration.workspace_id.like("test-%")).delete(synchronize_session=False)
        db.query(Auth).filter(Auth.workspace_id.like("test-%")).delete(synchronize_session=False)
        db.query(Workspace).filter(Workspace.workspace_id.like("test-%")).delete(synchronize_session=False)
        db.query(Customer).filter(Customer.customer_guid.like("test-%")).delete(synchronize_session=False)
        db.commit()

        # Create test data
        customer = Customer(customer_guid="test-cust-001", name="Test Customer", max_integration=100)
        db.add(customer)

        workspace = Workspace(workspace_id="test-ws-001", customer_guid="test-cust-001")
        db.add(workspace)

        auth = Auth(
            workspace_id="test-ws-001",
            auth_type="aws_iam",
            json_data='{"access_key": "test"}',
        )
        db.add(auth)

        # Create or get workflow (may already exist)
        workflow = db.query(Workflow).filter(Workflow.workflow_id == 1).first()
        if not workflow:
            workflow = Workflow(workflow_id=1, workflow_type="S3ToMongo")
            db.add(workflow)

        # Create or get access points (may already exist)
        s3_ap = db.query(AccessPoint).filter(AccessPoint.access_pt_id == 1).first()
        if not s3_ap:
            s3_ap = AccessPoint(access_pt_id=1, ap_type="S3")
            db.add(s3_ap)

        mongo_ap = db.query(AccessPoint).filter(AccessPoint.access_pt_id == 2).first()
        if not mongo_ap:
            mongo_ap = AccessPoint(access_pt_id=2, ap_type="MongoDB")
            db.add(mongo_ap)

        db.commit()

        auth_id = auth.auth_id

        yield {"workspace_id": "test-ws-001", "auth_id": auth_id}

    finally:
        # Cleanup (in correct order due to foreign keys)
        from control_plane.app.models.integration import Integration
        from control_plane.app.models.integration_run import IntegrationRun

        # Delete in correct order: children first, parents last
        try:
            # First, get all test integrations
            test_integrations = db.query(Integration).filter(Integration.workspace_id.like("test-%")).all()
            test_integration_ids = [integration.integration_id for integration in test_integrations]

            # Delete integration_runs first (child table)
            if test_integration_ids:
                db.query(IntegrationRun).filter(IntegrationRun.integration_id.in_(test_integration_ids)).delete(synchronize_session=False)

            # Then delete integrations
            db.query(Integration).filter(Integration.workspace_id.like("test-%")).delete(synchronize_session=False)
            db.query(Auth).filter(Auth.workspace_id.like("test-%")).delete(synchronize_session=False)
            db.query(Workspace).filter(Workspace.workspace_id.like("test-%")).delete(synchronize_session=False)
            db.query(Customer).filter(Customer.customer_guid.like("test-%")).delete(synchronize_session=False)
            db.commit()
        except Exception as e:
            print(f"Cleanup error (non-fatal): {e}")
            db.rollback()
        finally:
            db.close()
            engine.dispose()


class TestHealthEndpointsLive:
    """Test health check endpoints with live services."""

    def test_health_check(self, wait_for_services):
        """Test basic health check endpoint."""
        response = requests.get(f"{API_BASE_URL}/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "version" in data


class TestIntegrationEndpointsLive:
    """Test Integration CRUD endpoints with live services."""

    def test_create_integration(self, setup_database):
        """Test creating a new integration."""
        integration_data = {
            "workspace_id": setup_database["workspace_id"],
            "workflow_id": 1,
            "auth_id": setup_database["auth_id"],
            "source_access_pt_id": 1,
            "dest_access_pt_id": 2,
            "integration_type": "S3ToMongo",
            "usr_sch_cron": "0 2 * * *",
            "usr_timezone": "America/New_York",
            "schedule_type": "daily",
            "json_data": '{"s3_bucket": "test-bucket", "s3_prefix": "data/", "mongo_collection": "test_collection"}',
        }

        response = requests.post(f"{API_BASE_URL}/api/v1/integrations/", json=integration_data)
        assert response.status_code == 201
        data = response.json()
        assert data["integration_type"] == "S3ToMongo"
        assert data["usr_sch_status"] == "active"
        assert "integration_id" in data

        # Store for cleanup
        return data["integration_id"]

    def test_list_integrations(self, setup_database):
        """Test listing integrations."""
        # Create an integration first
        integration_data = {
            "workspace_id": setup_database["workspace_id"],
            "workflow_id": 1,
            "auth_id": setup_database["auth_id"],
            "source_access_pt_id": 1,
            "dest_access_pt_id": 2,
            "integration_type": "S3ToMongo",
            "schedule_type": "daily",
        }
        requests.post(f"{API_BASE_URL}/api/v1/integrations/", json=integration_data)

        # List integrations
        response = requests.get(f"{API_BASE_URL}/api/v1/integrations/")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_get_integration(self, setup_database):
        """Test getting a specific integration."""
        # Create an integration
        integration_data = {
            "workspace_id": setup_database["workspace_id"],
            "workflow_id": 1,
            "auth_id": setup_database["auth_id"],
            "source_access_pt_id": 1,
            "dest_access_pt_id": 2,
            "integration_type": "S3ToMongo",
            "schedule_type": "daily",
        }
        create_response = requests.post(f"{API_BASE_URL}/api/v1/integrations/", json=integration_data)
        integration_id = create_response.json()["integration_id"]

        # Get the integration
        response = requests.get(f"{API_BASE_URL}/api/v1/integrations/{integration_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["integration_id"] == integration_id
        assert data["integration_type"] == "S3ToMongo"

    def test_update_integration(self, setup_database):
        """Test updating an integration."""
        # Create an integration
        integration_data = {
            "workspace_id": setup_database["workspace_id"],
            "workflow_id": 1,
            "auth_id": setup_database["auth_id"],
            "source_access_pt_id": 1,
            "dest_access_pt_id": 2,
            "integration_type": "S3ToMongo",
            "schedule_type": "daily",
        }
        create_response = requests.post(f"{API_BASE_URL}/api/v1/integrations/", json=integration_data)
        integration_id = create_response.json()["integration_id"]

        # Update the integration
        update_data = {
            "usr_sch_status": "paused",
            "usr_sch_cron": "0 3 * * *",
        }
        response = requests.put(f"{API_BASE_URL}/api/v1/integrations/{integration_id}", json=update_data)
        assert response.status_code == 200
        data = response.json()
        assert data["usr_sch_status"] == "paused"
        assert data["usr_sch_cron"] == "0 3 * * *"

    def test_delete_integration(self, setup_database):
        """Test deleting an integration."""
        # Create an integration
        integration_data = {
            "workspace_id": setup_database["workspace_id"],
            "workflow_id": 1,
            "auth_id": setup_database["auth_id"],
            "source_access_pt_id": 1,
            "dest_access_pt_id": 2,
            "integration_type": "S3ToMongo",
            "schedule_type": "daily",
        }
        create_response = requests.post(f"{API_BASE_URL}/api/v1/integrations/", json=integration_data)
        integration_id = create_response.json()["integration_id"]

        # Delete the integration
        response = requests.delete(f"{API_BASE_URL}/api/v1/integrations/{integration_id}")
        assert response.status_code == 204

        # Verify it's deleted
        get_response = requests.get(f"{API_BASE_URL}/api/v1/integrations/{integration_id}")
        assert get_response.status_code == 404


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
