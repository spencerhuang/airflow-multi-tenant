"""Integration tests for Debezium CDC with real database changes.

These tests verify that Debezium correctly captures database changes
and publishes CDC events to Kafka when integrations are created, updated, or deleted.

Prerequisites:
- Docker services running (MySQL, Kafka, Kafka Connect)
- Debezium connector registered (run: python debezium/register_connector.py)
- Run with: pytest control_plane/tests/test_debezium_cdc.py -v -s
"""

import pytest
import json
import time
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from control_plane.app.core.database import Base
from control_plane.app.models.customer import Customer
from control_plane.app.models.workspace import Workspace
from control_plane.app.models.auth import Auth
from control_plane.app.models.workflow import Workflow
from control_plane.app.models.access_point import AccessPoint
from control_plane.app.models.integration import Integration


# Test configuration
DATABASE_URL = "mysql+pymysql://control_plane:control_plane@localhost:3306/control_plane"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
CDC_TOPIC = "cdc.integration.events"


@pytest.fixture(scope="session")
def wait_for_kafka():
    """Wait for Kafka to be ready."""
    max_retries = 30
    retry_delay = 2

    for i in range(max_retries):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                request_timeout_ms=5000,
            )
            consumer.close()
            print(f"\n✓ Kafka is ready")
            return True
        except NoBrokersAvailable:
            if i < max_retries - 1:
                print(f"Waiting for Kafka... ({i+1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                pytest.skip("Kafka not available - are Docker services running?")
        except Exception as e:
            if i < max_retries - 1:
                print(f"Waiting for Kafka... ({i+1}/{max_retries}): {e}")
                time.sleep(retry_delay)
            else:
                pytest.skip(f"Kafka error: {e}")


@pytest.fixture(scope="session")
def wait_for_database():
    """Wait for database to be ready."""
    max_retries = 30
    retry_delay = 2

    for i in range(max_retries):
        try:
            engine = create_engine(DATABASE_URL)
            engine.connect()
            engine.dispose()
            print(f"✓ Database is ready")
            return True
        except Exception:
            if i < max_retries - 1:
                print(f"Waiting for database... ({i+1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                pytest.skip("Database not available - are Docker services running?")


@pytest.fixture(scope="session")
def db_engine(wait_for_database):
    """Create database engine."""
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def db_session(db_engine):
    """Create database session."""
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)
    db = SessionLocal()

    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@pytest.fixture(scope="function")
def kafka_consumer(wait_for_kafka):
    """Create Kafka consumer for CDC events."""
    consumer = KafkaConsumer(
        CDC_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset="latest",  # Only read new messages
        enable_auto_commit=False,
        group_id=f"test-debezium-{int(time.time() * 1000)}",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=15000,  # 15 second timeout
    )

    # Clear any existing messages
    for _ in consumer:
        pass

    yield consumer
    consumer.close()


@pytest.fixture(scope="function")
def test_workspace(db_session):
    """Create test workspace with dependencies."""
    # Create customer
    customer = Customer(
        customer_guid=f"test-cdc-cust-{int(time.time())}",
        name="Test CDC Customer",
        max_integration=100
    )
    db_session.add(customer)

    # Create workspace
    workspace = Workspace(
        workspace_id=f"test-cdc-ws-{int(time.time())}",
        customer_guid=customer.customer_guid
    )
    db_session.add(workspace)

    # Create auth
    auth = Auth(
        workspace_id=workspace.workspace_id,
        auth_type="aws_iam",
        json_data='{"access_key": "test"}'
    )
    db_session.add(auth)

    # Create or get workflow
    workflow = db_session.query(Workflow).filter(Workflow.workflow_id == 1).first()
    if not workflow:
        workflow = Workflow(workflow_id=1, workflow_type="S3ToMongo")
        db_session.add(workflow)

    # Create or get access points
    s3_ap = db_session.query(AccessPoint).filter(AccessPoint.access_pt_id == 1).first()
    if not s3_ap:
        s3_ap = AccessPoint(access_pt_id=1, ap_type="S3")
        db_session.add(s3_ap)

    mongo_ap = db_session.query(AccessPoint).filter(AccessPoint.access_pt_id == 2).first()
    if not mongo_ap:
        mongo_ap = AccessPoint(access_pt_id=2, ap_type="MongoDB")
        db_session.add(mongo_ap)

    db_session.commit()
    db_session.refresh(auth)

    yield {
        "workspace_id": workspace.workspace_id,
        "customer_guid": customer.customer_guid,
        "auth_id": auth.auth_id,
    }

    # Cleanup
    try:
        # Get all test integrations
        test_integrations = db_session.query(Integration).filter(
            Integration.workspace_id == workspace.workspace_id
        ).all()

        # Delete integrations
        for integration in test_integrations:
            db_session.delete(integration)

        db_session.delete(auth)
        db_session.delete(workspace)
        db_session.delete(customer)
        db_session.commit()
    except Exception as e:
        print(f"Cleanup error: {e}")
        db_session.rollback()


class TestDebeziumCDC:
    """Test Debezium CDC captures database changes."""

    def test_cdc_integration_created(self, db_session, kafka_consumer, test_workspace):
        """Test that creating an integration produces a CDC event."""
        print("\n" + "="*60)
        print("TEST: CDC Integration Created Event")
        print("="*60)

        # Create an integration
        integration = Integration(
            workspace_id=test_workspace["workspace_id"],
            workflow_id=1,
            auth_id=test_workspace["auth_id"],
            source_access_pt_id=1,
            dest_access_pt_id=2,
            integration_type="S3ToMongo",
            schedule_type="on_demand",
            usr_sch_status="active",
            json_data='{"test": "data"}',
        )

        print(f"\n1. Creating integration in database...")
        db_session.add(integration)
        db_session.commit()
        db_session.refresh(integration)

        integration_id = integration.integration_id
        print(f"   ✓ Integration created: ID={integration_id}")

        # Wait for CDC event to be published
        print(f"\n2. Waiting for CDC event in Kafka topic: {CDC_TOPIC}")
        print(f"   (Debezium should detect the INSERT and publish event)")

        cdc_event = None
        message_count = 0

        for message in kafka_consumer:
            message_count += 1
            event = message.value
            print(f"\n   Received message {message_count}:")
            print(f"   - Offset: {message.offset}")
            print(f"   - Partition: {message.partition}")

            # Check if this is our integration
            if event.get("integration_id") == integration_id:
                cdc_event = event
                print(f"   ✓ Found CDC event for our integration!")
                break
            else:
                print(f"   - Different integration_id: {event.get('integration_id')} (skipping)")

        # Verify CDC event was received
        assert cdc_event is not None, f"CDC event not received after checking {message_count} messages. Debezium connector may not be running."

        print(f"\n3. Validating CDC event structure:")
        print(f"   Event keys: {list(cdc_event.keys())}")

        # Verify event contains expected fields from Debezium transforms
        assert "integration_id" in cdc_event, "Missing integration_id"
        assert "workspace_id" in cdc_event, "Missing workspace_id"
        assert "integration_type" in cdc_event, "Missing integration_type"
        assert "__op" in cdc_event, "Missing __op (operation type from Debezium)"
        assert "__source_ts_ms" in cdc_event, "Missing __source_ts_ms (timestamp)"

        # Verify operation type is CREATE
        assert cdc_event["__op"] == "c", f"Expected operation 'c' (create), got '{cdc_event['__op']}'"

        # Verify field values
        assert cdc_event["integration_id"] == integration_id
        assert cdc_event["workspace_id"] == test_workspace["workspace_id"]
        assert cdc_event["integration_type"] == "S3ToMongo"
        assert cdc_event["usr_sch_status"] == "active"

        print(f"   ✓ integration_id: {cdc_event['integration_id']}")
        print(f"   ✓ workspace_id: {cdc_event['workspace_id']}")
        print(f"   ✓ integration_type: {cdc_event['integration_type']}")
        print(f"   ✓ operation: {cdc_event['__op']} (create)")
        print(f"   ✓ timestamp: {cdc_event['__source_ts_ms']}")

        print(f"\n✅ CDC event successfully captured for integration creation!")

    def test_cdc_integration_updated(self, db_session, kafka_consumer, test_workspace):
        """Test that updating an integration produces a CDC event."""
        print("\n" + "="*60)
        print("TEST: CDC Integration Updated Event")
        print("="*60)

        # Create an integration first
        integration = Integration(
            workspace_id=test_workspace["workspace_id"],
            workflow_id=1,
            auth_id=test_workspace["auth_id"],
            source_access_pt_id=1,
            dest_access_pt_id=2,
            integration_type="S3ToMongo",
            schedule_type="on_demand",
            usr_sch_status="active",
            json_data='{"test": "data"}',
        )

        print(f"\n1. Creating integration...")
        db_session.add(integration)
        db_session.commit()
        db_session.refresh(integration)
        integration_id = integration.integration_id
        print(f"   ✓ Integration created: ID={integration_id}")

        # Clear the consumer to ignore the create event
        for _ in kafka_consumer:
            pass

        # Update the integration
        print(f"\n2. Updating integration status to 'paused'...")
        integration.usr_sch_status = "paused"
        integration.json_data = '{"test": "updated"}'
        db_session.commit()
        print(f"   ✓ Integration updated")

        # Wait for CDC event
        print(f"\n3. Waiting for CDC UPDATE event...")

        cdc_event = None
        message_count = 0

        for message in kafka_consumer:
            message_count += 1
            event = message.value

            if event.get("integration_id") == integration_id:
                cdc_event = event
                print(f"   ✓ Found CDC event for our integration!")
                break

        # Verify CDC event was received
        assert cdc_event is not None, f"CDC event not received after checking {message_count} messages"

        # Verify operation type is UPDATE
        assert cdc_event["__op"] == "u", f"Expected operation 'u' (update), got '{cdc_event['__op']}'"

        # Verify updated values
        assert cdc_event["usr_sch_status"] == "paused"
        assert cdc_event["integration_id"] == integration_id

        print(f"\n4. Validating CDC event:")
        print(f"   ✓ operation: {cdc_event['__op']} (update)")
        print(f"   ✓ usr_sch_status: {cdc_event['usr_sch_status']} (updated to 'paused')")

        print(f"\n✅ CDC event successfully captured for integration update!")

    def test_cdc_integration_deleted(self, db_session, kafka_consumer, test_workspace):
        """Test that deleting an integration produces a CDC event."""
        print("\n" + "="*60)
        print("TEST: CDC Integration Deleted Event")
        print("="*60)

        # Create an integration first
        integration = Integration(
            workspace_id=test_workspace["workspace_id"],
            workflow_id=1,
            auth_id=test_workspace["auth_id"],
            source_access_pt_id=1,
            dest_access_pt_id=2,
            integration_type="S3ToMongo",
            schedule_type="on_demand",
            usr_sch_status="active",
            json_data='{"test": "data"}',
        )

        print(f"\n1. Creating integration...")
        db_session.add(integration)
        db_session.commit()
        db_session.refresh(integration)
        integration_id = integration.integration_id
        print(f"   ✓ Integration created: ID={integration_id}")

        # Clear the consumer to ignore the create event
        for _ in kafka_consumer:
            pass

        # Delete the integration
        print(f"\n2. Deleting integration...")
        # First delete any related integration_runs (foreign key constraint)
        from control_plane.app.models.integration_run import IntegrationRun
        db_session.query(IntegrationRun).filter(
            IntegrationRun.integration_id == integration_id
        ).delete()
        db_session.delete(integration)
        db_session.commit()
        print(f"   ✓ Integration deleted")

        # Wait for CDC event
        print(f"\n3. Waiting for CDC DELETE event...")

        cdc_event = None
        message_count = 0

        for message in kafka_consumer:
            message_count += 1
            event = message.value

            if event.get("integration_id") == integration_id:
                cdc_event = event
                print(f"   ✓ Found CDC event for our integration!")
                break

        # Verify CDC event was received
        assert cdc_event is not None, f"CDC event not received after checking {message_count} messages"

        # Verify operation type is DELETE
        assert cdc_event["__op"] == "d", f"Expected operation 'd' (delete), got '{cdc_event['__op']}'"

        # Verify integration_id is present
        assert cdc_event["integration_id"] == integration_id

        print(f"\n4. Validating CDC event:")
        print(f"   ✓ operation: {cdc_event['__op']} (delete)")
        print(f"   ✓ integration_id: {cdc_event['integration_id']}")

        print(f"\n✅ CDC event successfully captured for integration deletion!")

    def test_cdc_multiple_operations_in_sequence(self, db_session, kafka_consumer, test_workspace):
        """Test multiple CDC events in sequence."""
        print("\n" + "="*60)
        print("TEST: CDC Multiple Operations in Sequence")
        print("="*60)

        # Create integration
        integration = Integration(
            workspace_id=test_workspace["workspace_id"],
            workflow_id=1,
            auth_id=test_workspace["auth_id"],
            source_access_pt_id=1,
            dest_access_pt_id=2,
            integration_type="S3ToMongo",
            schedule_type="on_demand",
            usr_sch_status="active",
            json_data='{"test": "data"}',
        )

        print(f"\n1. Performing CREATE, UPDATE, DELETE operations...")
        db_session.add(integration)
        db_session.commit()
        db_session.refresh(integration)
        integration_id = integration.integration_id
        print(f"   ✓ Created integration: ID={integration_id}")

        time.sleep(1)

        integration.usr_sch_status = "paused"
        db_session.commit()
        print(f"   ✓ Updated integration status")

        time.sleep(1)

        # Delete any related integration_runs first (foreign key constraint)
        from control_plane.app.models.integration_run import IntegrationRun
        db_session.query(IntegrationRun).filter(
            IntegrationRun.integration_id == integration_id
        ).delete()
        db_session.delete(integration)
        db_session.commit()
        print(f"   ✓ Deleted integration")

        # Collect all CDC events
        print(f"\n2. Collecting CDC events...")
        events = []
        message_count = 0

        for message in kafka_consumer:
            message_count += 1
            event = message.value

            if event.get("integration_id") == integration_id:
                events.append(event)
                print(f"   ✓ Event {len(events)}: operation={event['__op']}")

                if len(events) >= 3:
                    break

        # Verify we got all 3 events
        assert len(events) >= 3, f"Expected 3 CDC events, got {len(events)}"

        # Verify event sequence
        operations = [e["__op"] for e in events]
        assert "c" in operations, "Missing CREATE event"
        assert "u" in operations, "Missing UPDATE event"
        assert "d" in operations, "Missing DELETE event"

        print(f"\n3. Validation:")
        print(f"   ✓ Received all 3 CDC events (create, update, delete)")
        print(f"   ✓ Event sequence: {' -> '.join(operations)}")

        print(f"\n✅ Multiple CDC operations captured successfully!")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
