#!/usr/bin/env python3
"""
Script to register Debezium MySQL connector with Kafka Connect.

This script registers the integration CDC connector that monitors the
integrations table and publishes change events to Kafka.

Usage:
    python debezium/register_connector.py
    python debezium/register_connector.py --delete  # Delete connector first
"""

import sys
import json
import time
import argparse
import os
import requests
from pathlib import Path


# Allow overriding via environment when running inside Docker
KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
CONNECTOR_NAME = "integration-cdc-connector"
CONFIG_FILE = Path(__file__).parent / "integration-connector.json"


def wait_for_kafka_connect(max_retries=30, retry_delay=2):
    """Wait for Kafka Connect to be ready."""
    print(f"Waiting for Kafka Connect at {KAFKA_CONNECT_URL}...")

    for attempt in range(max_retries):
        try:
            response = requests.get(f"{KAFKA_CONNECT_URL}/connectors", timeout=5)
            if response.status_code == 200:
                print(f"✓ Kafka Connect is ready")
                return True
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"  Waiting... ({attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                print(f"✗ Kafka Connect not available after {max_retries} retries")
                print(f"  Error: {e}")
                return False

    return False


def get_connector_status(connector_name):
    """Get the status of a connector."""
    try:
        response = requests.get(
            f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status",
            timeout=5
        )
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return None
        else:
            print(f"Warning: Unexpected status code {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error checking connector status: {e}")
        return None


def delete_connector(connector_name):
    """Delete an existing connector."""
    print(f"\nDeleting connector: {connector_name}")

    try:
        response = requests.delete(
            f"{KAFKA_CONNECT_URL}/connectors/{connector_name}",
            timeout=10
        )

        if response.status_code == 204:
            print(f"✓ Connector deleted successfully")
            time.sleep(2)  # Give Kafka Connect time to clean up
            return True
        elif response.status_code == 404:
            print(f"  Connector does not exist (already deleted)")
            return True
        else:
            print(f"✗ Failed to delete connector: {response.status_code}")
            print(f"  Response: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Error deleting connector: {e}")
        return False


def register_connector(config_file, force=False):
    """Register a Debezium connector with Kafka Connect."""

    # Load connector configuration
    if not config_file.exists():
        print(f"✗ Configuration file not found: {config_file}")
        return False

    with open(config_file, 'r') as f:
        config = json.load(f)

    connector_name = config.get("name")
    if not connector_name:
        print("✗ Connector name not found in configuration")
        return False

    print(f"\n{'='*60}")
    print(f"Registering Debezium Connector: {connector_name}")
    print(f"{'='*60}")

    # Check if connector already exists
    status = get_connector_status(connector_name)

    if status:
        print(f"\n✓ Connector already exists")
        print(f"  State: {status.get('connector', {}).get('state', 'unknown')}")

        if force:
            print(f"\n  Force flag set - deleting and recreating connector")
            if not delete_connector(connector_name):
                return False
        else:
            print(f"\n  Use --force to delete and recreate")
            return True

    # Register the connector
    print(f"\nRegistering connector...")
    print(f"  Database: {config['config']['database.include.list']}")
    print(f"  Table: {config['config']['table.include.list']}")
    print(f"  Topic: cdc.integration.events")

    try:
        response = requests.post(
            f"{KAFKA_CONNECT_URL}/connectors",
            headers={"Content-Type": "application/json"},
            data=json.dumps(config),
            timeout=10
        )

        if response.status_code == 201:
            print(f"\n✓ Connector registered successfully!")

            # Wait a moment and check status
            time.sleep(3)
            status = get_connector_status(connector_name)

            if status:
                connector_state = status.get('connector', {}).get('state', 'unknown')
                print(f"\n✓ Connector Status:")
                print(f"  State: {connector_state}")

                tasks = status.get('tasks', [])
                if tasks:
                    for i, task in enumerate(tasks):
                        task_state = task.get('state', 'unknown')
                        print(f"  Task {i}: {task_state}")
                        if task_state != 'RUNNING':
                            print(f"    Warning: Task not running")
                            if 'trace' in task:
                                print(f"    Error: {task['trace']}")

                if connector_state == 'RUNNING':
                    print(f"\n🎉 CDC connector is now active and monitoring changes!")
                    return True
                else:
                    print(f"\n⚠️  Connector registered but not running")
                    return False
            else:
                print(f"\n⚠️  Connector registered but couldn't verify status")
                return True

        elif response.status_code == 409:
            print(f"\n✓ Connector already exists")
            return True
        else:
            print(f"\n✗ Failed to register connector: {response.status_code}")
            print(f"  Response: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"\n✗ Error registering connector: {e}")
        return False


def list_connectors():
    """List all registered connectors."""
    try:
        response = requests.get(f"{KAFKA_CONNECT_URL}/connectors", timeout=5)
        if response.status_code == 200:
            connectors = response.json()
            print(f"\nRegistered Connectors: {len(connectors)}")
            for connector in connectors:
                print(f"  - {connector}")
            return True
        else:
            print(f"Failed to list connectors: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error listing connectors: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Register Debezium CDC connector")
    parser.add_argument('--delete', action='store_true', help='Delete connector before registering')
    parser.add_argument('--force', action='store_true', help='Force recreate if connector exists')
    parser.add_argument('--list', action='store_true', help='List all connectors')
    parser.add_argument('--status', action='store_true', help='Show connector status')
    args = parser.parse_args()

    # Wait for Kafka Connect to be ready
    if not wait_for_kafka_connect():
        print("\n✗ Kafka Connect is not available")
        print("  Make sure Docker services are running:")
        print("  docker-compose up -d")
        sys.exit(1)

    # List connectors if requested
    if args.list:
        list_connectors()
        return

    # Show status if requested
    if args.status:
        status = get_connector_status(CONNECTOR_NAME)
        if status:
            print(f"\nConnector Status:")
            print(json.dumps(status, indent=2))
        else:
            print(f"\n✗ Connector not found: {CONNECTOR_NAME}")
        return

    # Delete connector if requested
    if args.delete:
        if not delete_connector(CONNECTOR_NAME):
            sys.exit(1)
        return

    # Register the connector
    if register_connector(CONFIG_FILE, force=args.force):
        print(f"\n{'='*60}")
        print(f"✅ SUCCESS")
        print(f"{'='*60}")
        print(f"\nThe Debezium CDC connector is now active!")
        print(f"\nNext steps:")
        print(f"  1. Create an integration via API")
        print(f"  2. CDC event will automatically appear in Kafka")
        print(f"  3. Kafka consumer service will trigger Airflow DAG")
        print(f"\nMonitoring:")
        print(f"  - Kafka UI: http://localhost:8081")
        print(f"  - Debezium UI: http://localhost:8088")
        print(f"  - Kafka Connect API: {KAFKA_CONNECT_URL}")
    else:
        print(f"\n{'='*60}")
        print(f"✗ FAILED")
        print(f"{'='*60}")
        sys.exit(1)


if __name__ == "__main__":
    main()
