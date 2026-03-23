#!/bin/bash
# Script to create Kafka topics including Dead Letter Queue

set -e

# Kafka connection details
KAFKA_BROKER="${KAFKA_BROKER:-kafka:29092}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"
PARTITIONS="${PARTITIONS:-3}"

echo "Waiting for Kafka to be ready..."
# Wait for Kafka to be available
while ! kafka-broker-api-versions --bootstrap-server $KAFKA_BROKER > /dev/null 2>&1; do
    echo "Waiting for Kafka broker..."
    sleep 2
done

echo "Kafka is ready. Creating topics..."

# Create CDC events topic
kafka-topics --create \
    --bootstrap-server $KAFKA_BROKER \
    --topic cdc.integration.events \
    --partitions $PARTITIONS \
    --replication-factor $REPLICATION_FACTOR \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete \
    --if-not-exists

echo "✓ Created topic: cdc.integration.events"

# Create Dead Letter Queue topic
kafka-topics --create \
    --bootstrap-server $KAFKA_BROKER \
    --topic cdc.integration.events.dlq \
    --partitions $PARTITIONS \
    --replication-factor $REPLICATION_FACTOR \
    --config retention.ms=2592000000 \
    --config cleanup.policy=delete \
    --if-not-exists

echo "✓ Created topic: cdc.integration.events.dlq"

# Create test CDC topic (for testing)
kafka-topics --create \
    --bootstrap-server $KAFKA_BROKER \
    --topic test.cdc.integration.events \
    --partitions $PARTITIONS \
    --replication-factor $REPLICATION_FACTOR \
    --config retention.ms=86400000 \
    --config cleanup.policy=delete \
    --if-not-exists

echo "✓ Created topic: test.cdc.integration.events"

# Create audit events topic (6 partitions for customer_guid-based partitioning)
kafka-topics --create \
    --bootstrap-server $KAFKA_BROKER \
    --topic audit.events \
    --partitions 6 \
    --replication-factor $REPLICATION_FACTOR \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete \
    --if-not-exists

echo "✓ Created topic: audit.events"

# List all topics
echo ""
echo "All Kafka topics:"
kafka-topics --list --bootstrap-server $KAFKA_BROKER

echo ""
echo "✅ Kafka topics created successfully!"
echo ""
echo "Topic details:"
echo "  - cdc.integration.events (retention: 7 days)"
echo "  - cdc.integration.events.dlq (retention: 30 days)"
echo "  - test.cdc.integration.events (retention: 1 day)"
echo "  - audit.events (retention: 7 days, 6 partitions)"
