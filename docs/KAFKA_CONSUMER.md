# Kafka Consumer Service Documentation

## Overview

The Kafka Consumer Service is a background service in the Control Plane that subscribes to CDC (Change Data Capture) events from Kafka and processes integration lifecycle events.

## Architecture

```
┌──────────────────┐
│   Debezium CDC   │  Captures MySQL changes
│   Connector      │  (integrations table)
└────────┬─────────┘
         │
         │ Publishes events
         ↓
┌──────────────────┐
│   Kafka Topic    │  "cdc.integration.events"
│                  │
└────────┬─────────┘
         │
         │ Subscribes
         ↓
┌──────────────────┐
│  Control Plane   │  KafkaConsumerService
│  Kafka Consumer  │  (Background thread)
└────────┬─────────┘
         │
         │ Processes events
         ↓
┌──────────────────┐
│  Event Handler   │  Triggers workflows,
│                  │  updates, alerts, etc.
└──────────────────┘
```

## Implementation

### Location

- **Service**: [control_plane/app/services/kafka_consumer_service.py](control_plane/app/services/kafka_consumer_service.py)
- **Startup**: [control_plane/app/main.py](control_plane/app/main.py)

### Key Components

#### 1. KafkaConsumerService Class

Main service class that manages Kafka consumer lifecycle:

```python
class KafkaConsumerService:
    """
    Kafka consumer service for processing CDC events.

    Subscribes to CDC topic and processes integration events.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str = "control-plane-consumer",
        event_handler: Optional[Callable] = None,
    ):
        ...

    def start(self) -> None:
        """Start the Kafka consumer in a background thread."""

    def stop(self) -> None:
        """Stop the Kafka consumer."""
```

#### 2. Event Types

The consumer processes the following CDC event types:

| Event Type | Description | Trigger |
|------------|-------------|---------|
| `integration.created` | New integration created | INSERT into integrations table |
| `integration.updated` | Integration configuration updated | UPDATE on integrations table |
| `integration.deleted` | Integration removed | DELETE from integrations table |
| `integration.run.started` | Workflow execution started | DAG run started |
| `integration.run.completed` | Workflow execution completed | DAG run succeeded |
| `integration.run.failed` | Workflow execution failed | DAG run failed |

#### 3. Event Message Format

CDC events follow this structure:

```json
{
  "event_type": "integration.created",
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-02-04T12:00:00Z",
  "data": {
    "integration_id": 123,
    "tenant_id": "customer_abc",
    "workflow_type": "s3_to_mongo",
    "source_config": {...},
    "destination_config": {...}
  }
}
```

#### 4. Application Lifecycle

The Kafka consumer is managed as part of the FastAPI application lifecycle:

**Startup** ([main.py:38-50](control_plane/app/main.py#L38-L50)):
```python
@app.on_event("startup")
async def startup_event():
    """Initialize Kafka consumer on application startup."""
    initialize_kafka_consumer()
```

**Shutdown** ([main.py:53-65](control_plane/app/main.py#L53-L65)):
```python
@app.on_event("shutdown")
async def shutdown_event():
    """Gracefully stop Kafka consumer on shutdown."""
    shutdown_kafka_consumer()
```

## Configuration

### Environment Variables

Configure Kafka connection in [control_plane/app/core/config.py](control_plane/app/core/config.py):

```python
KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
KAFKA_TOPIC_CDC: str = "cdc.integration.events"
```

In Docker Compose ([docker-compose.yml:269](docker-compose.yml#L269)):

```yaml
environment:
  KAFKA_BOOTSTRAP_SERVERS: kafka:29092
```

### Consumer Settings

- **Group ID**: `control-plane-consumer` - Allows multiple Control Plane instances to share workload
- **Auto Offset Reset**: `earliest` - Start from beginning on first run
- **Auto Commit**: Enabled - Automatically commit offsets
- **Consumer Timeout**: 1 second - For graceful shutdown
- **Max Records per Poll**: 10 messages

## Event Processing

### Default Event Processing

The service includes default processing logic for each event type:

**integration.created**:
```python
# TODO: Trigger initial data sync if auto_trigger is enabled
logger.info(f"Integration created: {integration_id}")
```

**integration.updated**:
```python
# TODO: Update configuration in Airflow if needed
logger.info(f"Integration updated: {integration_id}")
```

**integration.deleted**:
```python
# TODO: Cleanup Airflow DAG if needed
logger.info(f"Integration deleted: {integration_id}")
```

**integration.run.failed**:
```python
# TODO: Send alerts, retry logic, etc.
logger.error(f"Integration run failed: {run_id}")
```

### Custom Event Handler

You can provide a custom event handler function:

```python
def my_event_handler(event_type: str, data: dict) -> None:
    """Custom event processing logic."""
    if event_type == "integration.created":
        # Trigger initial sync
        integration_service.trigger_integration(data["integration_id"])
    elif event_type == "integration.run.failed":
        # Send alert
        alert_service.notify_failure(data)

# Initialize with custom handler
initialize_kafka_consumer(event_handler=my_event_handler)
```

## Running the Consumer

### Start Control Plane Service

The Kafka consumer starts automatically when the Control Plane service starts:

```bash
# Using Docker Compose
docker-compose up -d control-plane

# Or locally
cd control_plane
python -m control_plane.app.main
```

### Verify Consumer is Running

Check the logs:

```bash
# Docker logs
docker-compose logs -f control-plane

# Expected output:
# INFO:control_plane.app.services.kafka_consumer_service:Starting Kafka consumer for topic: cdc.integration.events
# INFO:control_plane.app.services.kafka_consumer_service:Kafka consumer connected to kafka:29092
# INFO:control_plane.app.services.kafka_consumer_service:Subscribed to topic: cdc.integration.events
# INFO:control_plane.app.main:Kafka consumer initialized successfully
```

### Monitor with Kafka UI

Open Kafka UI to view consumer activity: http://localhost:8081

- **Consumer Groups**: View `control-plane-consumer` group
- **Consumer Lag**: Check if consumer is keeping up with events
- **Messages**: View CDC event messages

## Testing

### Unit Tests

Test the consumer service in isolation:

```python
from control_plane.app.services.kafka_consumer_service import KafkaConsumerService

def test_event_processing():
    events_processed = []

    def test_handler(event_type, data):
        events_processed.append(event_type)

    consumer = KafkaConsumerService(
        bootstrap_servers="localhost:9092",
        topic="test-topic",
        event_handler=test_handler
    )

    # Test event processing
    consumer._process_message({
        "event_type": "integration.created",
        "event_id": "test-123",
        "timestamp": "2026-02-04T12:00:00Z",
        "data": {"integration_id": 1}
    })

    assert "integration.created" in events_processed
```

### Integration Tests

The existing CDC tests in [control_plane/tests/test_cdc_kafka.py](control_plane/tests/test_cdc_kafka.py) verify Kafka connectivity and message publishing.

To test the consumer:

```bash
# Start services
docker-compose up -d

# Run CDC tests
pytest control_plane/tests/test_cdc_kafka.py -v -s
```

### Manual Testing

1. **Publish Test Event**:

```bash
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic cdc.integration.events

# Paste this JSON and press Enter:
{"event_type":"integration.created","event_id":"test-1","timestamp":"2026-02-04T12:00:00Z","data":{"integration_id":1}}
```

2. **View Consumer Logs**:

```bash
docker-compose logs -f control-plane | grep "Processing CDC event"

# Expected output:
# INFO:control_plane.app.services.kafka_consumer_service:Processing CDC event: integration.created
```

## Error Handling

### Connection Failures

If Kafka is unavailable during startup:
- Consumer logs error but application continues to run
- API endpoints remain functional
- Consumer will not be available until manual restart

```python
try:
    initialize_kafka_consumer()
    logger.info("Kafka consumer initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Kafka consumer: {e}")
    # Don't crash the application
```

### Message Processing Errors

If an individual message fails to process:
- Error is logged with full context
- Consumer continues processing other messages
- Failed message offset is still committed

```python
try:
    self._process_message(record.value)
except Exception as e:
    logger.error(
        f"Error processing message: {e}",
        exc_info=True,
        extra={"message": record.value}
    )
```

### Graceful Shutdown

On application shutdown:
- Consumer stops polling for new messages
- In-flight messages are allowed to complete (5 second timeout)
- Offsets are committed before closing connection

## Troubleshooting

### Issue: Consumer Not Starting

**Symptoms**:
```
ERROR: Failed to initialize Kafka consumer: NoBrokersAvailable
```

**Solutions**:
```bash
# Check Kafka is running
docker-compose ps kafka

# Check Kafka logs
docker-compose logs kafka

# Verify Kafka connectivity
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Restart Kafka
docker-compose restart kafka
```

### Issue: Consumer Lag

**Symptoms**: Consumer group has high lag in Kafka UI

**Solutions**:
- Check Control Plane logs for processing errors
- Increase consumer timeout or batch size
- Deploy multiple Control Plane instances for load distribution

### Issue: Events Not Being Consumed

**Check**:
```bash
# Verify topic exists
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check messages in topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.integration.events \
  --from-beginning \
  --max-messages 10

# Verify consumer group
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group control-plane-consumer
```

### Issue: Duplicate Event Processing

**Cause**: Multiple Control Plane instances with same consumer group

**Solution**: This is expected behavior. Kafka distributes partitions across consumers in the same group.

If you need single-instance processing:
```python
# Use a unique group ID per instance
group_id=f"control-plane-consumer-{instance_id}"
```

## Production Considerations

### 1. Scalability

For high-throughput scenarios:
- Deploy multiple Control Plane instances
- All instances join the same consumer group
- Kafka automatically distributes partitions

### 2. Monitoring

Essential metrics to monitor:
- Consumer lag (messages behind)
- Processing rate (messages/second)
- Error rate (failed message processing)
- Rebalance frequency

### 3. Security

For production:
```python
# Enable SSL/TLS
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=servers,
    security_protocol="SSL",
    ssl_cafile="/path/to/ca-cert",
    ssl_certfile="/path/to/client-cert",
    ssl_keyfile="/path/to/client-key",
)

# Enable SASL authentication
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=servers,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="username",
    sasl_plain_password="password",
)
```

### 4. Event Ordering

Within a single partition:
- Events are processed in order
- Use integration_id as partition key for ordering guarantees

Across partitions:
- No ordering guarantees
- Design event handlers to be idempotent

### 5. Idempotency

Event handlers should be idempotent:
```python
def idempotent_handler(event_type: str, data: dict):
    """Handler that can safely process duplicate events."""
    if event_type == "integration.created":
        # Check if integration already exists
        integration = db.query(Integration).filter_by(
            integration_id=data["integration_id"]
        ).first()

        if integration:
            logger.info(f"Integration {integration_id} already exists, skipping")
            return

        # Create integration
        create_integration(data)
```

## Next Steps

1. **Implement CDC Triggering**: Automatically trigger DAG runs when integrations are created
2. **Add Retry Logic**: Implement exponential backoff for failed event processing
3. **Alerting**: Send notifications for integration failures
4. **Metrics**: Export consumer metrics to Prometheus/OpenTelemetry
5. **Dead Letter Queue**: Move failed messages to DLQ for manual review

## Related Documentation

- [E2E Test Guide](E2E_TEST_GUIDE.md)
- [Testing Guide](TESTING.md)
- [CDC Kafka Tests](control_plane/tests/test_cdc_kafka.py)
- [Control Plane API](http://localhost:8000/docs)
- [Kafka UI](http://localhost:8081)
