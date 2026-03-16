# Dead Letter Queue (DLQ) Implementation

## Overview

This document describes the Dead Letter Queue (DLQ) implementation for handling poison pills and failed message processing in the Kafka consumer service.

## What is a Poison Pill?

A **poison pill** is a message that cannot be processed successfully and causes the consumer to repeatedly fail and retry indefinitely. Without proper handling, poison pills can:
- Block the consumer from processing other messages
- Cause infinite retry loops
- Consume system resources
- Trigger cascading failures

## DLQ Architecture

```
1. Message Arrives
   Kafka Topic: cdc.integration.events
   |
2. Consumer Attempts Processing
   Try #1: Error (Exception)
   Try #2: Error (Exception)
   Try #3: Error (Exception)
   |
3. Max Retries Exceeded (default: 3)
   |
4. Send to Dead Letter Queue
   PRIMARY:  MySQL dead_letter_messages table (durable, queryable)
   SECONDARY: Kafka Topic cdc.integration.events.dlq (optional, for real-time alerting)
   |
5. Continue Processing Other Messages
   Consumer is not blocked!
   |
6. DLQ Management via REST API
   GET  /dlq          - List/filter entries
   GET  /dlq/stats    - Counts by status
   POST /dlq/{id}/retry  - Re-process message
   PUT  /dlq/{id}/resolve - Mark as resolved
   POST /dlq/bulk/resolve - Bulk resolve
   POST /dlq/bulk/retry   - Bulk retry
```

## Implementation Details

### 1. Kafka Consumer Service

The `KafkaConsumerService` class has been enhanced with DLQ support:

**File**: [`kafka_consumer/app/services/kafka_consumer_service.py`](kafka_consumer/app/services/kafka_consumer_service.py)

**Key Components**:

#### MessageRetryTracker
- Tracks retry attempts in memory
- Thread-safe (uses threading.Lock)
- Configurable max retries (default: 3)

```python
retry_tracker = MessageRetryTracker(max_retries=3)
```

#### DLQ Producer
- Separate Kafka producer for DLQ messages
- Initialized on service start
- Enriches messages with error metadata

```python
dlq_producer = KafkaProducer(
    bootstrap_servers=[bootstrap_servers],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
```

#### Error Handling Flow

```python
try:
    self._process_message(message)
    # Success - reset retry counter
    self.retry_tracker.reset_retry(message_key)
except Exception as e:
    # Increment retry count
    retry_count = self.retry_tracker.increment_retry(message_key)

    if retry_count >= self.max_retries:
        # Send to DLQ
        self._send_to_dlq(message, e, retry_count)
```

### 2. Database Persistence (Primary Store)

**Table**: `dead_letter_messages` in [`packages/shared_models/shared_models/tables.py`](packages/shared_models/shared_models/tables.py)

**Repository**: [`kafka_consumer/app/services/dlq_repository.py`](kafka_consumer/app/services/dlq_repository.py)

Failed messages are persisted to MySQL as the primary durable store. The Kafka DLQ topic remains as an optional secondary channel for real-time alerting subscribers.

Key features:
- **Durable**: Survives Kafka topic retention expiry
- **Queryable**: Filter by status, integration_id, date range
- **Manageable**: Retry, resolve, and bulk operations via REST API
- **Observable**: DLQ stats included in `/health/detailed` endpoint

The `integration_id` column uses a soft reference (no FK) so DLQ entries survive integration deletion.

### 3. REST API Endpoints

**File**: [`kafka_consumer/app/api/dlq.py`](kafka_consumer/app/api/dlq.py)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/dlq` | List entries (paginated, filterable) |
| `GET` | `/dlq/stats` | Counts by status |
| `GET` | `/dlq/{dlq_id}` | Single entry details |
| `POST` | `/dlq/{dlq_id}/retry` | Re-process original message |
| `PUT` | `/dlq/{dlq_id}/resolve` | Mark as resolved with notes |
| `POST` | `/dlq/bulk/resolve` | Bulk resolve |
| `POST` | `/dlq/bulk/retry` | Bulk retry |

### 4. Configuration

**File**: [`kafka_consumer/app/core/config.py`](kafka_consumer/app/core/config.py)

```python
# Dead Letter Queue configuration
KAFKA_TOPIC_DLQ: str = "cdc.integration.events.dlq"
KAFKA_DLQ_ENABLED: bool = True
KAFKA_DLQ_MAX_RETRIES: int = 3
KAFKA_DLQ_DB_ENABLED: bool = True  # Enable DB-backed DLQ persistence
```

### 3. Kafka Topics

Two topics are created:

#### Main Topic: `cdc.integration.events`
- **Purpose**: Primary CDC events from Debezium
- **Retention**: 7 days
- **Partitions**: 3
- **Replication Factor**: 1 (adjust for production)

#### DLQ Topic: `cdc.integration.events.dlq`
- **Purpose**: Failed messages after max retries
- **Retention**: 30 days (longer for investigation)
- **Partitions**: 3
- **Replication Factor**: 1 (adjust for production)

### 4. DLQ Message Format

Messages in the DLQ topic include:

```json
{
  "original_message": {
    "integration_id": 123,
    "workspace_id": "ws-001",
    "__op": "c",
    ...
  },
  "error": {
    "type": "ValueError",
    "message": "Invalid configuration",
    "timestamp": "2026-02-04T10:30:00.000Z"
  },
  "retry_count": 3,
  "source_topic": "cdc.integration.events",
  "consumer_group": "cdc-consumer"
}
```

## Setup Instructions

### Docker

#### 1. Create Kafka Topics

Run the topic creation script:

```bash
# Method 1: Use the script directly
docker exec kafka bash /docker/kafka-create-topics.sh

# Method 2: Manual creation
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic cdc.integration.events.dlq \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=2592000000 \
  --if-not-exists
```

#### 2. Verify Topics Created

```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Expected output:
# cdc.integration.events
# cdc.integration.events.dlq
# test.cdc.integration.events
```

#### 3. Restart Control Plane

```bash
docker-compose restart kafka-consumer
```

#### 4. Verify DLQ Enabled

```bash
docker-compose logs kafka-consumer | grep "DLQ"

# Expected output:
# Initializing Kafka consumer: kafka:29092, topic: cdc.integration.events, DLQ: True (topic: cdc.integration.events.dlq, max_retries: 3)
# DLQ producer initialized for topic: cdc.integration.events.dlq
# Kafka consumer initialized successfully with DLQ support (max_retries: 3)
```

### Kubernetes

#### 1. Apply Kafka Topics ConfigMap

```bash
kubectl apply -f k8s/configmaps/kafka-topics-config.yaml
```

#### 2. Run Topic Creation Job

```bash
# Check if topics exist
kubectl logs -n airflow -l component=topics-creator

# Verify job completed
kubectl get jobs -n airflow kafka-topics-creator

# Expected output:
# NAME                    COMPLETIONS   DURATION   AGE
# kafka-topics-creator    1/1           10s        1m
```

#### 3. Update Kafka Consumer ConfigMap

Edit ConfigMap for the kafka-consumer deployment (or create `k8s/configmaps/kafka-consumer-config.yaml`):

```yaml
# Dead Letter Queue Configuration
KAFKA_TOPIC_DLQ: "cdc.integration.events.dlq"
KAFKA_DLQ_ENABLED: "True"
KAFKA_DLQ_MAX_RETRIES: "3"
```

Apply the changes:

```bash
kubectl apply -f k8s/configmaps/kafka-consumer-config.yaml
kubectl rollout restart deployment/kafka-consumer -n airflow
```

## Monitoring DLQ

### 1. Check DLQ Message Count

```bash
# Docker
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.integration.events.dlq \
  --from-beginning \
  --timeout-ms 5000 \
  | wc -l

# Kubernetes
kubectl exec -n airflow kafka-0 -- \
  kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.integration.events.dlq \
  --from-beginning \
  --timeout-ms 5000 \
  | wc -l
```

### 2. View DLQ Messages

```bash
# Docker
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.integration.events.dlq \
  --from-beginning \
  --max-messages 10

# Kubernetes
kubectl exec -n airflow kafka-0 -- \
  kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.integration.events.dlq \
  --from-beginning \
  --max-messages 10
```

### 3. Monitor Consumer Logs

```bash
# Docker
docker-compose logs -f kafka-consumer | grep "DLQ\|retry"

# Kubernetes
kubectl logs -f -n airflow -l app=kafka-consumer | grep "DLQ\|retry"
```

### 4. Use Kafka UI

Access Kafka UI: http://localhost:8081 (Docker) or appropriate service endpoint (K8s)

Navigate to:
- **Topics** → `cdc.integration.events.dlq`
- View message count, partition distribution
- Browse individual messages

## Testing DLQ

### 1. Simulate Poison Pill

Create a test that triggers failures:

```python
# Inject invalid message
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send message that will cause processing error
poison_pill = {
    "integration_id": 999999,  # Non-existent
    "workspace_id": "invalid",
    "__op": "c",
    # Missing required fields
}

producer.send('cdc.integration.events', poison_pill)
producer.flush()
```

### 2. Verify DLQ Behavior

```bash
# Wait for retries (3 attempts with consumer timeout)
sleep 10

# Check DLQ topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.integration.events.dlq \
  --from-beginning \
  --max-messages 1
```

Expected output: Message with error metadata

### 3. Verify Consumer Continues

```bash
# Send valid message after poison pill
# Verify it gets processed successfully
# Consumer should NOT be blocked
```

## DLQ Recovery Strategies

### Strategy 1: Fix and Replay

1. **Investigate** the error in DLQ message
2. **Fix** the root cause (code bug, data issue, etc.)
3. **Extract** messages from DLQ
4. **Replay** to main topic

```bash
# Extract DLQ messages to file
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.integration.events.dlq \
  --from-beginning \
  > dlq_messages.json

# Process and replay (after fixing the issue)
python scripts/replay_dlq_messages.py dlq_messages.json
```

### Strategy 2: Manual Processing

1. **Review** DLQ messages
2. **Process manually** (database updates, API calls, etc.)
3. **Mark as resolved** (delete from DLQ or move to resolved topic)

### Strategy 3: Alert and Investigate

1. **Set up alerts** on DLQ message count
2. **Investigate** root cause when alert fires
3. **Prevent** similar issues in the future
4. **Purge** DLQ after resolution

## Configuration Options

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_TOPIC_DLQ` | `cdc.integration.events.dlq` | DLQ Kafka topic name |
| `KAFKA_DLQ_ENABLED` | `True` | Enable Kafka DLQ topic (secondary) |
| `KAFKA_DLQ_MAX_RETRIES` | `3` | Max retry attempts before DLQ |
| `KAFKA_DLQ_DB_ENABLED` | `True` | Enable database DLQ persistence (primary) |

### Tuning Parameters

#### Max Retries
- **Low (1-2)**: Fast failure, more messages in DLQ
- **Medium (3-5)**: Balanced (recommended)
- **High (10+)**: Slower failure detection, fewer DLQ messages

#### DLQ Topic Retention
- **Short (7 days)**: For low-criticality data
- **Medium (30 days)**: Recommended for investigation time
- **Long (90+ days)**: For compliance/audit requirements

#### Consumer Timeout
- **Short (1s)**: Fast processing, quick retry cycles
- **Medium (5s)**: Balanced
- **Long (10s+)**: Allows for longer processing, slower retries

## Alerting

### Recommended Alerts

#### 1. DLQ Message Count Alert

```yaml
alert: DLQMessagesPresent
expr: kafka_topic_partition_current_offset{topic="cdc.integration.events.dlq"} > 0
for: 5m
annotations:
  summary: "DLQ has messages - investigate failures"
  description: "{{ $value }} messages in Dead Letter Queue"
```

#### 2. High DLQ Rate Alert

```yaml
alert: HighDLQRate
expr: rate(kafka_topic_partition_current_offset{topic="cdc.integration.events.dlq"}[5m]) > 10
for: 10m
annotations:
  summary: "High rate of messages sent to DLQ"
  description: "More than 10 messages/5min sent to DLQ"
```

#### 3. DLQ Consumer Lag Alert

```yaml
alert: DLQConsumerLag
expr: kafka_consumergroup_lag{topic="cdc.integration.events"} > 1000
for: 15m
annotations:
  summary: "Consumer falling behind - may indicate poison pills"
  description: "Consumer lag: {{ $value }} messages"
```

## Best Practices

### 1. Monitor DLQ Regularly
- Set up dashboards for DLQ metrics
- Review DLQ messages weekly
- Investigate patterns in failures

### 2. Keep DLQ Retention Long
- 30 days minimum (recommended)
- Allows time for investigation
- Enables correlation with other events

### 3. Add Detailed Error Context
- Include stack traces in DLQ messages
- Add request/response data when applicable
- Track correlation IDs

### 4. Test Poison Pill Scenarios
- Regularly test DLQ with invalid messages
- Verify consumer continues processing
- Practice recovery procedures

### 5. Implement Circuit Breaker
- If DLQ rate is too high, pause consumer
- Investigate and fix root cause
- Resume when issue resolved

### 6. Use Separate Consumer for DLQ
- Create dedicated consumer for DLQ topic
- Implement replay/recovery logic
- Keep separate from main processing

## Troubleshooting

### Issue: Messages Not Sent to DLQ

**Possible Causes**:
- DLQ disabled (`KAFKA_DLQ_ENABLED=False`)
- DLQ producer initialization failed
- Topic doesn't exist

**Solution**:
```bash
# Check configuration
docker-compose logs kafka-consumer | grep "DLQ"

# Verify topic exists
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check DLQ producer
docker-compose logs kafka-consumer | grep "DLQ producer"
```

### Issue: Consumer Stops Processing

**Possible Causes**:
- Poison pill not being sent to DLQ
- Retry count not incrementing
- DLQ topic full or unavailable

**Solution**:
```bash
# Check retry logs
docker-compose logs kafka-consumer | grep "attempt\|retry"

# Verify DLQ topic is writable
docker exec kafka kafka-topics --describe --bootstrap-server localhost:9092 --topic cdc.integration.events.dlq
```

### Issue: High Memory Usage

**Possible Causes**:
- In-memory retry tracker growing indefinitely
- Not cleaning up old entries

**Solution**:
- Implement TTL for retry tracker entries
- Use Redis for distributed retry tracking
- Monitor and limit retry tracker size

## Future Enhancements

### 1. Persistent Retry Tracking
- Use Redis instead of in-memory tracking
- Survive consumer restarts
- Enable distributed consumer groups

### 2. Exponential Backoff
- Add delay between retry attempts
- Reduce load during transient failures
- Configurable backoff strategy

### 3. Dead Letter Analytics
- Track failure patterns over time
- Identify problematic integrations
- Dashboard for DLQ trends

### 4. Automated DLQ Cleanup
- Periodic job to expire old resolved entries
- Configurable retention for resolved vs pending entries

## Summary

**Implemented**:
- Dead Letter Queue for poison pill handling
- Configurable retry logic (max 3 retries)
- Error metadata enrichment
- Database persistence as primary durable store (survives Kafka retention expiry)
- Kafka DLQ topic as optional secondary (for real-time alerting)
- REST API for DLQ management (list, retry, resolve, bulk operations)
- DLQ stats in `/health/detailed` endpoint
- Docker and Kubernetes support

**Benefits**:
- **Resilience**: Consumer never blocked by poison pills
- **Durability**: Failed messages stored permanently in MySQL, not lost after Kafka retention
- **Observability**: All failures captured with context, queryable via API
- **Recovery**: Messages can be retried via API after fixing issues
- **Production-Ready**: Follows industry best practices

For more information:
- [Kafka Consumer Service](kafka_consumer/app/services/kafka_consumer_service.py)
- [DLQ Repository](kafka_consumer/app/services/dlq_repository.py)
- [DLQ API](kafka_consumer/app/api/dlq.py)
- [Configuration](kafka_consumer/app/core/config.py)
- [Debezium CDC Implementation](DEBEZIUM_CDC_IMPLEMENTATION.md)
