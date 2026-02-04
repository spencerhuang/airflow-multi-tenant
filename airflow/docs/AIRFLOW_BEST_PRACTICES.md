# Airflow Best Practices for Multi-Tenant Architecture

## 📘 Overview

This document consolidates all Airflow best practices from the architecture specification (Sections 4, 5, 10, 11, and 12). Following these practices ensures scalability, maintainability, and operational safety in our multi-tenant Airflow deployment.

---

## 🎯 Core Principles

### The Multi-Tenant Model
- **One DAG = One Workflow Type** (e.g., `s3_to_mongo`)
- **Tenants are NOT encoded in DAGs**
- **Each DAG run is parameterized** with `tenant_id` and `integration_id`
- **Scales to thousands of tenants** without DAG explosion

---

## 📂 Section 1: Code Organization

### 1.1 Directory Structure

Per spec Section 11, follow this strict organization:

| Component | Location | Purpose |
|-----------|----------|---------|
| **DAGs** | `dags/` | Only DAG wiring and task dependencies |
| **Operators** | `plugins/operators/` | Custom Airflow operators |
| **Hooks** | `plugins/hooks/` | Connection management and API wrappers |
| **Logging** | `plugins/logging/` | Custom logging handlers |
| **Business Logic** | External Python modules | Pure Python logic, no Airflow dependency |
| **Connectors** | `connectors/` | Reusable data source modules (S3, Azure, etc.) |
| **Spark Jobs** | External scripts/JARs | Spark applications |

### 1.2 Current Implementation

```
airflow-multi-tenant/
├── airflow/
│   ├── dags/                          # DAG files only
│   │   ├── s3_to_mongo_daily_00.py
│   │   ├── s3_to_mongo_daily_01.py
│   │   ├── ...
│   │   ├── s3_to_mongo_daily_23.py
│   │   └── s3_to_mongo_ondemand.py
│   ├── plugins/
│   │   ├── operators/                 # Custom operators
│   │   │   └── s3_to_mongo_operators.py
│   │   └── hooks/                     # (Future: custom hooks)
│   └── docs/                          # Documentation
├── connectors/                        # External, reusable
│   ├── s3/
│   │   ├── client.py
│   │   ├── auth.py
│   │   └── reader.py
│   ├── azure_blob/
│   ├── mongo/
│   └── mysql/
└── control_plane/                     # Business logic
```

---

## 📄 Section 2: DAG File Best Practices

### 2.1 The Critical Rules

Per spec Section 11:

✅ **DO:**
- **Contain ONLY DAG wiring** (task definitions and dependencies)
- Keep files lightweight and fast to parse
- Use templating for dynamic values (`{{ dag_run.conf['tenant_id'] }}`)

❌ **DON'T:**
- No heavy imports (defer to task execution time)
- No SDK calls (no `boto3.client()`, `pymongo.MongoClient()` at module level)
- No business logic (move to operators or external modules)
- No database queries (defer to tasks)

### 2.2 Good Example

```python
"""
S3 to MongoDB Daily DAG - Scheduled for 02:00 UTC.
✅ GOOD: Only DAG wiring, no heavy logic
"""
from datetime import datetime, timedelta
from airflow import DAG

# ✅ Import operators (lightweight)
from plugins.operators.s3_to_mongo_operators import (
    PrepareS3ToMongoTask,
    ValidateS3ToMongoTask,
    ExecuteS3ToMongoTask,
    CleanUpS3ToMongoTask,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="s3_to_mongo_daily_02",
    default_args=default_args,
    description="S3 to MongoDB daily workflow at 02:00 UTC",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=10,
    max_active_tasks=5,
) as dag:

    # ✅ Simple task instantiation
    prepare = PrepareS3ToMongoTask(task_id="prepare")
    validate = ValidateS3ToMongoTask(task_id="validate")
    execute = ExecuteS3ToMongoTask(task_id="execute")
    cleanup = CleanUpS3ToMongoTask(task_id="cleanup")

    # ✅ Clear dependency chain
    prepare >> validate >> execute >> cleanup
```

### 2.3 Bad Example (Anti-Pattern)

```python
"""
❌ BAD: Heavy imports, SDK calls, business logic in DAG file
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3  # ❌ Heavy import
import pymongo  # ❌ Heavy import
import pandas as pd  # ❌ Heavy import
import logging

# ❌ SDK client at module level - runs during DAG parsing!
s3_client = boto3.client('s3')
mongo_client = pymongo.MongoClient("mongodb://...")

# ❌ Business logic in DAG file
def process_data(**context):
    """❌ Business logic should be in external modules"""
    bucket = context['dag_run'].conf['s3_bucket']

    # ❌ SDK calls in DAG file
    response = s3_client.get_object(Bucket=bucket, Key='data.json')
    data = json.loads(response['Body'].read())

    # ❌ Data transformation in DAG file
    df = pd.DataFrame(data)
    df['processed'] = df['value'].apply(lambda x: x * 2)

    # ❌ Database writes in DAG file
    mongo_client.db.collection.insert_many(df.to_dict('records'))

with DAG(dag_id="bad_example", ...) as dag:
    # ❌ Inline business logic
    task = PythonOperator(
        task_id="process",
        python_callable=process_data,
    )
```

**Why this is bad:**
- Heavy imports slow down DAG parsing (runs every 30 seconds by default)
- SDK clients created during parsing waste resources
- Business logic in DAG file is not reusable or testable
- Database connections opened during parsing cause connection exhaustion

---

## 🔧 Section 3: Operator Best Practices

### 3.1 Operator Design Principles

✅ **DO:**
- Extend `BaseOperator` for custom operators
- Keep operators lightweight (orchestration only)
- Delegate business logic to external modules
- Use XCom for small data passing between tasks
- Document all parameters with docstrings

❌ **DON'T:**
- Don't put business logic in operators
- Don't create heavy objects in `__init__` (use `execute()`)
- Don't return large data from tasks (use external storage)

### 3.2 Good Operator Example

```python
"""
plugins/operators/s3_to_mongo_operators.py
✅ GOOD: Operator delegates to external modules
"""
from airflow.models.baseoperator import BaseOperator
from connectors.s3.client import S3Client
from connectors.mongo.client import MongoClient


class ExecuteS3ToMongoTask(BaseOperator):
    """
    Executes S3 to MongoDB data transfer.

    ✅ Lightweight operator that delegates to connectors
    """

    def __init__(self, task_id: str, **kwargs):
        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context):
        """
        Execute the S3 to MongoDB transfer.

        ✅ Business logic in external connectors, not in operator
        """
        # Get config from XCom
        config = context['ti'].xcom_pull(task_ids='prepare')

        # ✅ Delegate to external connectors
        s3_client = S3Client(
            bucket=config['s3_bucket'],
            prefix=config['s3_prefix']
        )

        mongo_client = MongoClient(
            connection_string=config['mongo_uri'],
            database=config['mongo_db'],
            collection=config['mongo_collection']
        )

        # ✅ Orchestrate the transfer
        data = s3_client.read_json(config['s3_key'])
        result = mongo_client.insert_many(data)

        # ✅ Return small metadata only
        return {
            "records_processed": len(data),
            "insert_ids": result.inserted_ids[:10]  # Only first 10
        }
```

---

## 🔌 Section 4: Connector Architecture

### 4.1 What Is a Connector?

Per spec Section 5:

A **connector** is a reusable module that wraps a data source API (e.g., S3, Azure Blob, MongoDB).

**Characteristics:**
- ✅ Owned **outside** DAG files
- ✅ Uses official SDKs (e.g., `boto3` for S3)
- ✅ Stateless and reusable
- ✅ **No Airflow dependency**
- ✅ Shared across multiple workflows

### 4.2 Why Connectors?

**Without connectors (bad):**
```
s3_to_mongo DAG    →  S3 logic duplicated
s3_to_mysql DAG    →  S3 logic duplicated
s3_to_snowflake    →  S3 logic duplicated
```

**With connectors (good):**
```
s3_to_mongo DAG    ↘
s3_to_mysql DAG    → S3 Connector (shared)
s3_to_snowflake    ↗
```

### 4.3 Connector Organization

```
connectors/
├── s3/
│   ├── __init__.py
│   ├── client.py         # S3Client class
│   ├── auth.py           # S3Auth class
│   └── reader.py         # S3Reader class
├── azure_blob/
│   ├── __init__.py
│   └── client.py
├── mongo/
│   ├── __init__.py
│   ├── client.py
│   └── auth.py
└── mysql/
    ├── __init__.py
    └── client.py
```

### 4.4 Connector Implementation Example

```python
"""
connectors/s3/client.py
✅ GOOD: No Airflow dependency, pure Python SDK wrapper
"""
import boto3
from typing import Optional, Dict, Any


class S3Client:
    """
    S3 connector that wraps boto3.

    ✅ No Airflow dependency
    ✅ Stateless and reusable
    ✅ Uses official SDK
    """

    def __init__(
        self,
        bucket: str,
        prefix: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1"
    ):
        """Initialize S3 client."""
        self.bucket = bucket
        self.prefix = prefix or ""

        # Create boto3 client (official SDK)
        self.client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

    def read_json(self, key: str) -> Dict[Any, Any]:
        """Read JSON object from S3."""
        response = self.client.get_object(
            Bucket=self.bucket,
            Key=f"{self.prefix}{key}"
        )
        return json.loads(response['Body'].read().decode('utf-8'))

    def list_objects(self, prefix: Optional[str] = None) -> list:
        """List objects in S3 bucket."""
        prefix = prefix or self.prefix
        response = self.client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=prefix
        )
        return response.get('Contents', [])
```

---

## 🏗️ Section 5: Workflow-Based DAG Design

### 5.1 Common Abstract Structure

Per spec Section 4, every workflow follows this pattern:

```
prepare >> validate >> execute >> store >> cleanup
```

### 5.2 Abstract Task Pattern

Define abstract base operators for common steps:

```python
"""
plugins/operators/base_operators.py
✅ Abstract base pattern for reusability
"""
from airflow.models.baseoperator import BaseOperator
from abc import ABC, abstractmethod


class PrepareTask(BaseOperator, ABC):
    """
    Abstract prepare task.

    All workflows implement this pattern with workflow-specific logic.
    """

    @abstractmethod
    def prepare_config(self, context) -> dict:
        """
        Workflow-specific preparation logic.

        Returns:
            dict: Configuration for downstream tasks
        """
        pass

    def execute(self, context):
        """Execute preparation and push config to XCom."""
        config = self.prepare_config(context)
        context['ti'].xcom_push(key='config', value=config)
        return config


class ValidateTask(BaseOperator, ABC):
    """Abstract validate task."""

    @abstractmethod
    def validate_source(self, config: dict) -> bool:
        """Validate source data availability."""
        pass

    @abstractmethod
    def validate_destination(self, config: dict) -> bool:
        """Validate destination accessibility."""
        pass

    def execute(self, context):
        """Execute validation."""
        config = context['ti'].xcom_pull(task_ids='prepare', key='config')

        if not self.validate_source(config):
            raise ValueError("Source validation failed")

        if not self.validate_destination(config):
            raise ValueError("Destination validation failed")

        return True
```

### 5.3 Workflow-Specific Implementation

```python
"""
plugins/operators/s3_to_mongo_operators.py
✅ Workflow-specific implementation of abstract pattern
"""
from plugins.operators.base_operators import PrepareTask, ValidateTask
from connectors.s3.client import S3Client
from connectors.mongo.client import MongoClient


class PrepareS3ToMongoTask(PrepareTask):
    """S3 to MongoDB specific preparation."""

    def prepare_config(self, context) -> dict:
        """Resolve S3 and MongoDB configuration."""
        dag_conf = context['dag_run'].conf

        return {
            'integration_id': dag_conf['integration_id'],
            'tenant_id': dag_conf['tenant_id'],
            's3_bucket': dag_conf['s3_bucket'],
            's3_prefix': dag_conf['s3_prefix'],
            'mongo_uri': dag_conf['mongo_uri'],
            'mongo_db': dag_conf['mongo_db'],
            'mongo_collection': dag_conf['mongo_collection'],
        }


class ValidateS3ToMongoTask(ValidateTask):
    """S3 to MongoDB specific validation."""

    def validate_source(self, config: dict) -> bool:
        """Validate S3 bucket and objects exist."""
        s3_client = S3Client(
            bucket=config['s3_bucket'],
            prefix=config['s3_prefix']
        )
        objects = s3_client.list_objects()
        return len(objects) > 0

    def validate_destination(self, config: dict) -> bool:
        """Validate MongoDB connection."""
        mongo_client = MongoClient(
            connection_string=config['mongo_uri']
        )
        return mongo_client.ping()
```

**Why this pattern?**
- ✅ Consistent structure across all workflows
- ✅ Shared logic in base classes
- ✅ Workflow-specific behavior injected at runtime
- ✅ No code duplication

---

## ⚡ Section 6: Scheduler & Worker Safety

### 6.1 Critical Rules (Spec Section 12)

1. **Prefer deferrable operators and async sensors**
2. **Avoid blocking worker slots**
3. **Use correct sensor mode (`reschedule` vs `poke`)**
4. **Limit DAG parsing cost**

### 6.2 Task Concurrency Limits

✅ **REQUIRED: Always set `max_active_tasks`**

```python
with DAG(
    dag_id="s3_to_mongo_daily_02",
    max_active_runs=100,              # Limit concurrent DAG instances
    max_active_tasks=50,              # ✅ REQUIRED - prevents worker exhaustion
    catchup=False,
):
```

**Why?**
- Without `max_active_tasks`: A DAG with 100 tasks could run all 100 simultaneously
- 10 concurrent DAG runs × 100 tasks = 1000 worker slots needed
- With `max_active_tasks=5`: Only 50 worker slots needed (95% reduction)

### 6.3 Exponential Backoff Retries

✅ **REQUIRED: Use exponential backoff**

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,      # ✅ REQUIRED
    "max_retry_delay": timedelta(minutes=15),
}
```

**Retry timeline:**
- 1st retry: 1 minute
- 2nd retry: 2 minutes
- 3rd retry: 4 minutes
- Total: 7 minutes (vs 15 minutes with fixed 5-minute delays)

### 6.4 Sensor Best Practices

✅ **CRITICAL: Always use `mode='reschedule'`**

See [SENSOR_BEST_PRACTICES.md](SENSOR_BEST_PRACTICES.md) for complete guide.

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# ✅ GOOD: Frees worker between checks
sensor = S3KeySensor(
    task_id='wait_for_file',
    bucket_name='my-bucket',
    bucket_key='data/file.json',
    mode='reschedule',      # ✅ REQUIRED
    poke_interval=60,
    timeout=3600,
)
```

**Impact:**
- `mode='poke'`: 100 tenants = 100 blocked worker slots ❌
- `mode='reschedule'`: 100 tenants = ~5-10 active worker slots ✅

---

## 🚀 Section 7: DAG Configuration Best Practices

### 7.1 Required DAG Parameters

Every DAG MUST have these configurations:

```python
with DAG(
    dag_id="workflow_name",

    # ✅ Schedule
    schedule="0 2 * * *",        # Daily DAGs: Use cron
    # OR
    schedule=None,                # On-demand DAGs: No schedule

    # ✅ Date Configuration
    start_date=datetime(2024, 1, 1),
    catchup=False,                # ✅ REQUIRED - prevent backfills

    # ✅ Concurrency Control
    max_active_runs=10,           # ✅ Limit concurrent DAG instances
    max_active_tasks=5,           # ✅ REQUIRED - limit task parallelism

    # ✅ Metadata
    description="Clear description of workflow purpose",
    tags=["source", "destination", "frequency", "workflow_type"],

    # ✅ Default Arguments
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
    },
) as dag:
```

### 7.2 Daily Scheduled DAGs

For daily workflows (spec Section 6.1):

```python
with DAG(
    dag_id="s3_to_mongo_daily_02",
    schedule="0 2 * * *",         # Runs at 02:00 UTC
    max_active_runs=10,           # Up to 10 tenants at this hour
    max_active_tasks=5,
):
```

**Naming convention:**
- `{workflow}_daily_{hour}` (e.g., `s3_to_mongo_daily_02`)
- One DAG per hour (00-23) for distribution

### 7.3 On-Demand DAGs

For CDC, manual, backfill (spec Section 6.2):

```python
with DAG(
    dag_id="s3_to_mongo_ondemand",
    schedule=None,                # ✅ No schedule - API triggered
    max_active_runs=50,           # Higher limit for on-demand
    max_active_tasks=5,
):
```

---

## 📊 Section 8: XCom Best Practices

### 8.1 What to Use XCom For

✅ **DO use XCom for:**
- Task configuration (small dictionaries)
- Metadata (row counts, file paths, IDs)
- Coordination flags (success/failure indicators)

❌ **DON'T use XCom for:**
- Large datasets (use S3, database, or external storage)
- Binary data
- Anything over 1 MB

### 8.2 XCom Pattern

```python
class PrepareTask(BaseOperator):
    def execute(self, context):
        config = {
            'integration_id': 123,
            's3_bucket': 'my-bucket',
            's3_key': 'data/file.json',
        }

        # ✅ Push small config
        context['ti'].xcom_push(key='config', value=config)
        return config


class ExecuteTask(BaseOperator):
    def execute(self, context):
        # ✅ Pull config from previous task
        config = context['ti'].xcom_pull(
            task_ids='prepare',
            key='config'
        )

        # Use config...
```

---

## 🧪 Section 9: Testing Best Practices

### 9.1 Unit Testing (Spec Section 13.1)

✅ **DO:**
- Test operators and hooks in isolation
- No Airflow dependency in pure business logic
- No containers, fast and mocked
- Mock SDK clients (e.g., `boto3`)

```python
"""
tests/test_s3_to_mongo_operators.py
✅ GOOD: Isolated operator testing
"""
from unittest.mock import Mock, patch
from plugins.operators.s3_to_mongo_operators import ExecuteS3ToMongoTask


def test_execute_task():
    """Test ExecuteS3ToMongoTask in isolation."""
    # Mock context
    context = {
        'ti': Mock(),
        'dag_run': Mock(conf={'integration_id': 123}),
    }
    context['ti'].xcom_pull.return_value = {
        's3_bucket': 'test-bucket',
        's3_key': 'data.json',
    }

    # Mock connectors
    with patch('connectors.s3.client.S3Client') as mock_s3:
        with patch('connectors.mongo.client.MongoClient') as mock_mongo:
            mock_s3.return_value.read_json.return_value = [{'id': 1}]

            # Execute task
            task = ExecuteS3ToMongoTask(task_id='execute')
            result = task.execute(context)

            # Verify
            assert result['records_processed'] == 1
```

### 9.2 Connector Testing (Spec Section 13.2)

```python
"""
connectors/tests/test_s3_client.py
✅ GOOD: Mock SDK, fast tests
"""
from unittest.mock import Mock, patch
from connectors.s3.client import S3Client


def test_read_json():
    """Test S3Client.read_json() with mocked boto3."""
    with patch('boto3.client') as mock_boto:
        # Setup mock
        mock_s3 = Mock()
        mock_boto.return_value = mock_s3
        mock_s3.get_object.return_value = {
            'Body': Mock(read=lambda: b'{"key": "value"}')
        }

        # Test
        client = S3Client(bucket='test-bucket')
        result = client.read_json('data.json')

        # Verify
        assert result == {'key': 'value'}
        mock_s3.get_object.assert_called_once_with(
            Bucket='test-bucket',
            Key='data.json'
        )
```

### 9.3 DAG Validation (Spec Section 13.3)

```python
"""
tests/test_dag_integrity.py
✅ GOOD: Static DAG validation
"""
from airflow.models import DagBag


def test_dags_load_without_errors():
    """Test all DAGs can be imported without errors."""
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)

    assert len(dag_bag.import_errors) == 0, \
        f"DAG import errors: {dag_bag.import_errors}"


def test_dag_has_tags():
    """Ensure all DAGs have tags."""
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)

    for dag_id, dag in dag_bag.dags.items():
        assert len(dag.tags) > 0, f"DAG {dag_id} missing tags"


def test_dag_has_owner():
    """Ensure all DAGs have owner."""
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)

    for dag_id, dag in dag_bag.dags.items():
        assert dag.default_args.get('owner'), \
            f"DAG {dag_id} missing owner"
```

---

## 📚 Section 10: Documentation Requirements

### 10.1 DAG Documentation

✅ **Every DAG must have:**
- Module-level docstring explaining purpose
- Usage examples in docstring
- Clear description parameter

```python
"""
S3 to MongoDB Daily DAG - Scheduled for 02:00 UTC.

This DAG runs once per day at 02:00 UTC for all tenants scheduled at this hour.
Each DAG run is parameterized with tenant_id and integration configuration.

Expected configuration:
    {
        "integration_id": 123,
        "tenant_id": "workspace-abc",
        "s3_bucket": "my-bucket",
        "s3_prefix": "data/",
        "mongo_collection": "my_collection"
    }
"""
```

### 10.2 Task Documentation

✅ **Every task should have:**
- `doc_md` parameter with markdown documentation

```python
prepare = PrepareS3ToMongoTask(
    task_id="prepare",
    doc_md="""
    ### Prepare Task
    Resolves S3 bucket, prefix, and MongoDB configuration from dag_run.conf.
    Validates required parameters and pushes config to XCom.

    **Expected Input:**
    - `dag_run.conf['integration_id']`
    - `dag_run.conf['s3_bucket']`
    - `dag_run.conf['mongo_uri']`

    **Output:**
    - Pushes config dict to XCom
    """,
)
```

### 10.3 Operator Documentation

✅ **Every operator must have:**
- Class docstring with usage example
- Parameter documentation
- Return value documentation

```python
class ExecuteS3ToMongoTask(BaseOperator):
    """
    Executes S3 to MongoDB data transfer.

    This operator reads data from S3 and writes to MongoDB. It handles
    data transformation and batch writing for optimal performance.

    Args:
        task_id: Unique task identifier
        **kwargs: Additional BaseOperator arguments

    Returns:
        dict: Execution statistics including records_processed

    Example:
        >>> execute = ExecuteS3ToMongoTask(task_id='execute')
        >>> result = execute.execute(context)
        >>> print(result['records_processed'])
    """
```

---

## 🎯 Section 11: Summary Checklist

### Before Creating a New DAG

- [ ] Does this follow the workflow pattern? (`prepare >> validate >> execute >> cleanup`)
- [ ] Is it parameterized by `tenant_id` and `integration_id`?
- [ ] Does it use connectors instead of duplicating SDK code?
- [ ] Are heavy imports avoided in the DAG file?
- [ ] Does it have `max_active_tasks` configured?
- [ ] Does it use exponential backoff for retries?
- [ ] Are all sensors using `mode='reschedule'`?
- [ ] Does it have comprehensive documentation?
- [ ] Have you written unit tests for new operators?

### Before Deploying to Production

- [ ] Run `pytest` - all tests passing
- [ ] Run DAG validation tests
- [ ] Check DAG parsing time (<1 second)
- [ ] Verify `catchup=False` to prevent backfill storms
- [ ] Test with sample `dag_run.conf` data
- [ ] Review worker slot requirements
- [ ] Confirm error handling and logging
- [ ] Document any new connectors or operators

---

## 📖 Related Documentation

- [DST Developer Guide](DST_DEVELOPER_GUIDE.md) - Daylight Saving Time handling
- [Sensor Best Practices](SENSOR_BEST_PRACTICES.md) - Sensor configuration guide
- [Worker Efficiency Improvements](../../WORKER_EFFICIENCY_IMPROVEMENTS.md) - Operational safety
- [Testing Guide](../../control_plane/tests/README.md) - Complete testing strategy

---

## 🔗 References

- **Spec Section 4:** Workflow-Based DAG Design
- **Spec Section 5:** Connector Architecture
- **Spec Section 10:** Example DAGs
- **Spec Section 11:** Airflow Best Practices
- **Spec Section 12:** Scheduler & Worker Safety
- **Spec Section 13:** Testing Strategy

---

**Last Updated:** 2026-02-03
**Status:** ✅ Production Ready
**Compliance:** Fully aligned with specification
