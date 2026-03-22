# Controller DAG Pattern for Scheduled Integrations

## Problem

Airflow's scheduler triggers DAG runs with `conf={}`. Our data pipeline operators (e.g., `PrepareS3ToMongoTask`) require configuration like `s3_bucket`, `mongo_collection`, and credentials — all passed via `dag_run.conf`. This means **scheduled DAGs cannot run the pipeline directly**.

## Solution: Controller DAG with Dynamic Task Mapping (DTM)

A single **Controller DAG** (`s3_to_mongo_controller`) runs every hour and uses DTM to dispatch all due integrations:

```
Airflow Scheduler (cron: 0 * * * *)
  → s3_to_mongo_controller runs find_due_integrations (@task)
    → Queries control plane DB: WHERE utc_next_run <= now
    → For each due integration:
      1. Builds conf dict (same as control plane / Kafka consumer)
      2. Generates trigger_run_id
      3. Advances utc_next_run to next cron occurrence
    → Returns list of {conf, trigger_run_id} dicts
  → TriggerDagRunOperator.expand_kwargs(due)
    → Fires one s3_to_mongo_ondemand DAG run per integration
    → wait_for_completion=False — controller finishes in seconds
```

Each integration gets its own isolated DAG run on `s3_to_mongo_ondemand`, reusing the existing pipeline (`Prepare → Validate → Execute → Cleanup`) without modification.

## Why This Pattern

| Alternative | Problem |
|---|---|
| Pass conf in scheduled DAG | Scheduler provides `conf={}` — impossible |
| Hardcode config in DAG file | Doesn't scale to thousands of tenants |
| Static per-hour DAGs (daily_02, daily_03, ...) | Need 24+ DAG files, operational overhead |
| **Controller + DTM (chosen)** | Single DAG, queries DB at runtime, fires all due runs in seconds |

### Key Benefits

- **Single DAG** replaces 26+ static dispatcher files (24 hourly + weekly + monthly)
- **`utc_next_run <= now`** — universal "is due" check handles daily, weekly, monthly uniformly
- **`wait_for_completion=False`** — controller fires and forgets, no "hanging parent" risk
- **Native TriggerDagRunOperator** — no REST API self-triggering needed
- **`catchup=False`** — the controller itself doesn't backfill, but past-due integrations are caught via `utc_next_run <= now`
- **Per-schedule backfill policy** — daily skips missed runs; weekly/monthly backfill one occurrence per controller cycle

## How It Works

### Controller DAG

Located in `airflow/dags/s3_to_mongo_controller.py`.

```python
with DAG(
    dag_id="s3_to_mongo_controller",
    schedule="0 * * * *",  # Every hour
    catchup=False,
    max_active_runs=1,
) as dag:

    @task
    def find_due_integrations() -> list[dict]:
        from operators.dispatch_operators import find_and_prepare_due_integrations
        return find_and_prepare_due_integrations(integration_type="s3_to_mongo")

    due = find_due_integrations()

    TriggerDagRunOperator.partial(
        task_id="trigger_ondemand",
        trigger_dag_id="s3_to_mongo_ondemand",
        wait_for_completion=False,
        reset_dag_run=False,
    ).expand_kwargs(due)
```

### find_and_prepare_due_integrations()

Located in `airflow/plugins/operators/dispatch_operators.py`.

**Query** — single unified query for all schedule types:
```sql
SELECT * FROM integrations
WHERE usr_sch_status = 'active'
  AND integration_type = :type
  AND schedule_type IN ('daily', 'weekly', 'monthly')
  AND utc_next_run IS NOT NULL
  AND utc_next_run <= :now
```

**Per integration:**
1. **Build conf** — merges integration fields, `json_data`, and auth credentials (same pattern as control plane API and Kafka consumer)
2. **Generate trigger_run_id** — format: `{tenant_id}_s3_to_mongo_ondemand_scheduled_{timestamp}`
3. **Advance utc_next_run** — schedule-type-aware backfill policy (see below)
4. **Error isolation** — if one integration fails, log and skip; others still dispatched

### Backfill Policy

`_advance_next_run` uses different base times depending on schedule type:

| Schedule Type | Base for croniter | Behavior After Downtime |
|---|---|---|
| **daily** | `now` | Skips missed runs — jumps to next future occurrence. Daily jobs are high-frequency; replaying a week of missed dailies would flood the system with stale data. |
| **weekly** | Current `utc_next_run` | Backfills one occurrence per controller cycle. If 3 weeks were missed, 3 runs are dispatched across 3 consecutive hourly cycles. |
| **monthly** | Current `utc_next_run` | Same as weekly — backfills one month at a time until caught up. |

**Example**: System was down for 3 weeks. A weekly integration had `utc_next_run = March 1`.

| Controller Cycle | `utc_next_run` Before | Action | `utc_next_run` After |
|---|---|---|---|
| Cycle 1 | March 1 (past) | Dispatch run, advance from March 1 | March 8 |
| Cycle 2 | March 8 (past) | Dispatch run, advance from March 8 | March 15 |
| Cycle 3 | March 15 (past) | Dispatch run, advance from March 15 | March 22 |
| Cycle 4 | March 22 (future) | Not due — skipped | March 22 |

**Returns** `list[dict]` where each dict has `{"conf": {...}, "trigger_run_id": "..."}` — suitable for `TriggerDagRunOperator.expand_kwargs()`.

## Environment Variables

The controller runs **inside Airflow workers** and needs to query the control plane MySQL database:

| Variable | Purpose | Default |
|---|---|---|
| `CONTROL_PLANE_DB_URL` | MySQL connection for querying integrations and auth records | — |

Note: `AIRFLOW_INTERNAL_API_URL` / `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` are **not needed** for the controller — it uses native `TriggerDagRunOperator` instead of REST API. These variables are still needed by the control plane service and Kafka consumer for external triggering.

## Data Flow Comparison

All three triggering paths target the same `s3_to_mongo_ondemand` DAG and build identical conf dicts. The `dag_run_id` encodes the origin:

| Trigger Source | When | Sample `dag_run_id` |
|---|---|---|
| Control plane API | On-demand / manual | `ws-abc123_s3_to_mongo_ondemand_manual_20260310_183116_3` |
| Kafka consumer | CDC event | `ws-abc123_s3_to_mongo_ondemand_cdc_20260310_183335_2` |
| **Controller DAG** | Hourly cron | `ws-abc123_s3_to_mongo_ondemand_scheduled_20260310_204234_0` |

## Testing

### Unit Tests

```bash
pytest airflow/tests/test_s3_to_mongo_operators.py -v -k "FindAndPrepare"
```

Covers: correct confs, empty results, error isolation, utc_next_run advancement, missing DB URL, trigger_run_id format.

### E2E Test

```bash
pytest control_plane/tests/test_cron_scheduled_e2e.py -v -s
```

Full pipeline validation:
1. Seeds MySQL with test customer, workspace, auth records, and integration (with `utc_next_run` in the past)
2. Uploads test data to MinIO
3. Deploys a test controller DAG (`schedule="* * * * *"`)
4. Waits for Airflow scheduler to trigger it
5. Waits for dispatched ondemand DAG to complete
6. Verifies data in MongoDB
7. Verifies IntegrationRun record in MySQL
8. Cleans up all test state
