"""Shared helpers for e2e tests that use the CDC pipeline.

Called in test fixture teardown (``finally`` blocks) so cleanup runs
regardless of pass or fail. All steps are best-effort and idempotent.

Cleanup strategy:
  1. Stop the triggerer so the Kafka consumer group becomes inactive.
  2. Reset consumer group offset to latest — skips any stale CDC events
     left on the topic from this test session.
  3. Delete cdc_integration_processor DAG runs — prevents stale
     AssetEvents from queuing processor runs in the next session.
  4. Clear pending AssetEvents from Airflow's metastore — these are
     separate from Kafka offsets and will trigger processor runs if not cleared.
  5. Clear the retry state file in the triggerer container.
  6. Restart the triggerer so the watcher resumes from the new offset.
"""

import subprocess
import time

def cleanup_cdc_pipeline(
    kafka_bootstrap_servers: str = "localhost:9092",
    consumer_group: str = "cdc-consumer-airflow",
    topic: str = "cdc.integration.events",
    airflow_api_url: str = "http://localhost:8080/api/v2",
    get_airflow_headers: callable = None,
) -> None:
    """Clean up CDC pipeline state for next test run."""
    _stop_triggerer()
    _reset_consumer_group_offset(consumer_group, topic)
    _cleanup_processor_dag_runs(airflow_api_url, get_airflow_headers)
    _clear_asset_events()
    _clear_triggerer_retry_state()
    _start_triggerer()


def _stop_triggerer() -> None:
    try:
        subprocess.run(
            ["docker-compose", "stop", "airflow-triggerer"],
            capture_output=True, timeout=30,
        )
        time.sleep(3)
    except Exception as e:
        print(f"  Triggerer stop warning: {e}")


def _start_triggerer() -> None:
    try:
        subprocess.run(
            ["docker-compose", "start", "airflow-triggerer"],
            capture_output=True, timeout=30,
        )
        print("✓ Restarted airflow-triggerer")
    except Exception as e:
        print(f"  Triggerer start warning: {e}")


def _reset_consumer_group_offset(consumer_group: str, topic: str) -> None:
    """Reset consumer group offset to latest so stale events are skipped."""
    try:
        result = subprocess.run(
            [
                "docker", "exec", "kafka",
                "kafka-consumer-groups",
                "--bootstrap-server", "localhost:9092",
                "--group", consumer_group,
                "--topic", topic,
                "--reset-offsets", "--to-latest", "--execute",
            ],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            print(f"✓ Reset {consumer_group} offset to latest")
        else:
            print(f"  Consumer group offset reset skipped: {result.stderr.strip()}")
    except Exception as e:
        print(f"  Offset reset warning: {e}")


def _cleanup_processor_dag_runs(airflow_api_url: str, get_headers: callable) -> None:
    """Delete all cdc_integration_processor DAG runs."""
    try:
        import requests

        if get_headers is None:
            return

        headers = get_headers()
        dag_id = "cdc_integration_processor"
        r = requests.get(
            f"{airflow_api_url}/dags/{dag_id}/dagRuns",
            headers=headers, timeout=10,
            params={"limit": 100},
        )
        if r.status_code == 200:
            deleted = 0
            for run in r.json().get("dag_runs", []):
                resp = requests.delete(
                    f"{airflow_api_url}/dags/{dag_id}/dagRuns/{run['dag_run_id']}",
                    headers=headers, timeout=10,
                )
                if resp.status_code in (200, 204):
                    deleted += 1
            if deleted:
                print(f"✓ Deleted {deleted} cdc_integration_processor DAG run(s)")
    except Exception as e:
        print(f"  Processor DAG run cleanup warning: {e}")


def _clear_asset_events(metadb_conn: str = "") -> None:
    """Clear pending AssetEvents from Airflow's metastore.

    AssetEvents live in Airflow's postgres database, separate from Kafka.
    Resetting Kafka consumer offsets does NOT clear these — they will still
    trigger cdc_integration_processor DAG runs on the next session.

    Uses docker exec to run psql directly in the postgres container,
    avoiding the need for psycopg2 in the test environment.
    """
    try:
        sql = (
            "DELETE FROM asset_dag_run_queue; "
            "DELETE FROM dagrun_asset_event; "
            "DELETE FROM asset_event;"
        )
        result = subprocess.run(
            ["docker", "exec", "airflow-postgres",
             "psql", "-U", "airflow", "-c", sql],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            # Parse DELETE count from psql output (e.g. "DELETE 6")
            lines = [l.strip() for l in result.stdout.strip().split("\n") if l.startswith("DELETE")]
            total = sum(int(l.split()[-1]) for l in lines if len(l.split()) == 2)
            if total:
                print(f"✓ Cleared {total} stale AssetEvent(s) from Airflow metastore")
        else:
            print(f"  AssetEvent cleanup warning: {result.stderr.strip()}")
    except Exception as e:
        print(f"  AssetEvent cleanup warning: {e}")


def _clear_triggerer_retry_state() -> None:
    """Clear the retry state file in the triggerer container."""
    try:
        result = subprocess.run(
            ["docker", "exec", "airflow-triggerer",
             "rm", "-f", "/tmp/cdc_apply_retries.json"],
            capture_output=True, timeout=10,
        )
        if result.returncode == 0:
            print("✓ Cleared triggerer retry state")
    except Exception as e:
        print(f"  Retry state cleanup warning: {e}")
