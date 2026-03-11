"""Shared error tracking utilities for Airflow pipeline tasks.

Provides a consistent pattern for pipeline tasks (Prepare, Validate, Execute)
to push structured error records to XCom, and for CleanUp to pull and persist
them to the integration_run_errors table.

XCom space is limited, so messages are truncated and error counts are capped.
"""

from typing import Any, Dict, List

# XCom key prefix — each task stores errors under "{PREFIX}_{task_id}"
TASK_ERRORS_XCOM_KEY = "task_errors"

# Limits to avoid XCom bloat
MAX_ERROR_MESSAGE_LENGTH = 2000
MAX_ERRORS_PER_TASK = 20


def push_task_errors(
    ti: Any,
    task_id: str,
    errors: List[Dict[str, str]],
    log: Any = None,
) -> None:
    """Push error records to XCom for CleanUpTask to collect.

    Each error dict should have:
        - error_code: str (e.g., "VALIDATION_ERROR", "DATA_ERROR")
        - message: str (human-readable description)

    Args:
        ti: Airflow TaskInstance (for xcom_push).
        task_id: The originating task's ID (e.g., "prepare", "validate").
        errors: List of error dicts with 'error_code' and 'message'.
        log: Optional logger for info messages.
    """
    if not errors:
        return

    truncated = []
    for err in errors[:MAX_ERRORS_PER_TASK]:
        truncated.append({
            "task_id": task_id,
            "error_code": str(err.get("error_code", "UNKNOWN"))[:100],
            "message": str(err.get("message", ""))[:MAX_ERROR_MESSAGE_LENGTH],
        })

    xcom_key = f"{TASK_ERRORS_XCOM_KEY}_{task_id}"
    ti.xcom_push(key=xcom_key, value=truncated)

    if log:
        log.info(f"Pushed {len(truncated)} error(s) to XCom key '{xcom_key}'")


def pull_all_task_errors(
    ti: Any,
    upstream_task_ids: List[str],
) -> List[Dict[str, str]]:
    """Pull all error records pushed by upstream tasks.

    Args:
        ti: Airflow TaskInstance (for xcom_pull).
        upstream_task_ids: Task IDs to collect errors from.

    Returns:
        Combined list of error dicts from all upstream tasks.
    """
    all_errors: List[Dict[str, str]] = []
    for task_id in upstream_task_ids:
        xcom_key = f"{TASK_ERRORS_XCOM_KEY}_{task_id}"
        errors = ti.xcom_pull(key=xcom_key)
        if errors and isinstance(errors, list):
            all_errors.extend(errors)
    return all_errors
