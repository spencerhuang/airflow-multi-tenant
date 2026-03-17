"""Base abstract operators for workflow tasks."""

from abc import ABC, abstractmethod
from airflow.sdk import BaseOperator
from typing import Dict, Any


class TraceIdMixin:
    """Mixin providing W3C TraceContext extraction for distributed tracing.

    Parses the traceparent from dag_run conf (set by Kafka consumer or dispatcher)
    or from XCom (pushed by PrepareTask). All task operators should inherit
    this mixin and call _get_trace_context(context) at the start of execute().
    """

    def _get_trace_context(self, context: Dict[str, Any]):
        """Extract TraceContext from dag_run conf or XCom.

        Looks for 'traceparent' (W3C format) in dag_run.conf first,
        then falls back to XCom. Generates a new context if neither exists
        (e.g. manual Airflow UI triggers).

        Args:
            context: Airflow task context

        Returns:
            TraceContext instance (always returns a valid context)
        """
        from shared_utils import TraceContext

        # Try dag_run.conf first (set by Kafka consumer or dispatcher)
        dag_run = context.get("dag_run")
        dag_run_conf = (dag_run.conf if dag_run else None) or {}
        traceparent = dag_run_conf.get("traceparent", "")

        # Fallback to XCom (pushed by PrepareTask)
        if not traceparent:
            ti = context.get("ti")
            if ti:
                traceparent = ti.xcom_pull(task_ids="prepare", key="traceparent") or ""

        # Parse or generate new
        if traceparent:
            try:
                return TraceContext.from_traceparent(traceparent)
            except ValueError:
                pass

        return TraceContext.new()


class PrepareTask(TraceIdMixin, BaseOperator, ABC):
    """
    Abstract base operator for prepare tasks.

    Prepare tasks handle:
    - Resolving credentials and authentication
    - Validating source/destination configuration
    - Setting up execution context

    Subclasses must implement execute() method with workflow-specific logic.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute prepare logic.

        Args:
            context: Airflow task context

        Returns:
            Dictionary of prepared configuration for downstream tasks

        Raises:
            Exception: If preparation fails
        """
        pass


class ValidateTask(TraceIdMixin, BaseOperator, ABC):
    """
    Abstract base operator for validate tasks.

    Validate tasks handle:
    - Checking source data availability
    - Validating data schemas
    - Verifying destination connectivity

    Subclasses must implement execute() method with workflow-specific logic.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> bool:
        """
        Execute validation logic.

        Args:
            context: Airflow task context

        Returns:
            True if validation passes

        Raises:
            Exception: If validation fails
        """
        pass


class CleanUpTask(TraceIdMixin, BaseOperator, ABC):
    """
    Abstract base operator for cleanup tasks.

    Cleanup tasks handle:
    - Removing temporary files
    - Closing connections
    - Updating metadata

    Subclasses must implement execute() method with workflow-specific logic.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> None:
        """
        Execute cleanup logic.

        Args:
            context: Airflow task context

        Raises:
            Exception: If cleanup fails
        """
        pass
