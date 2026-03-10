"""Base abstract operators for workflow tasks."""

from abc import ABC, abstractmethod
from airflow.sdk import BaseOperator
from typing import Dict, Any


class PrepareTask(BaseOperator, ABC):
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


class ValidateTask(BaseOperator, ABC):
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


class CleanUpTask(BaseOperator, ABC):
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
