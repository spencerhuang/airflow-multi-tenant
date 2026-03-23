"""Airflow plugin that registers the audit event listener."""

from airflow.plugins_manager import AirflowPlugin
from listeners import audit_listener


class AuditListenerPlugin(AirflowPlugin):
    name = "audit_listener"
    listeners = [audit_listener]
