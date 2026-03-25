"""CDC Event Listener — Asset + AssetWatcher definition.

Defines an Asset backed by a KafkaMessageQueueTrigger that continuously
polls the ``cdc.integration.events`` Kafka topic from the Airflow
triggerer process.  When the apply_function returns a non-None value
(i.e., an ``integration.created`` event), the trigger fires an
AssetEvent whose payload flows to any DAG scheduled on this Asset.

This file is a pure definition — it contains no tasks.
The processing logic lives in ``cdc_integration_processor.py``.
"""

from airflow.sdk import Asset, AssetWatcher
from airflow.providers.apache.kafka.triggers.msg_queue import KafkaMessageQueueTrigger

integration_cdc_asset = Asset(
    name="integration_cdc_events",
    uri="kafka://kafka/cdc.integration.events",
    watchers=[
        AssetWatcher(
            name="cdc_integration_watcher",
            trigger=KafkaMessageQueueTrigger(
                topics=["cdc.integration.events"],
                kafka_config_id="kafka_default",
                apply_function="callbacks.cdc_apply_function.cdc_apply_function",
                poll_timeout=1,
                poll_interval=1,
            ),
        )
    ],
)
