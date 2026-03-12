from __future__ import annotations

from unittest.mock import Mock

import pytest
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from gba.tasks.parse_flatten_events import get_parse_flatten_events_task


@pytest.mark.unit
class TestParseFlattenEventsTaskUnit:
    def test_factory_builds_expected_spark_submit_operator(self, monkeypatch):
        fake_settings = Mock()
        fake_settings.S3_BRONZE_ZONE_BUCKET_NAME = "gba-bronze-zone-test"
        fake_settings.SPARK_MASTER_URL = "spark://spark-master:7077"
        fake_settings.AWS_PROFILE = "gba-admin"
        fake_settings.AWS_CONFIG_FILE = "/home/spark/.aws/config"
        fake_settings.AWS_SHARED_CREDENTIALS_FILE = "/home/spark/.aws/credentials"
        monkeypatch.setattr(
            "gba.tasks.parse_flatten_events.get_parse_flatten_settings",
            Mock(return_value=fake_settings),
        )

        task = get_parse_flatten_events_task(
            "s3://landing/raw/events.json.gz", dt="{{ ds }}", hour="{{ logical_date.hour }}"
        )

        assert isinstance(task, SparkSubmitOperator)
        assert task.task_id == "parse_flatten_events"
        assert task._conn_id == "spark_default"
        assert task.application == "/opt/airflow/dags/gba/services/parse_flatten.py"
        assert task.application_args[0] == "--input-path"
        assert task.application_args[1] == "s3://landing/raw/events.json.gz"
        assert task.application_args[2] == "--output-path"
        assert "s3a://gba-bronze-zone-test/gh_events_flat" in task.application_args[3]
        assert task.conf["spark.master"] == "spark://spark-master:7077"
        assert task.conf["spark.executorEnv.PYTHONPATH"] == "/opt/airflow/dags"
