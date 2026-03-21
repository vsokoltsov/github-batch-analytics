from __future__ import annotations

from typing import cast
from unittest.mock import Mock

import pytest

from gba.tasks.parse_flatten_events import (
    get_parse_flatten_events_task,
    ParseFLattenEvents,
)


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
            "s3://landing/raw/events.json.gz",
            dt="{{ ds }}",
            hour="{{ logical_date.hour }}",
        )

        assert isinstance(task, ParseFLattenEvents)
        assert task.task.task_id == "parse_flatten_events"
        assert task.task._conn_id == "spark_default"
        assert (
            task.task.application == "/opt/airflow/dags/gba/services/parse_flatten.py"
        )

        app_args = task.task.application_args
        assert app_args is not None
        app_args = cast(list[str], app_args)
        assert app_args[0] == "--input-path"
        assert app_args[1] == "s3://landing/raw/events.json.gz"
        assert app_args[2] == "--output-path"
        assert "s3a://gba-bronze-zone-test/gh_events_flat" in app_args[3]

        conf = task.task.conf
        assert conf is not None
        conf = cast(dict[str, str], conf)
        assert conf["spark.master"] == "spark://spark-master:7077"
        assert conf["spark.executorEnv.PYTHONPATH"] == "/opt/airflow/dags"
