from __future__ import annotations

from datetime import datetime

from airflow import DAG
import pendulum
import pytest

from gba.settings.parse_flatten import get_parse_flatten_settings
from gba.tasks.parse_flatten_events import get_parse_flatten_events_task


@pytest.mark.integration
class TestParseFlattenEventsTaskIntegration:
    def test_task_attaches_to_dag_and_renders_templates(self, monkeypatch):
        monkeypatch.setenv("S3_BRONZE_ZONE_BUCKET_NAME", "gba-bronze-zone-test")
        monkeypatch.setenv("AWS_PROFILE", "gba-admin")
        monkeypatch.setenv("AWS_CONFIG_FILE", "/home/spark/.aws/config")
        monkeypatch.setenv(
            "AWS_SHARED_CREDENTIALS_FILE", "/home/spark/.aws/credentials"
        )
        monkeypatch.setenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        get_parse_flatten_settings.cache_clear()

        with DAG(
            dag_id="test_parse_flatten_events_task",
            start_date=datetime(2026, 1, 1),
            schedule=None,
            catchup=False,
        ):
            task = get_parse_flatten_events_task(
                "s3://landing/raw/events.json.gz",
                dt="{{ ds }}",
                hour="{{ logical_date.hour }}",
            )

        context = {
            "ds": "2026-03-08",
            "logical_date": pendulum.datetime(2026, 3, 8, 20, tz="UTC"),
        }
        task.render_template_fields(context=context)

        assert task.dag is not None
        assert task.task_id in task.dag.task_dict
        assert task.application_args[3].endswith("dt=2026-03-08/hr=20/")
