from __future__ import annotations

from datetime import datetime
from typing import cast

import pendulum
import pytest
from airflow import DAG
from airflow.sdk.definitions.context import Context

from gba.settings.build_candidates import get_build_candidates_settings
from gba.tasks.build_candidates import build_org_candidates, build_repo_candidates


@pytest.mark.integration
class TestBuildCandidatesTaskIntegration:
    def test_repo_task_attaches_to_dag_and_renders_templates(self, monkeypatch):
        monkeypatch.setenv("S3_SILVER_ZONE_BUCKET_NAME", "gba-silver-zone-test")
        monkeypatch.setenv("AWS_PROFILE", "gba-admin")
        monkeypatch.setenv("AWS_CONFIG_FILE", "/home/spark/.aws/config")
        monkeypatch.setenv(
            "AWS_SHARED_CREDENTIALS_FILE", "/home/spark/.aws/credentials"
        )
        monkeypatch.setenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        monkeypatch.setenv("CANDIDATES_SIZE", "25")
        get_build_candidates_settings.cache_clear()

        with DAG(
            dag_id="test_build_repo_candidates_task",
            start_date=datetime(2026, 1, 1),
            schedule=None,
            catchup=False,
        ):
            event = build_repo_candidates(
                "s3a://gba-silver-zone-test/repo_aggregates/dt={{ ds }}/hr={{ logical_date.hour }}/",
                dt="{{ ds }}",
                hour="{{ logical_date.hour }}",
            )
            task = event.task

        context = cast(
            Context,
            {
                "ds": "2026-03-08",
                "logical_date": pendulum.datetime(2026, 3, 8, 20, tz="UTC"),
            },
        )
        task.render_template_fields(context=context)

        assert task.dag is not None
        assert task.task_id in task.dag.task_dict
        app_args = cast(list[str], task.application_args)
        assert (
            app_args[1]
            == "s3a://gba-silver-zone-test/repo_aggregates/dt=2026-03-08/hr=20/"
        )
        assert (
            app_args[3]
            == "s3a://gba-silver-zone-test/repo_candidates/dt=2026-03-08/hr=20/"
        )
        assert app_args[5] == "repo"
        assert app_args[7] == 25

    def test_org_task_attaches_to_dag_and_renders_templates(self, monkeypatch):
        monkeypatch.setenv("S3_SILVER_ZONE_BUCKET_NAME", "gba-silver-zone-test")
        monkeypatch.setenv("AWS_PROFILE", "gba-admin")
        monkeypatch.setenv("AWS_CONFIG_FILE", "/home/spark/.aws/config")
        monkeypatch.setenv(
            "AWS_SHARED_CREDENTIALS_FILE", "/home/spark/.aws/credentials"
        )
        monkeypatch.setenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        monkeypatch.setenv("CANDIDATES_SIZE", "25")
        get_build_candidates_settings.cache_clear()

        with DAG(
            dag_id="test_build_org_candidates_task",
            start_date=datetime(2026, 1, 1),
            schedule=None,
            catchup=False,
        ):
            event = build_org_candidates(
                "s3a://gba-silver-zone-test/org_aggregates/dt={{ ds }}/hr={{ logical_date.hour }}/",
                dt="{{ ds }}",
                hour="{{ logical_date.hour }}",
            )
            task = event.task

        context = cast(
            Context,
            {
                "ds": "2026-03-08",
                "logical_date": pendulum.datetime(2026, 3, 8, 20, tz="UTC"),
            },
        )
        task.render_template_fields(context=context)

        assert task.dag is not None
        assert task.task_id in task.dag.task_dict
        app_args = cast(list[str], task.application_args)
        assert (
            app_args[1]
            == "s3a://gba-silver-zone-test/org_aggregates/dt=2026-03-08/hr=20/"
        )
        assert (
            app_args[3]
            == "s3a://gba-silver-zone-test/org_candidates/dt=2026-03-08/hr=20/"
        )
        assert app_args[5] == "org"
        assert app_args[7] == 25
