from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pendulum
import pytest
from airflow.models import DagBag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk.definitions.context import Context
from gba.settings.build_aggregates import get_build_aggregates_settings
from gba.settings.build_candidates import get_build_candidates_settings
from gba.settings.parse_flatten import get_parse_flatten_settings
from gba.settings.get_archive import get_download_archive_settings

DAG_ID = "github_batch_analysis"
DAG_FILE = Path(__file__).resolve().parents[2] / "dags" / "github_analysis.py"
TEST_ENV = {
    "S3_LANDING_ZONE_BUCKET_NAME": "test-landing",
    "S3_BRONZE_ZONE_BUCKET_NAME": "test-bronze",
    "S3_SILVER_ZONE_BUCKET_NAME": "test-silver",
    "SPARK_MASTER_URL": "spark://spark-master:7077",
    "AWS_PROFILE": "gba-admin",
    "AWS_CONFIG_FILE": "/tmp/aws/config",
    "AWS_SHARED_CREDENTIALS_FILE": "/tmp/aws/credentials",
    "CANDIDATES_SIZE": "25",
}


@pytest.fixture()
def dagbag() -> DagBag:
    with patch.dict("os.environ", TEST_ENV, clear=False):
        get_download_archive_settings.cache_clear()
        get_parse_flatten_settings.cache_clear()
        get_build_aggregates_settings.cache_clear()
        get_build_candidates_settings.cache_clear()
        return DagBag(dag_folder=str(DAG_FILE), include_examples=False)


@pytest.fixture()
def dag(dagbag: DagBag):
    return dagbag.dags[DAG_ID]


@pytest.mark.integration
class TestGithubAnalysisDag:
    def test_dag_loads_without_import_errors(self, dagbag: DagBag):
        assert dagbag.import_errors == {}
        assert DAG_ID in dagbag.dags

    def test_dag_basic_metadata(self, dag):
        assert dag is not None
        assert dag.dag_id == DAG_ID
        assert dag.schedule is None
        assert dag.catchup is False
        assert set(dag.tags) == {"github", "batch", "spark"}

    def test_expected_tasks_exist(self, dag):
        expected = {
            "_get_github_events_archive",
            "parse_flatten_events",
            "build_repo_aggregates",
            "build_org_aggregates",
            "build_repo_candidates",
            "build_org_candidates",
            "enrich_repo_candidates",
            "enrich_org_candidates",
        }
        assert set(dag.task_dict) == expected

    def test_task_types(self, dag):
        assert isinstance(dag.task_dict["parse_flatten_events"], SparkSubmitOperator)
        assert isinstance(dag.task_dict["build_repo_aggregates"], SparkSubmitOperator)
        assert isinstance(dag.task_dict["build_org_aggregates"], SparkSubmitOperator)

    def test_dependencies(self, dag):
        download = dag.task_dict["_get_github_events_archive"]
        parse_flatten = dag.task_dict["parse_flatten_events"]
        repo = dag.task_dict["build_repo_aggregates"]
        org = dag.task_dict["build_org_aggregates"]
        repo_candidates = dag.task_dict["build_repo_candidates"]
        org_candidates = dag.task_dict["build_org_candidates"]
        enrich_repo = dag.task_dict["enrich_repo_candidates"]
        enrich_org = dag.task_dict["enrich_org_candidates"]

        assert download.downstream_task_ids == {"parse_flatten_events"}
        assert parse_flatten.upstream_task_ids == {"_get_github_events_archive"}
        assert parse_flatten.downstream_task_ids == {
            "build_repo_aggregates",
            "build_org_aggregates",
        }
        assert repo.upstream_task_ids == {"parse_flatten_events"}
        assert repo.downstream_task_ids == {"build_repo_candidates"}
        assert org.upstream_task_ids == {"parse_flatten_events"}
        assert org.downstream_task_ids == {"build_org_candidates"}
        assert repo_candidates.upstream_task_ids == {"build_repo_aggregates"}
        assert repo_candidates.downstream_task_ids == {"enrich_repo_candidates"}
        assert org_candidates.upstream_task_ids == {"build_org_aggregates"}
        assert org_candidates.downstream_task_ids == {"enrich_org_candidates"}
        assert enrich_repo.upstream_task_ids == {"build_repo_candidates"}
        assert enrich_org.upstream_task_ids == {"build_org_candidates"}

    def test_parse_flatten_task_configuration(self, dag):
        task = dag.task_dict["parse_flatten_events"]

        assert task.application == "/opt/airflow/dags/gba/services/parse_flatten.py"
        assert task._conn_id == "spark_default"
        assert task.conf is not None
        assert task.conf["spark.executorEnv.PYTHONPATH"] == "/opt/airflow/dags"

    def test_build_aggregates_task_configuration(self, dag):
        repo_task = dag.task_dict["build_repo_aggregates"]
        org_task = dag.task_dict["build_org_aggregates"]

        assert (
            repo_task.application
            == "/opt/airflow/dags/gba/services/build_aggregates.py"
        )
        assert (
            org_task.application == "/opt/airflow/dags/gba/services/build_aggregates.py"
        )
        assert repo_task.application_args is not None
        assert org_task.application_args is not None

        assert repo_task.application_args[-5:] == [
            "repo",
            "--dt",
            "{{ ds }}",
            "--hr",
            "{{ logical_date.hour }}",
        ]
        assert org_task.application_args[-5:] == [
            "org",
            "--dt",
            "{{ ds }}",
            "--hr",
            "{{ logical_date.hour }}",
        ]

    def test_rendered_candidate_paths(self, dag):
        task = dag.task_dict["build_repo_aggregates"]
        context = Context(
            {
                "ds": "2026-03-20",
                "logical_date": pendulum.datetime(2026, 3, 20, 21, tz="UTC"),
            }
        )

        task.render_template_fields(context)

        assert task.application_args is not None
        assert (
            task.application_args[1]
            == "s3a://test-bronze/gh_events_flat/dt=2026-03-20/hr=21/"
        )
        assert (
            task.application_args[3]
            == "s3a://test-silver/repo_aggregates/dt=2026-03-20/hr=21/"
        )
        assert task.application_args[5] == "repo"

    def test_dag_has_no_cycles(self, dag):
        dag.check_cycle()

    def test_no_import_errors(self):
        with patch.dict("os.environ", TEST_ENV, clear=False):
            get_download_archive_settings.cache_clear()
            get_parse_flatten_settings.cache_clear()
            get_build_aggregates_settings.cache_clear()
            get_build_candidates_settings.cache_clear()
            dagbag = DagBag(dag_folder="dags", include_examples=False)
        assert dagbag.import_errors == {}
