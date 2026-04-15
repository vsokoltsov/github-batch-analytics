from __future__ import annotations

from typing import NamedTuple

from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup

from gba.services.materialize_bucketed_marts import (
    BucketedMartConfig,
    build_drop_table_query,
    submit_bucketed_mart_ctas,
)
from gba.settings.materialize_bucketed_marts import (
    get_materialize_bucketed_marts_settings,
)


class MaterializeBucketedMartEvent(NamedTuple):
    task_group: TaskGroup
    output_path: str


def _materialize_bucketed_marts(
    *,
    group_id: str,
    submit_display_name: str,
    wait_display_name: str,
    drop_display_name: str,
    config: BucketedMartConfig,
    aws_region: str,
    workgroup_name: str,
) -> MaterializeBucketedMartEvent:
    with TaskGroup(group_id=group_id) as task_group:
        submit_task = PythonOperator(
            task_id="submit_ctas",
            task_display_name=submit_display_name,
            python_callable=submit_bucketed_mart_ctas,
            op_kwargs={
                "aws_region": aws_region,
                "workgroup_name": workgroup_name,
                "config": {
                    "database_name": config.database_name,
                    "source_table_name": config.source_table_name,
                    "target_table_name": config.target_table_name,
                    "query_results_bucket_name": config.query_results_bucket_name,
                    "marts_bucket_name": config.marts_bucket_name,
                    "dt": config.dt,
                    "hr": config.hr,
                    "bucket_column": config.bucket_column,
                    "bucket_count": config.bucket_count,
                },
            },
        )

        wait_task = AthenaSensor(
            task_id="wait_for_ctas",
            task_display_name=wait_display_name,
            query_execution_id=(
                "{{ ti.xcom_pull(task_ids='"
                f"{group_id}.submit_ctas"
                "')['query_execution_id'] }}"
            ),
            sleep_time=15,
        )

        drop_task = AthenaOperator(
            task_id="drop_temp_table",
            task_display_name=drop_display_name,
            query=build_drop_table_query(
                config.database_name,
                "{{ ti.xcom_pull(task_ids='"
                f"{group_id}.submit_ctas"
                "')['temp_table_name'] }}",
            ),
            database=config.database_name,
            output_location=config.athena_results_location,
            workgroup=workgroup_name,
            sleep_time=15,
        )

        submit_task >> wait_task >> drop_task

    return MaterializeBucketedMartEvent(
        task_group=task_group, output_path=config.output_path
    )


def materialize_repo_bucketed_marts(dt: str, hour: str) -> MaterializeBucketedMartEvent:
    settings = get_materialize_bucketed_marts_settings()
    return _materialize_bucketed_marts(
        group_id="materialize_repo_bucketed_marts",
        submit_display_name="Submit repository bucketed marts CTAS",
        wait_display_name="Wait for repository bucketed marts CTAS",
        drop_display_name="Drop repository bucketed marts temp table",
        aws_region=settings.AWS_REGION,
        workgroup_name=settings.ATHENA_WORKGROUP_NAME,
        config=BucketedMartConfig(
            database_name=settings.ATHENA_DATABASE_NAME,
            source_table_name=settings.ATHENA_REPOSITORY_TABLE_NAME,
            target_table_name=settings.ATHENA_REPOSITORY_TABLE_NAME,
            query_results_bucket_name=settings.ATHENA_QUERY_RESULTS_BUCKET_NAME,
            marts_bucket_name=settings.S3_MARTS_BUCKET_NAME,
            dt=dt,
            hr=hour,
            bucket_column="repo_id",
            bucket_count=settings.ATHENA_REPOSITORY_BUCKET_COUNT,
        ),
    )


def materialize_org_bucketed_marts(dt: str, hour: str) -> MaterializeBucketedMartEvent:
    settings = get_materialize_bucketed_marts_settings()
    return _materialize_bucketed_marts(
        group_id="materialize_org_bucketed_marts",
        submit_display_name="Submit organization bucketed marts CTAS",
        wait_display_name="Wait for organization bucketed marts CTAS",
        drop_display_name="Drop organization bucketed marts temp table",
        aws_region=settings.AWS_REGION,
        workgroup_name=settings.ATHENA_WORKGROUP_NAME,
        config=BucketedMartConfig(
            database_name=settings.ATHENA_DATABASE_NAME,
            source_table_name=settings.ATHENA_ORGANIZATION_TABLE_NAME,
            target_table_name=settings.ATHENA_ORGANIZATION_TABLE_NAME,
            query_results_bucket_name=settings.ATHENA_QUERY_RESULTS_BUCKET_NAME,
            marts_bucket_name=settings.S3_MARTS_BUCKET_NAME,
            dt=dt,
            hr=hour,
            bucket_column="org_id",
            bucket_count=settings.ATHENA_ORGANIZATION_BUCKET_COUNT,
        ),
    )
