from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import TaskGroup

from gba.tasks.get_archive import get_github_events_archive
from gba.tasks.build_aggregates import build_org_aggregates, build_repo_aggregates
from gba.tasks.parse_flatten_events import get_parse_flatten_events_task
from gba.tasks.build_candidates import build_repo_candidates, build_org_candidates
from gba.tasks.enrich_candidates import (
    get_enrich_repo_candidates_task,
    get_enrich_org_candidates_task,
)
from gba.tasks.build_marts import build_repo_marts, build_org_marts
from gba.tasks.build_dashboard_views import (
    repository_summary_dashboard,
    repository_language_dashboard,
    org_summary_dashboard,
    org_top_100_dashboard,
    common_rollup_dashboard,
    common_language_location_dashboard,
)

with DAG(
    dag_id="github_batch_analysis",
    start_date=datetime(2026, 1, 1),
    schedule="5 * * * *",
    catchup=False,
    tags={"github", "batch", "spark"},
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
    }
) as dag:
    download_step = get_github_events_archive(
        landing_date="{{ ds }}",
        archive_url="https://data.gharchive.org/{{ ds }}-{{ logical_date.hour }}.json.gz",
    )

    parse_flatten_step = get_parse_flatten_events_task(
        input_path=download_step, dt="{{ ds }}", hour="{{ logical_date.hour }}"
    )
    parse_flatten_task = parse_flatten_step.task

    repo_aggregates = build_repo_aggregates(
        input_path=parse_flatten_step.output_path,
        dt="{{ ds }}",
        hour="{{ logical_date.hour }}",
    )
    org_aggregates = build_org_aggregates(
        input_path=parse_flatten_step.output_path,
        dt="{{ ds }}",
        hour="{{ logical_date.hour }}",
    )
    repo_aggregate_task = repo_aggregates.task
    org_aggregate_task = org_aggregates.task

    repo_candidates = build_repo_candidates(
        input_path=repo_aggregates.output_path,
        dt="{{ ds }}",
        hour="{{ logical_date.hour }}",
    )
    org_candidates = build_org_candidates(
        input_path=org_aggregates.output_path,
        dt="{{ ds }}",
        hour="{{ logical_date.hour }}",
    )
    repo_candidates_task = repo_candidates.task
    org_candidates_task = org_candidates.task

    enrich_repo_candidates = get_enrich_repo_candidates_task(
        repo_candidates.output_path,
        dt="{{ ds }}",
        hour="{{ logical_date.hour }}",
    )
    enrich_org_candidates = get_enrich_org_candidates_task(
        org_candidates.output_path,
        dt="{{ ds }}",
        hour="{{ logical_date.hour }}",
    )

    build_repository_marts = build_repo_marts(
        candidates_path=repo_candidates.output_path,
        github_snapshot_path=enrich_repo_candidates.output["repo_snapshot_path"],
        dt="{{ ds }}",
        hour="{{ logical_date.hour }}",
    )
    build_organization_marts = build_org_marts(
        candidates_path=org_candidates.output_path,
        github_snapshot_path=enrich_org_candidates.output["org_snapshot_path"],
        dt="{{ ds }}",
        hour="{{ logical_date.hour }}",
    )
    build_repository_marts_task = build_repository_marts.task
    build_organization_marts_task = build_organization_marts.task

    with TaskGroup(group_id="repository_dashboards") as repository_dashboards:
        repo_summary = repository_summary_dashboard(
            input_path=build_repository_marts.output_path,
            dt="{{ ds }}",
            hr="{{ logical_date.hour }}",
        )
        repo_language = repository_language_dashboard(
            input_path=build_repository_marts.output_path,
            dt="{{ ds }}",
            hr="{{ logical_date.hour }}",
        )

    with TaskGroup(group_id="organization_dashboards") as organization_dashboards:
        org_summary = org_summary_dashboard(
            input_path=build_organization_marts.output_path,
            dt="{{ ds }}",
            hr="{{ logical_date.hour }}",
        )
        org_top_100 = org_top_100_dashboard(
            input_path=build_organization_marts.output_path,
            dt="{{ ds }}",
            hr="{{ logical_date.hour }}",
        )

    with TaskGroup(group_id="common_dashboards") as common_dashboards:
        common_rollup = common_rollup_dashboard(
            repo_path=build_repository_marts.output_path,
            org_path=build_organization_marts.output_path,
            dt="{{ ds }}",
            hr="{{ logical_date.hour }}",
        )
        common_language_location = common_language_location_dashboard(
            repo_path=build_repository_marts.output_path,
            org_path=build_organization_marts.output_path,
            dt="{{ ds }}",
            hr="{{ logical_date.hour }}",
        )

    download_step >> parse_flatten_task

    parse_flatten_task >> [repo_aggregate_task, org_aggregate_task]

    repo_aggregate_task >> repo_candidates_task
    org_aggregate_task >> org_candidates_task

    repo_candidates_task >> enrich_repo_candidates
    org_candidates_task >> enrich_org_candidates

    enrich_repo_candidates >> build_repository_marts_task
    enrich_org_candidates >> build_organization_marts_task

    build_repository_marts_task >> repository_dashboards
    build_organization_marts_task >> organization_dashboards
    [build_repository_marts_task, build_organization_marts_task] >> common_dashboards
