from __future__ import annotations

from datetime import datetime

from airflow import DAG

from gba.tasks.get_archive import get_github_events_archive
from gba.tasks.build_aggregates import build_org_aggregates, build_repo_aggregates
from gba.tasks.parse_flatten_events import get_parse_flatten_events_task
from gba.tasks.build_candidates import build_repo_candidates, build_org_candidates
from gba.tasks.enrich_candidates import (
    get_enrich_repo_candidates_task,
    get_enrich_org_candidates_task,
)

with DAG(
    dag_id="github_batch_analysis",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags={"github", "batch", "spark"},
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

    download_step >> parse_flatten_task

    parse_flatten_task >> [repo_aggregate_task, org_aggregate_task]

    repo_aggregate_task >> repo_candidates_task
    org_aggregate_task >> org_candidates_task

    repo_candidates_task >> enrich_repo_candidates
    org_candidates_task >> enrich_org_candidates
    # [repo_candidate_task, org_candidates_task] >> enrich_candidates_task
