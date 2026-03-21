from __future__ import annotations

from datetime import datetime

from airflow import DAG

from gba.tasks.get_archive import get_github_events_archive
from gba.tasks.parse_flatten_events import get_parse_flatten_events_task
from gba.tasks.build_candidates import build_repo_candidates, build_org_candidates

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

    repo_candidates = build_repo_candidates(
        input_path=parse_flatten_step.output_path,
        dt="{{ ds }}",
        hour="{{ logical_date.hour }}",
    )
    org_candidates = build_org_candidates(
        input_path=parse_flatten_step.output_path,
        dt="{{ ds }}",
        hour="{{ logical_date.hour }}",
    )
    repo_task = repo_candidates.task
    org_task = org_candidates.task

    download_step >> parse_flatten_task
    parse_flatten_task >> [repo_task, org_task]
