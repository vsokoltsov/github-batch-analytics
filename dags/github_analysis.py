from __future__ import annotations

from datetime import datetime

from airflow import DAG

from gba.tasks.get_archive import get_github_events_archive
from gba.tasks.parse_flatten_events import get_parse_flatten_events_task

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
        input_path=download_step,
        dt="{{ ds }}",
        hour="{{ logical_date.hour }}"
    )

    download_step >> parse_flatten_step
