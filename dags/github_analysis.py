from __future__ import annotations

import os
from datetime import datetime
from airflow import DAG
from airflow.sdk import task
from gba.tasks.get_archive import get_github_events_archive


with DAG(
    dag_id="github_batch_analysis",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags={"example"},
) as dag:    
    download_step = get_github_events_archive(
        landing_date="{{ ds }}",
        archive_url="https://data.gharchive.org/{{ ds }}-{{ logical_date.hour }}.json.gz",
    )