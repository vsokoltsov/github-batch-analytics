from __future__ import annotations

from airflow.providers.standard.operators.python import PythonOperator

from gba.services.enrichment.organization import run_org_enrichment_pipeline
from gba.services.enrichment.repository import run_repo_enrichment_pipeline
from gba.settings.enrich_candidates import get_enrich_candidates_settings


def get_enrich_repo_candidates_task(
    input_path: str, dt: str, hour: str
) -> PythonOperator:
    settings = get_enrich_candidates_settings()
    return PythonOperator(
        task_id="enrich_repo_candidates",
        task_display_name="Enrich repository candidates",
        python_callable=run_repo_enrichment_pipeline,
        op_kwargs={
            "output_bucket": settings.S3_BRONZE_ZONE_BUCKET_NAME,
            "input_path": input_path,
            "dt": dt,
            "hr": hour,
        },
    )


def get_enrich_org_candidates_task(
    input_path: str,
    dt: str,
    hour: str,
) -> PythonOperator:
    settings = get_enrich_candidates_settings()
    return PythonOperator(
        task_id="enrich_org_candidates",
        task_display_name="Enrich organization candidates",
        python_callable=run_org_enrichment_pipeline,
        op_kwargs={
            "output_bucket": settings.S3_BRONZE_ZONE_BUCKET_NAME,
            "input_path": input_path,
            "dt": dt,
            "hr": hour,
        },
    )
