from __future__ import annotations

from typing import NamedTuple
from airflow.models.xcom_arg import XComArg
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from gba.settings.parse_flatten import get_parse_flatten_settings
from gba.tasks.spark_conf import build_spark_conf


class ParseFLattenEvents(NamedTuple):
    task: SparkSubmitOperator
    output_path: str


def get_parse_flatten_events_task(
    input_path: str | XComArg, dt: str, hour: str
) -> ParseFLattenEvents:
    settings = get_parse_flatten_settings()

    output_path = (
        f"s3a://{settings.S3_BRONZE_ZONE_BUCKET_NAME}/gh_events_flat/dt={dt}/hr={hour}/"
    )
    task = SparkSubmitOperator(
        task_display_name="Parse flatten events",
        task_id="parse_flatten_events",
        conn_id="spark_default",
        application="/opt/airflow/dags/gba/services/parse_flatten.py",
        application_args=[
            "--input-path",
            input_path,
            "--output-path",
            output_path,
        ],
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
        conf=build_spark_conf(
            spark_master_url=settings.SPARK_MASTER_URL,
            aws_profile=settings.AWS_PROFILE,
            aws_config_file=settings.AWS_CONFIG_FILE,
            aws_shared_credentials_file=settings.AWS_SHARED_CREDENTIALS_FILE,
        ),
    )
    return ParseFLattenEvents(task=task, output_path=output_path)
