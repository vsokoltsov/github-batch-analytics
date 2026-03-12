from __future__ import annotations

from airflow.models.xcom_arg import XComArg
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from gba.settings.parse_flatten import get_parse_flatten_settings


def get_parse_flatten_events_task(input_path: str | XComArg, dt: str, hour: str) -> SparkSubmitOperator:
    settings = get_parse_flatten_settings()

    return SparkSubmitOperator(
        task_id="parse_flatten_events",
        conn_id="spark_default",
        application="/opt/airflow/dags/gba/services/parse_flatten.py",
        application_args=[
            "--input-path",
            input_path,
            "--output-path",
            f"s3a://{settings.S3_BRONZE_ZONE_BUCKET_NAME}/gh_events_flat/dt={dt}/hr={hour}/",
        ],
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
        conf={
            "spark.master": settings.SPARK_MASTER_URL,
            "spark.cores.max": "2",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": (
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
            ),
            "spark.executorEnv.AWS_PROFILE": settings.AWS_PROFILE,
            "spark.executorEnv.AWS_SDK_LOAD_CONFIG": "1",
            "spark.executorEnv.AWS_CONFIG_FILE": settings.AWS_CONFIG_FILE,
            "spark.executorEnv.AWS_SHARED_CREDENTIALS_FILE": (
                settings.AWS_SHARED_CREDENTIALS_FILE
            ),
            "spark.executorEnv.PYTHONPATH": "/opt/airflow/dags",
        },
    )
