from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from gba.tasks.get_archive import get_github_events_archive

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

    parse_flatten_step = SparkSubmitOperator(
        task_id="parse_flatten_events",
        conn_id="spark_default",
        application="/opt/spark/jobs/parse_flatten.py",
        application_args=[
            "--input-path",
            download_step,
            "--output-path",
            (
                "s3a://gba-bronze-zone-prod/gh_events_flat/dt={{ ds }}/hr={{ logical_date.hour }}/"
            ),
        ],
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.cores.max": "2",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": (
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
            ),
            "spark.executorEnv.AWS_PROFILE": "gba-admin",
            "spark.executorEnv.AWS_SDK_LOAD_CONFIG": "1",
            "spark.executorEnv.AWS_CONFIG_FILE": "/home/spark/.aws/config",
            "spark.executorEnv.AWS_SHARED_CREDENTIALS_FILE": "/home/spark/.aws/credentials"
        },
    )

    download_step >> parse_flatten_step
