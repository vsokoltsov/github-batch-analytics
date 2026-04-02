from airflow.models.xcom_arg import XComArg
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from typing import NamedTuple
from gba.settings.enums import CandidatesType
from gba.settings.build_aggregates import get_build_aggregates_settings


class BuildAggregateEvent(NamedTuple):
    task: SparkSubmitOperator
    output_path: str


def build_repo_aggregates(
    input_path: str | XComArg, dt: str, hour: str
) -> BuildAggregateEvent:
    settings = get_build_aggregates_settings()
    output_path = f"s3a://{settings.S3_SILVER_ZONE_BUCKET_NAME}/repo_aggregates/dt={dt}/hr={hour}/"
    task = SparkSubmitOperator(
        task_display_name="Build repository aggregates",
        task_id="build_repo_aggregates",
        conn_id="spark_default",
        application="/opt/airflow/dags/gba/services/build_aggregates.py",
        application_args=[
            "--input-path",
            input_path,
            "--output-path",
            output_path,
            "--type",
            CandidatesType.REPO.value,
            "--dt",
            dt,
            "--hr",
            hour,
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
    return BuildAggregateEvent(task=task, output_path=output_path)


def build_org_aggregates(
    input_path: str | XComArg, dt: str, hour: str
) -> BuildAggregateEvent:
    settings = get_build_aggregates_settings()
    output_path = (
        f"s3a://{settings.S3_SILVER_ZONE_BUCKET_NAME}/org_aggregates/dt={dt}/hr={hour}/"
    )
    task = SparkSubmitOperator(
        task_id="build_org_aggregates",
        task_display_name="Build organization aggregates",
        conn_id="spark_default",
        application="/opt/airflow/dags/gba/services/build_aggregates.py",
        application_args=[
            "--input-path",
            input_path,
            "--output-path",
            output_path,
            "--type",
            CandidatesType.ORG.value,
            "--dt",
            dt,
            "--hr",
            hour,
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
    return BuildAggregateEvent(task=task, output_path=output_path)
