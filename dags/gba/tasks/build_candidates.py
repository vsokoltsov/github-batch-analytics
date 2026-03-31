from typing import NamedTuple

from airflow.models.xcom_arg import XComArg
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from gba.settings.enums import CandidatesType
from gba.settings.build_candidates import get_build_candidates_settings


class BuildCandidateEvent(NamedTuple):
    task: SparkSubmitOperator
    output_path: str


def build_repo_candidates(
    input_path: str | XComArg, dt: str, hour: str
) -> BuildCandidateEvent:
    settings = get_build_candidates_settings()
    output_path = f"s3a://{settings.S3_SILVER_ZONE_BUCKET_NAME}/repo_candidates/dt={dt}/hr={hour}/"

    task = SparkSubmitOperator(
        task_id="build_repo_candidates",
        conn_id="spark_default",
        application="/opt/airflow/dags/gba/services/build_candidates.py",
        application_args=[
            "--input-path",
            input_path,
            "--output-path",
            output_path,
            "--type",
            CandidatesType.REPO.value,
            "--top-n",
            settings.CANDIDATES_SIZE,
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
            "spark.executorEnv.AWS_SHARED_CREDENTIALS_FILE": settings.AWS_SHARED_CREDENTIALS_FILE,
            "spark.executorEnv.PYTHONPATH": "/opt/airflow/dags",
        },
    )
    return BuildCandidateEvent(task=task, output_path=output_path)


def build_org_candidates(
    input_path: str | XComArg, dt: str, hour: str
) -> BuildCandidateEvent:
    settings = get_build_candidates_settings()
    output_path = (
        f"s3a://{settings.S3_SILVER_ZONE_BUCKET_NAME}/org_candidates/dt={dt}/hr={hour}/"
    )

    task = SparkSubmitOperator(
        task_id="build_org_candidates",
        conn_id="spark_default",
        application="/opt/airflow/dags/gba/services/build_candidates.py",
        application_args=[
            "--input-path",
            input_path,
            "--output-path",
            output_path,
            "--type",
            CandidatesType.ORG.value,
            "--top-n",
            "25",
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
            "spark.executorEnv.AWS_SHARED_CREDENTIALS_FILE": settings.AWS_SHARED_CREDENTIALS_FILE,
            "spark.executorEnv.PYTHONPATH": "/opt/airflow/dags",
        },
    )
    return BuildCandidateEvent(task=task, output_path=output_path)
