from typing import NamedTuple

from airflow.models.xcom_arg import XComArg
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from gba.settings.enums import CandidatesType
from gba.settings.build_candidates import get_build_candidates_settings
from gba.tasks.spark_conf import build_spark_conf


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
        task_display_name="Build repository candidates",
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
            str(settings.CANDIDATES_SIZE),
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
        task_display_name="Build organization candidates",
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
            str(settings.CANDIDATES_SIZE),
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
    return BuildCandidateEvent(task=task, output_path=output_path)
