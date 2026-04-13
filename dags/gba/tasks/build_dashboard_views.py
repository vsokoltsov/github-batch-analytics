from airflow.models.xcom_arg import XComArg
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from gba.settings.enums import DashboardType
from gba.settings.build_dashboard_views import (
    get_dashboard_views_settings,
    BuildDashboardViewsSettings,
)


def _build_repo_dashboard_view(
    task_id: str,
    task_display_name: str,
    input_path: str,
    output_path: str,
    settings: BuildDashboardViewsSettings,
    sql_file: str,
    type: DashboardType,
) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        task_display_name=task_display_name,
        conn_id="spark_default",
        application="/opt/airflow/dags/gba/services/build_dashboard_views.py",
        application_args=[
            "--repo-path",
            input_path,
            "--sql-file",
            sql_file,
            "--output-path",
            output_path,
            "--type",
            type.value,
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


def _build_org_dashboard_view(
    task_id: str,
    task_display_name: str,
    input_path: str,
    output_path: str,
    settings: BuildDashboardViewsSettings,
    sql_file: str,
    type: DashboardType,
) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        task_display_name=task_display_name,
        conn_id="spark_default",
        application="/opt/airflow/dags/gba/services/build_dashboard_views.py",
        application_args=[
            "--org-path",
            input_path,
            "--sql-file",
            sql_file,
            "--output-path",
            output_path,
            "--type",
            type.value,
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


def _build_common_dashboard_view(
    task_id: str,
    task_display_name: str,
    repo_path: str,
    org_path: str,
    output_path: str,
    settings: BuildDashboardViewsSettings,
    sql_file: str,
    type: DashboardType,
) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        task_display_name=task_display_name,
        conn_id="spark_default",
        application="/opt/airflow/dags/gba/services/build_dashboard_views.py",
        application_args=[
            "--repo-path",
            repo_path,
            "--org-path",
            org_path,
            "--sql-file",
            sql_file,
            "--output-path",
            output_path,
            "--type",
            type.value,
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


def repository_summary_dashboard(
    input_path: str | XComArg, dt: str, hr: str
) -> SparkSubmitOperator:
    settings = get_dashboard_views_settings()
    output_path = f"s3a://{settings.S3_MARTS_BUCKET_NAME}/dashboards/repositories/summary/dt={dt}/hr={hr}/"
    return _build_repo_dashboard_view(
        task_id="build_repo_dashboard_summary",
        task_display_name="Build repository dashboard summary",
        sql_file="repositories/summary.sql",
        input_path=str(input_path),
        output_path=output_path,
        settings=settings,
        type=DashboardType.REPO,
    )


def _repository_dashboard_dataset(
    input_path: str | XComArg,
    dt: str,
    hr: str,
    *,
    dataset_name: str,
    task_suffix: str,
    task_display_name: str,
    sql_file: str,
) -> SparkSubmitOperator:
    settings = get_dashboard_views_settings()
    output_path = (
        f"s3a://{settings.S3_MARTS_BUCKET_NAME}/dashboards/repositories/"
        f"{dataset_name}/dt={dt}/hr={hr}/"
    )
    return _build_repo_dashboard_view(
        task_id=f"build_repo_dashboard_{task_suffix}",
        task_display_name=task_display_name,
        sql_file=f"repositories/{sql_file}",
        input_path=str(input_path),
        output_path=output_path,
        settings=settings,
        type=DashboardType.REPO,
    )


def repository_event_type_dashboard(
    input_path: str | XComArg,
    dt: str,
    hr: str,
) -> SparkSubmitOperator:
    return _repository_dashboard_dataset(
        input_path=input_path,
        dt=dt,
        hr=hr,
        dataset_name="event_type",
        task_suffix="event_type",
        task_display_name="Build repository dashboard event type",
        sql_file="event_type.sql",
    )


def repository_fork_dashboard(
    input_path: str | XComArg,
    dt: str,
    hr: str,
) -> SparkSubmitOperator:
    return _repository_dashboard_dataset(
        input_path=input_path,
        dt=dt,
        hr=hr,
        dataset_name="fork",
        task_suffix="fork",
        task_display_name="Build repository dashboard fork distribution",
        sql_file="fork.sql",
    )


def repository_freshness_dashboard(
    input_path: str | XComArg,
    dt: str,
    hr: str,
) -> SparkSubmitOperator:
    return _repository_dashboard_dataset(
        input_path=input_path,
        dt=dt,
        hr=hr,
        dataset_name="freshness",
        task_suffix="freshness",
        task_display_name="Build repository dashboard freshness",
        sql_file="freshness.sql",
    )


def repository_language_dashboard(
    input_path: str | XComArg,
    dt: str,
    hr: str,
) -> SparkSubmitOperator:
    return _repository_dashboard_dataset(
        input_path=input_path,
        dt=dt,
        hr=hr,
        dataset_name="language",
        task_suffix="language",
        task_display_name="Build repository dashboard language",
        sql_file="language.sql",
    )


def repository_owner_dashboard(
    input_path: str | XComArg,
    dt: str,
    hr: str,
) -> SparkSubmitOperator:
    return _repository_dashboard_dataset(
        input_path=input_path,
        dt=dt,
        hr=hr,
        dataset_name="owner",
        task_suffix="owner",
        task_display_name="Build repository dashboard owner distribution",
        sql_file="owner.sql",
    )


def repository_top_100_dashboard(
    input_path: str | XComArg,
    dt: str,
    hr: str,
) -> SparkSubmitOperator:
    return _repository_dashboard_dataset(
        input_path=input_path,
        dt=dt,
        hr=hr,
        dataset_name="top_100",
        task_suffix="top_100",
        task_display_name="Build repository dashboard top 100",
        sql_file="top_100.sql",
    )


def repository_visibility_dashboard(
    input_path: str | XComArg,
    dt: str,
    hr: str,
) -> SparkSubmitOperator:
    return _repository_dashboard_dataset(
        input_path=input_path,
        dt=dt,
        hr=hr,
        dataset_name="visibility",
        task_suffix="visibility",
        task_display_name="Build repository dashboard visibility",
        sql_file="visibility.sql",
    )


def org_summary_dashboard(
    input_path: str | XComArg, dt: str, hr: str
) -> SparkSubmitOperator:
    settings = get_dashboard_views_settings()
    output_path = f"s3a://{settings.S3_MARTS_BUCKET_NAME}/dashboards/organizations/summary/dt={dt}/hr={hr}/"
    return _build_org_dashboard_view(
        task_id="build_org_dashboard_summary",
        task_display_name="Build organizations dashboard summary",
        sql_file="organizations/summary.sql",
        input_path=str(input_path),
        output_path=output_path,
        settings=settings,
        type=DashboardType.ORG,
    )


def _organization_dashboard_dataset(
    input_path: str | XComArg,
    dt: str,
    hr: str,
    *,
    dataset_name: str,
    task_suffix: str,
    task_display_name: str,
    sql_file: str,
) -> SparkSubmitOperator:
    settings = get_dashboard_views_settings()
    output_path = (
        f"s3a://{settings.S3_MARTS_BUCKET_NAME}/dashboards/organizations/"
        f"{dataset_name}/dt={dt}/hr={hr}/"
    )
    return _build_org_dashboard_view(
        task_id=f"build_org_dashboard_{task_suffix}",
        task_display_name=task_display_name,
        sql_file=f"organizations/{sql_file}",
        input_path=str(input_path),
        output_path=output_path,
        settings=settings,
        type=DashboardType.ORG,
    )


def org_company_dashboard(
    input_path: str | XComArg,
    dt: str,
    hr: str,
) -> SparkSubmitOperator:
    return _organization_dashboard_dataset(
        input_path=input_path,
        dt=dt,
        hr=hr,
        dataset_name="company",
        task_suffix="company",
        task_display_name="Build organizations dashboard company",
        sql_file="company.sql",
    )


def org_location_dashboard(
    input_path: str | XComArg,
    dt: str,
    hr: str,
) -> SparkSubmitOperator:
    return _organization_dashboard_dataset(
        input_path=input_path,
        dt=dt,
        hr=hr,
        dataset_name="location",
        task_suffix="location",
        task_display_name="Build organizations dashboard location",
        sql_file="location.sql",
    )


def org_size_dashboard(
    input_path: str | XComArg,
    dt: str,
    hr: str,
) -> SparkSubmitOperator:
    return _organization_dashboard_dataset(
        input_path=input_path,
        dt=dt,
        hr=hr,
        dataset_name="size",
        task_suffix="size",
        task_display_name="Build organizations dashboard size",
        sql_file="size.sql",
    )


def org_social_dashboard(
    input_path: str | XComArg,
    dt: str,
    hr: str,
) -> SparkSubmitOperator:
    return _organization_dashboard_dataset(
        input_path=input_path,
        dt=dt,
        hr=hr,
        dataset_name="social",
        task_suffix="social",
        task_display_name="Build organizations dashboard social",
        sql_file="social.sql",
    )


def org_top_100_dashboard(
    input_path: str | XComArg,
    dt: str,
    hr: str,
) -> SparkSubmitOperator:
    return _organization_dashboard_dataset(
        input_path=input_path,
        dt=dt,
        hr=hr,
        dataset_name="top_100",
        task_suffix="top_100",
        task_display_name="Build organizations dashboard top 100",
        sql_file="top_100.sql",
    )


def org_verified_distribution_dashboard(
    input_path: str | XComArg,
    dt: str,
    hr: str,
) -> SparkSubmitOperator:
    return _organization_dashboard_dataset(
        input_path=input_path,
        dt=dt,
        hr=hr,
        dataset_name="verified_distribution",
        task_suffix="verified_distribution",
        task_display_name="Build organizations dashboard verified distribution",
        sql_file="verified_distribution.sql",
    )


def common_rollup_dashboard(
    repo_path: str | XComArg, org_path: str | XComArg, dt: str, hr: str
) -> SparkSubmitOperator:
    settings = get_dashboard_views_settings()
    output_path = f"s3a://{settings.S3_MARTS_BUCKET_NAME}/dashboards/common/summary/dt={dt}/hr={hr}/"
    return _build_common_dashboard_view(
        task_id="build_common_dashboard_rollup",
        task_display_name="Build common dashboard rollup",
        sql_file="common/rollup.sql",
        repo_path=str(repo_path),
        org_path=str(org_path),
        output_path=output_path,
        settings=settings,
        type=DashboardType.COMMON,
    )


def _common_dashboard_dataset(
    repo_path: str | XComArg,
    org_path: str | XComArg,
    dt: str,
    hr: str,
    *,
    dataset_name: str,
    task_suffix: str,
    task_display_name: str,
    sql_file: str,
) -> SparkSubmitOperator:
    settings = get_dashboard_views_settings()
    output_path = (
        f"s3a://{settings.S3_MARTS_BUCKET_NAME}/dashboards/common/"
        f"{dataset_name}/dt={dt}/hr={hr}/"
    )
    return _build_common_dashboard_view(
        task_id=f"build_common_dashboard_{task_suffix}",
        task_display_name=task_display_name,
        sql_file=f"common/{sql_file}",
        repo_path=str(repo_path),
        org_path=str(org_path),
        output_path=output_path,
        settings=settings,
        type=DashboardType.COMMON,
    )


def common_language_location_dashboard(
    repo_path: str | XComArg,
    org_path: str | XComArg,
    dt: str,
    hr: str,
) -> SparkSubmitOperator:
    return _common_dashboard_dataset(
        repo_path=repo_path,
        org_path=org_path,
        dt=dt,
        hr=hr,
        dataset_name="language_location",
        task_suffix="language_location",
        task_display_name="Build common dashboard language location",
        sql_file="language_location.sql",
    )


def common_verified_dashboard(
    repo_path: str | XComArg,
    org_path: str | XComArg,
    dt: str,
    hr: str,
) -> SparkSubmitOperator:
    return _common_dashboard_dataset(
        repo_path=repo_path,
        org_path=org_path,
        dt=dt,
        hr=hr,
        dataset_name="verified",
        task_suffix="verified",
        task_display_name="Build common dashboard verified distribution",
        sql_file="verified.sql",
    )
