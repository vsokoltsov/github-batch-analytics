from __future__ import annotations

import os
from pathlib import Path

import boto3

from pyspark.sql import SparkSession


def _find_project_root() -> Path:
    root = Path.cwd()
    while root != root.parent and not (root / "pyproject.toml").exists():
        root = root.parent
    return root


def _resolve_aws_paths() -> tuple[str, str]:
    project_aws_dir = _find_project_root() / ".local" / "aws"
    home_aws_dir = Path.home() / ".aws"

    aws_dir = project_aws_dir if project_aws_dir.exists() else home_aws_dir
    return str(aws_dir / "config"), str(aws_dir / "credentials")


def create_spark_session(
    app_name: str = "gba-notebook",
    master: str | None = None,
    aws_profile: str = "gba-admin",
) -> SparkSession:
    resolved_master = master or os.environ.get("SPARK_MASTER_URL", "spark://localhost:7077")
    aws_config, aws_credentials = _resolve_aws_paths()
    ivy_dir = "/tmp/jupyter/ivy"
    os.makedirs(ivy_dir, exist_ok=True)

    # Local notebook kernel runs on host machine. Force host-valid AWS paths.
    os.environ["AWS_PROFILE"] = aws_profile
    os.environ["AWS_SDK_LOAD_CONFIG"] = "1"
    os.environ["AWS_CONFIG_FILE"] = aws_config
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = aws_credentials
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"

    # Resolve static/session credentials through boto3 once and inject into
    # Hadoop config directly. This avoids Java profile parsing differences.
    session = boto3.Session(profile_name=aws_profile)
    creds = session.get_credentials()
    if creds is None:
        raise ValueError(f"No AWS credentials found for profile: {aws_profile}")
    frozen = creds.get_frozen_credentials()

    spark = (
        SparkSession.builder.master(resolved_master)
        .appName(app_name)
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.jars.ivy", ivy_dir)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.access.key",
            frozen.access_key,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            frozen.secret_key,
        )
        .config("spark.hadoop.fs.s3a.session.token", frozen.token or "")
        .config("spark.executorEnv.AWS_PROFILE", aws_profile)
        .config("spark.executorEnv.AWS_SDK_LOAD_CONFIG", "1")
        .config("spark.executorEnv.AWS_CONFIG_FILE", "/home/spark/.aws/config")
        .config(
            "spark.executorEnv.AWS_SHARED_CREDENTIALS_FILE", "/home/spark/.aws/credentials"
        )
        .config("spark.executorEnv.AWS_EC2_METADATA_DISABLED", "true")
        .config(
            "spark.executor.extraJavaOptions",
            f"-Daws.profile={aws_profile} -Daws.ec2MetadataDisabled=true",
        )
        .config("spark.driverEnv.AWS_PROFILE", aws_profile)
        .config("spark.driverEnv.AWS_SDK_LOAD_CONFIG", "1")
        .config("spark.driverEnv.AWS_CONFIG_FILE", aws_config)
        .config("spark.driverEnv.AWS_SHARED_CREDENTIALS_FILE", aws_credentials)
        .config("spark.driverEnv.AWS_EC2_METADATA_DISABLED", "true")
        .getOrCreate()
    )
    return spark
