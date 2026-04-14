from __future__ import annotations

import os


def build_spark_conf(
    *,
    spark_master_url: str,
    aws_profile: str = "",
    aws_config_file: str = "",
    aws_shared_credentials_file: str = "",
) -> dict[str, str]:
    spark_cores_max = os.getenv("SPARK_CORES_MAX", "2")
    spark_executor_cores = os.getenv("SPARK_EXECUTOR_CORES", "1")
    spark_executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "1g")

    conf = {
        "spark.master": spark_master_url,
        "spark.cores.max": spark_cores_max,
        "spark.executor.cores": spark_executor_cores,
        "spark.executor.memory": spark_executor_memory,
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        ),
        "spark.executorEnv.PYTHONPATH": "/opt/airflow/dags",
    }

    if driver_host := os.getenv("AIRFLOW_POD_IP", ""):
        # Executors must be able to connect back to the driver from another pod.
        conf["spark.driver.host"] = driver_host

    if aws_profile:
        conf["spark.executorEnv.AWS_PROFILE"] = aws_profile
        conf["spark.executorEnv.AWS_SDK_LOAD_CONFIG"] = "1"

    if aws_config_file:
        conf["spark.executorEnv.AWS_CONFIG_FILE"] = aws_config_file

    if aws_shared_credentials_file:
        conf["spark.executorEnv.AWS_SHARED_CREDENTIALS_FILE"] = (
            aws_shared_credentials_file
        )

    return conf
