from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path

import boto3

_TRANSFORMATIONS_DIR = Path(__file__).resolve().parent / "transformations"


@dataclass(frozen=True)
class BucketedMartConfig:
    database_name: str
    source_table_name: str
    target_table_name: str
    query_results_bucket_name: str
    marts_bucket_name: str
    dt: str
    hr: str
    bucket_column: str
    bucket_count: int

    @property
    def target_prefix(self) -> str:
        return f"{self.target_table_name}/dt={self.dt}/hr={self.hr}/"

    @property
    def target_location(self) -> str:
        return f"s3://{self.marts_bucket_name}/{self.target_prefix}"

    @property
    def table_root_location(self) -> str:
        return f"s3://{self.marts_bucket_name}/{self.target_table_name}/"

    @property
    def source_stage_table_name(self) -> str:
        return f"{self.source_table_name}_stage"

    @property
    def source_stage_location(self) -> str:
        return f"s3://{self.marts_bucket_name}/{self.source_stage_table_name}/"

    @property
    def output_path(self) -> str:
        return f"s3a://{self.marts_bucket_name}/{self.target_prefix}"

    @property
    def athena_results_location(self) -> str:
        return f"s3://{self.query_results_bucket_name}/results/"


def _s3_prefix_parts(path: str) -> tuple[str, str]:
    normalized = path.removeprefix("s3://")
    bucket, _, key = normalized.partition("/")
    return bucket, key


def delete_s3_prefix(s3_client, s3_path: str) -> None:
    bucket, prefix = _s3_prefix_parts(s3_path)
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])
        if not objects:
            continue

        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
        )


def get_non_partition_columns(
    glue_client, database_name: str, table_name: str
) -> list[str]:
    table = glue_client.get_table(DatabaseName=database_name, Name=table_name)["Table"]
    return [column["Name"] for column in table["StorageDescriptor"]["Columns"]]


def build_temp_table_name(config: BucketedMartConfig) -> str:
    return (
        f"tmp_{config.target_table_name}_{config.dt.replace('-', '')}_{config.hr}_"
        f"{uuid.uuid4().hex[:8]}"
    )


def build_bucketed_ctas_query(
    config: BucketedMartConfig,
    temp_table_name: str,
    select_columns: list[str],
) -> str:
    projection = ",\n        ".join(select_columns)
    template = (_TRANSFORMATIONS_DIR / "bucketed_mart_ctas.sql").read_text()
    return template.format(
        database_name=config.database_name,
        temp_table_name=temp_table_name,
        table_root_location=config.table_root_location,
        bucket_column=config.bucket_column,
        bucket_count=config.bucket_count,
        projection=projection,
        dt=config.dt,
        hr=int(config.hr),
        source_stage_table_name=config.source_stage_table_name,
    ).strip()


def build_drop_table_query(database_name: str, temp_table_name: str) -> str:
    return f"DROP TABLE IF EXISTS {database_name}.{temp_table_name}"


def normalize_bucketed_mart_config(
    config: BucketedMartConfig | dict[str, str | int],
) -> BucketedMartConfig:
    if isinstance(config, BucketedMartConfig):
        return config

    return BucketedMartConfig(
        database_name=str(config["database_name"]),
        source_table_name=str(config["source_table_name"]),
        target_table_name=str(config["target_table_name"]),
        query_results_bucket_name=str(config["query_results_bucket_name"]),
        marts_bucket_name=str(config["marts_bucket_name"]),
        dt=str(config["dt"]),
        hr=str(config["hr"]),
        bucket_column=str(config["bucket_column"]),
        bucket_count=int(config["bucket_count"]),
    )


def submit_bucketed_mart_ctas(
    *,
    aws_region: str,
    workgroup_name: str,
    config: BucketedMartConfig | dict[str, str | int],
) -> dict[str, str]:
    config = normalize_bucketed_mart_config(config)

    session = boto3.session.Session(region_name=aws_region)
    athena_client = session.client("athena")
    glue_client = session.client("glue")
    s3_client = session.client("s3")

    delete_s3_prefix(s3_client, config.target_location)

    select_columns = get_non_partition_columns(
        glue_client=glue_client,
        database_name=config.database_name,
        table_name=config.source_stage_table_name,
    )
    temp_table_name = build_temp_table_name(config)
    ctas_query = build_bucketed_ctas_query(
        config=config,
        temp_table_name=temp_table_name,
        select_columns=select_columns,
    )

    start = athena_client.start_query_execution(
        QueryString=ctas_query,
        WorkGroup=workgroup_name,
        QueryExecutionContext={"Database": config.database_name},
        ResultConfiguration={"OutputLocation": config.athena_results_location},
    )
    return {
        "query_execution_id": start["QueryExecutionId"],
        "temp_table_name": temp_table_name,
        "output_path": config.output_path,
    }
