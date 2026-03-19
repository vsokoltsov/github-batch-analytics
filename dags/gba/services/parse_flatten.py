from __future__ import annotations

import argparse

from dataclasses import dataclass
from pyspark.sql import Column
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit
from pyspark.sql.types import StructType


def _to_s3a(path: str) -> str:
    if path.startswith("s3://"):
        return "s3a://" + path[len("s3://") :]
    return path


def _has_nested_field(schema: StructType, dotted_path: str) -> bool:
    current_type: StructType | None = schema
    for part in dotted_path.split("."):
        if current_type is None:
            return False
        field = next((item for item in current_type.fields if item.name == part), None)
        if field is None:
            return False
        current_type = (
            field.dataType if isinstance(field.dataType, StructType) else None
        )
    return True


def _optional_col(
    schema: StructType, source_path: str, alias_name: str, data_type: str
) -> Column:
    if _has_nested_field(schema, source_path):
        return col(source_path).alias(alias_name)
    return lit(None).cast(data_type).alias(alias_name)


@dataclass
class ParseService:
    spark: SparkSession
    input_path: str
    output_path: str

    def flat(self):
        source_df = self.spark.read.json(self.input_path)
        schema = source_df.schema
        flattened_df = (
            source_df.select(
                col("id").alias("event_id"),
                col("type").alias("event_type"),
                col("created_at"),
                col("repo.id").alias("repo_id"),
                col("repo.name").alias("repo_full_name"),
                col("actor.id").alias("actor_id"),
                col("actor.login").alias("actor_login"),
                col("org.id").alias("org_id"),
                col("org.login").alias("org_login"),
                col("org.url").alias("org_url"),
                col("org.avatar_url").alias("org_avatar_url"),
                col("public").alias("is_public"),
                _optional_col(schema, "payload.action", "payload_action", "string"),
                _optional_col(schema, "payload.ref_type", "payload_ref_type", "string"),
                _optional_col(
                    schema, "payload.pull_request.id", "pull_request_id", "bigint"
                ),
                _optional_col(
                    schema,
                    "payload.pull_request.merged",
                    "pull_request_merged",
                    "boolean",
                ),
                _optional_col(schema, "payload.issue.id", "issue_id", "bigint"),
                _optional_col(schema, "payload.comment.id", "comment_id", "bigint"),
                _optional_col(schema, "payload.release.id", "release_id", "bigint"),
                _optional_col(schema, "payload.member.id", "member_id", "bigint"),
            )
            .withColumn("ingestion_ts", current_timestamp())
            .withColumn("source_file", input_file_name())
        )

        flattened_df.write.mode("overwrite").parquet(self.output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse and flatten GH Archive events")
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("gharchive-parse-flatten").getOrCreate()

    input_path = _to_s3a(args.input_path)
    output_path = _to_s3a(args.output_path)
    service = ParseService(spark=spark, input_path=input_path, output_path=output_path)
    service.flat()
    spark.stop()


if __name__ == "__main__":
    main()
