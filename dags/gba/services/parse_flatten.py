from __future__ import annotations

import argparse

from dataclasses import dataclass
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name


def _to_s3a(path: str) -> str:
    if path.startswith("s3://"):
        return "s3a://" + path[len("s3://") :]
    return path


@dataclass
class ParseService:
    spark: SparkSession
    input_path: str
    output_path: str

    def flat(self):
        source_df = self.spark.read.json(self.input_path)
        flattened_df = (
            source_df.select(
                col("id").alias("event_id"),
                col("type").alias("event_type"),
                col("created_at"),
                col("repo.id").alias("repo_id"),
                col("repo.name").alias("repo_full_name"),
                col("actor.id").alias("actor_id"),
                col("actor.login").alias("actor_login"),
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
    service = ParseService(
        spark=spark,
        input_path=input_path,
        output_path=output_path
    )
    service.flat()
    spark.stop()


if __name__ == "__main__":
    main()
