from __future__ import annotations

import argparse

import re
from typing import Dict, List, cast, Any
from dataclasses import dataclass, field
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as F
from gba.settings.build_candidates import CandidatesType


def camel_to_snake(value: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()


def _to_s3a(path: str) -> str:
    if path.startswith("s3://"):
        return "s3a://" + path[len("s3://") :]
    return path


@dataclass
class BuildCandidates:
    spark: SparkSession
    input_path: str
    output_path: str
    dt: str
    hr: str
    df: DataFrame = field(init=False)
    event_columns: List[Column] = field(init=False, default_factory=list)
    ratio_columns: Dict[str, Column] = field(init=False, default_factory=dict)

    def __post_init__(self):
        self.df = self.spark.read.parquet(self.input_path)
        for raw_name in self.event_types_list:
            alias_name = f"{camel_to_snake(raw_name)}s"
            column = F.sum(
                F.when(F.col("event_type") == raw_name, 1).otherwise(0)
            ).alias(alias_name)
            self.ratio_columns[f"{alias_name}_ratio"] = F.when(
                F.col("total_events") > 0, F.col(alias_name) / F.col("total_events")
            ).otherwise(F.lit(0.0))
            self.event_columns.append(column)

    def repositories(self):
        df = self._common_metrics(["repo_id", "repo_full_name"])
        df.write.mode("overwrite").parquet(self.output_path)

    def organizations(self):
        df = self._common_metrics(
            ["org_id", "org_login"],
            F.countDistinct("repo_id").alias("repos_count"),
        )
        df.write.mode("overwrite").parquet(self.output_path)

    def _common_metrics(self, group_by: List[str], *columns) -> DataFrame:
        metrics = self.df.groupBy(*group_by).agg(
            F.count("*").alias("total_events"),
            F.count("is_public").alias("public_events_count"),
            F.countDistinct("actor_id").alias("unique_actors"),
            F.max("created_at").alias("last_event_at"),
            F.sum(
                F.when(F.lower(F.col("actor_login")).ilike("%[bot]"), 1).otherwise(0)
            ).alias("bot_events"),
            F.countDistinct("issue_id").alias("issues_count"),
            F.countDistinct("comment_id").alias("comments_count"),
            F.countDistinct("release_id").alias("releases_count"),
            F.countDistinct("pull_request_merged").alias("merged_pr_count"),
            *self.event_columns,
            *columns,
        )

        for key, col in self.ratio_columns.items():
            metrics = metrics.withColumn(key, col)

        metrics = (
            metrics.withColumn(
                "composite_score",
                0.5 * F.log1p(F.col("total_events"))
                + 0.3 * F.col("push_events_ratio")
                + 0.2 * F.log1p(F.col("unique_actors")),
            )
            .withColumn(
                "bot_ratio",
                F.when(
                    F.col("total_events") > 0,
                    F.col("bot_events") / F.col("total_events"),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn("dt", F.lit(self.dt).cast("date"))
            .withColumn("hr", F.lit(self.hr).cast("int"))
            .withColumn(
                "last_event_at",
                F.to_timestamp("last_event_at", "yyyy-MM-dd'T'HH:mm:ssX"),
            )
        )
        return metrics

    @property
    def event_types_list(self) -> List:
        return [
            item.event_type
            for item in self.df.select("event_type").distinct().collect()
        ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse and flatten GH Archive events")
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--type", required=True)
    parser.add_argument("--dt", required=True)
    parser.add_argument("--hr", required=True)
    args = parser.parse_args()

    builder = cast(Any, SparkSession.builder)
    spark = builder.appName(f"gharchive-build-candidate-{args.type}").getOrCreate()
    input_path = _to_s3a(args.input_path)
    output_path = _to_s3a(args.output_path)

    service = BuildCandidates(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        dt=args.dt,
        hr=args.hr,
    )
    candidate_type = CandidatesType(args.type)
    match candidate_type:
        case CandidatesType.REPO:
            service.repositories()
        case CandidatesType.ORG:
            service.organizations()
    spark.stop()


if __name__ == "__main__":
    main()
