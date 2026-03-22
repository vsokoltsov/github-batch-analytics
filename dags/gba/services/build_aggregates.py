from __future__ import annotations

import argparse
import re
from dataclasses import dataclass, field
from typing import Dict, List, cast, Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from gba.settings.enums import CandidatesType

import logging

logger = logging.getLogger(__name__)


def camel_to_snake(value: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()


def _to_s3a(path: str) -> str:
    if path.startswith("s3://"):
        return "s3a://" + path[len("s3://") :]
    return path


@dataclass
class BuildAggregates:
    spark: SparkSession
    input_path: str
    output_path: str
    dt: str
    hr: str
    df: DataFrame = field(init=False)

    def __post_init__(self):
        self.df = self.spark.read.parquet(self.input_path)

    def repositories(self) -> None:
        df = (
            self.df.filter(F.isnotnull(F.col("repo_id")))
            .filter(F.isnotnull(F.col("repo_full_name")))
            .filter(F.col("repo_full_name").like("%/%"))
        )

        event_columns, ratio_columns = self._build_event_columns(df)
        metrics = self._common_metrics(
            df,
            ["repo_id", "repo_full_name"],
            event_columns,
            ratio_columns,
        )
        metrics.write.mode("overwrite").parquet(self.output_path)

    def organizations(self) -> None:
        df = self.df.filter(F.isnotnull(F.col("org_id"))).filter(
            F.isnotnull(F.col("org_login"))
        )

        event_columns, ratio_columns = self._build_event_columns(df)
        metrics = self._common_metrics(
            df,
            ["org_id", "org_login"],
            event_columns,
            ratio_columns,
            F.countDistinct("repo_id").alias("repos_count"),
        )
        metrics.write.mode("overwrite").parquet(self.output_path)

    def _build_event_columns(
        self,
        df: DataFrame,
    ) -> tuple[List[Column], Dict[str, Column]]:
        event_columns: List[Column] = []
        ratio_columns: Dict[str, Column] = {}

        event_types = [
            row.event_type
            for row in df.select("event_type").distinct().collect()
            if row.event_type is not None
        ]

        for raw_name in event_types:
            alias_name = f"{camel_to_snake(raw_name)}s"

            event_columns.append(
                F.sum(F.when(F.col("event_type") == raw_name, 1).otherwise(0)).alias(
                    alias_name
                )
            )

            ratio_columns[f"{alias_name}_ratio"] = F.when(
                F.col("total_events") > 0,
                F.col(alias_name) / F.col("total_events"),
            ).otherwise(F.lit(0.0))

        return event_columns, ratio_columns

    def _common_metrics(
        self,
        df: DataFrame,
        group_by: List[str],
        event_columns: List[Column],
        ratio_columns: Dict[str, Column],
        *extra_columns: Column,
    ) -> DataFrame:
        metrics = df.groupBy(*group_by).agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("is_public"), 1).otherwise(0)).alias(
                "public_events_count"
            ),
            F.countDistinct("actor_id").alias("unique_actors"),
            F.max("created_at").alias("last_event_at"),
            F.sum(
                F.when(F.lower(F.col("actor_login")).like("%[bot]%"), 1).otherwise(0)
            ).alias("bot_events"),
            F.countDistinct("issue_id").alias("issues_count"),
            F.countDistinct("comment_id").alias("comments_count"),
            F.countDistinct("release_id").alias("releases_count"),
            F.sum(F.when(F.col("pull_request_merged"), 1).otherwise(0)).alias(
                "merged_pr_count"
            ),
            *event_columns,
            *extra_columns,
        )

        for name, expr in ratio_columns.items():
            metrics = metrics.withColumn(name, expr)

        return (
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build repository and organization candidates"
    )
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--type", required=True)
    parser.add_argument("--dt", required=True)
    parser.add_argument("--hr", required=True)
    args = parser.parse_args()

    builder = cast(Any, SparkSession.builder)
    spark: SparkSession = builder.appName(
        f"gharchive-build-aggregate-{args.type}"
    ).getOrCreate()

    service = BuildAggregates(
        spark=spark,
        input_path=_to_s3a(args.input_path),
        output_path=_to_s3a(args.output_path),
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
