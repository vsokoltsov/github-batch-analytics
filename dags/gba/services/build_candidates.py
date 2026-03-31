from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from gba.settings.enums import CandidatesType
from gba.services.utils import spark_session, to_s3a

REPO_METRICS = [
    "total_events",
    "public_events_count",
    "unique_actors",
    "bot_events",
    "issues_count",
    "comments_count",
    "releases_count",
    "merged_pr_count",
    "pull_request_review_events",
    "push_events",
    "gollum_events",
    "release_events",
    "commit_comment_events",
    "create_events",
    "pull_request_review_comment_events",
    "issue_comment_events",
    "delete_events",
    "issues_events",
    "fork_events",
    "public_events",
    "member_events",
    "watch_events",
    "pull_request_events",
    "discussion_events",
    "pull_request_review_events_ratio",
    "push_events_ratio",
    "gollum_events_ratio",
    "release_events_ratio",
    "commit_comment_events_ratio",
    "create_events_ratio",
    "pull_request_review_comment_events_ratio",
    "issue_comment_events_ratio",
    "delete_events_ratio",
    "issues_events_ratio",
    "fork_events_ratio",
    "public_events_ratio",
    "member_events_ratio",
    "watch_events_ratio",
    "pull_request_events_ratio",
    "discussion_events_ratio",
    "composite_score",
    "bot_ratio",
]

ORG_METRICS = REPO_METRICS + ["repos_count"]


@dataclass
class BuildCandidates:
    spark: SparkSession
    input_path: str
    output_path: str
    top_n: int = 100

    def _top_n_union(
        self,
        df: DataFrame,
        entity_keys: list[str],
        metrics: list[str],
    ) -> DataFrame:
        candidate_frames: list[DataFrame] = []

        for metric in metrics:
            ranked = (
                df.filter(F.isnotnull(F.col(metric)))
                .withColumn(
                    "metric_rank",
                    F.row_number().over(
                        Window.orderBy(F.desc_nulls_last(F.col(metric)))
                    ),
                )
                .filter(F.col("metric_rank") <= self.top_n)
                .drop("metric_rank")
                .withColumn("selection_metric", F.lit(metric))
            )
            candidate_frames.append(ranked)

        unioned = candidate_frames[0]
        for frame in candidate_frames[1:]:
            unioned = unioned.unionByName(frame)

        return unioned.groupBy(*entity_keys).agg(
            *[
                F.first(c, ignorenulls=True).alias(c)
                for c in df.columns
                if c not in entity_keys
            ],
            F.array_sort(F.collect_set("selection_metric")).alias("selection_reasons"),
        )

    def repositories(self) -> None:
        aggregates = (
            self.spark.read.parquet(self.input_path)
            .filter(F.isnotnull(F.col("repo_id")))
            .filter(F.isnotnull(F.col("repo_full_name")))
            .filter(F.col("repo_full_name").like("%/%"))
        )

        candidates = self._top_n_union(
            aggregates,
            ["repo_id", "repo_full_name"],
            REPO_METRICS,
        )
        candidates.write.mode("overwrite").parquet(self.output_path)

    def organizations(self) -> None:
        aggregates = (
            self.spark.read.parquet(self.input_path)
            .filter(F.isnotnull(F.col("org_id")))
            .filter(F.isnotnull(F.col("org_login")))
        )

        candidates = self._top_n_union(
            aggregates,
            ["org_id", "org_login"],
            ORG_METRICS,
        )
        candidates.write.mode("overwrite").parquet(self.output_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--type", required=True)
    parser.add_argument("--top-n", type=int, default=100)
    args = parser.parse_args()

    spark = spark_session(f"gharchive-build-candidates-{args.type}")

    service = BuildCandidates(
        spark=spark,
        input_path=to_s3a(args.input_path),
        output_path=to_s3a(args.output_path),
        top_n=args.top_n,
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
