from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from gba.services.utils import spark_session, to_s3a
from gba.settings.enums import CandidatesType


@dataclass
class BuildMarts:
    spark: SparkSession
    candidates_path: str
    github_snapshot_path: str
    output_path: str
    candidates_df: DataFrame = field(init=False)
    github_snapshot_df: DataFrame = field(init=False)

    def __post_init__(self):
        self.candidates_df = self.spark.read.parquet(self.candidates_path)
        self.github_snapshot_df = self.spark.read.parquet(self.github_snapshot_path)

    @staticmethod
    def _merge_joined_frames(
        left_df: DataFrame,
        right_df: DataFrame,
        left_key: str,
        right_key: str,
    ) -> DataFrame:
        left = left_df.alias("c")
        right = right_df.alias("s")
        overlapping = set(left_df.columns) & set(right_df.columns)

        left_columns = [F.col(f"c.{column}") for column in left_df.columns]
        right_columns = [
            F.col(f"s.{column}")
            for column in right_df.columns
            if column not in overlapping
        ]

        return left.join(
            right,
            F.col(f"c.{left_key}") == F.col(f"s.{right_key}"),
            "left",
        ).select(*left_columns, *right_columns)

    def repositories(self) -> None:
        df = self._merge_joined_frames(
            self.candidates_df,
            self.github_snapshot_df,
            left_key="repo_id",
            right_key="repo_id",
        )
        df = df.drop("dt", "hr")
        df.write.mode("overwrite").parquet(self.output_path)

    def organizations(self) -> None:
        df = self._merge_joined_frames(
            self.candidates_df,
            self.github_snapshot_df,
            left_key="org_id",
            right_key="org_id",
        )
        df = df.drop("dt", "hr")
        df.write.mode("overwrite").parquet(self.output_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates-path", required=True)
    parser.add_argument("--github-snapshot-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--type", required=True)
    args = parser.parse_args()

    spark = spark_session(f"gharchive-build-markts-{args.type}")

    service = BuildMarts(
        spark=spark,
        candidates_path=to_s3a(args.candidates_path),
        github_snapshot_path=to_s3a(args.github_snapshot_path),
        output_path=to_s3a(args.output_path),
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
