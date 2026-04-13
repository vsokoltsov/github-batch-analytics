from __future__ import annotations

import os
import argparse
from dataclasses import dataclass, field
from pyspark.sql import DataFrame, SparkSession
from gba.services.utils import spark_session
from gba.settings.enums import DashboardType

_SERVICE_DIR = os.path.dirname(os.path.abspath(__file__))


@dataclass
class DashboardView:
    spark: SparkSession
    output_path: str
    sql_file: str


@dataclass
class RepoDashboardView(DashboardView):
    input_path: str
    df: DataFrame = field(init=False)

    def __post_init__(self):
        self.df = self.spark.read.parquet(self.input_path)
        self.df.createOrReplaceTempView("repo_marts")

    def build(self) -> None:
        with open(
            os.path.join(_SERVICE_DIR, "dashboards", self.sql_file),
            "r",
            encoding="utf-8",
        ) as f:
            query = f.read()
        result_df = self.spark.sql(query)
        result_df.write.mode("overwrite").parquet(self.output_path)


@dataclass
class OrgDashboardView(DashboardView):
    input_path: str
    df: DataFrame = field(init=False)

    def __post_init__(self):
        self.df = self.spark.read.parquet(self.input_path)
        self.df.createOrReplaceTempView("org_marts")

    def build(self) -> None:
        with open(
            os.path.join(_SERVICE_DIR, "dashboards", self.sql_file),
            "r",
            encoding="utf-8",
        ) as f:
            query = f.read()
        result_df = self.spark.sql(query)
        result_df.write.mode("overwrite").parquet(self.output_path)


@dataclass
class RepoOrgDashboardView(DashboardView):
    repo_path: str
    org_path: str
    repo_df: DataFrame = field(init=False)
    org_df: DataFrame = field(init=False)

    def __post_init__(self):
        self.repo_df = self.spark.read.parquet(self.repo_path)
        self.org_df = self.spark.read.parquet(self.org_path)
        self.repo_df.createOrReplaceTempView("repo_marts")
        self.org_df.createOrReplaceTempView("org_marts")

    def build(self) -> None:
        with open(
            os.path.join(_SERVICE_DIR, "dashboards", self.sql_file),
            "r",
            encoding="utf-8",
        ) as f:
            query = f.read()
        result_df = self.spark.sql(query)
        result_df.write.mode("overwrite").parquet(self.output_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-path", required=False)
    parser.add_argument("--org-path", required=False)
    parser.add_argument("--sql-file", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--type", required=True)
    args = parser.parse_args()

    spark = spark_session(f"gharchive-build-dashboards-{args.type}")

    candidate_type = DashboardType(args.type)
    match candidate_type:
        case DashboardType.REPO:
            service = RepoDashboardView(
                spark=spark,
                input_path=args.repo_path,
                output_path=args.output_path,
                sql_file=args.sql_file,
            )
        case DashboardType.ORG:
            service = OrgDashboardView(
                spark=spark,
                input_path=args.org_path,
                output_path=args.output_path,
                sql_file=args.sql_file,
            )
        case DashboardType.COMMON:
            service = RepoOrgDashboardView(
                spark=spark,
                repo_path=args.repo_path,
                org_path=args.org_path,
                output_path=args.output_path,
                sql_file=args.sql_file,
            )

    service.build()
    spark.stop()


if __name__ == "__main__":
    main()
