from __future__ import annotations

from argparse import Namespace
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pytest
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from gba.services.build_aggregates import (
    BuildAggregates,
    _to_s3a,
    camel_to_snake,
    main,
)
from gba.settings.enums import CandidatesType

INPUT_SCHEMA = StructType(
    [
        StructField("event_id", LongType(), True),
        StructField("event_type", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("repo_id", LongType(), True),
        StructField("repo_full_name", StringType(), True),
        StructField("actor_id", LongType(), True),
        StructField("actor_login", StringType(), True),
        StructField("org_id", LongType(), True),
        StructField("org_login", StringType(), True),
        StructField("org_url", StringType(), True),
        StructField("org_avatar_url", StringType(), True),
        StructField("is_public", BooleanType(), True),
        StructField("payload_action", StringType(), True),
        StructField("payload_ref_type", StringType(), True),
        StructField("pull_request_id", LongType(), True),
        StructField("pull_request_merged", BooleanType(), True),
        StructField("issue_id", LongType(), True),
        StructField("comment_id", LongType(), True),
        StructField("release_id", LongType(), True),
        StructField("member_id", LongType(), True),
        StructField("ingestion_ts", StringType(), True),
        StructField("source_file", StringType(), True),
    ]
)


def _write_input_parquet(spark, tmp_path: Path, rows: list[dict]) -> str:
    input_path = tmp_path / "input"
    spark.createDataFrame(rows, schema=INPUT_SCHEMA).write.mode("overwrite").parquet(
        str(input_path)
    )
    return str(input_path)


@pytest.fixture
def repo_aggregate_input_rows() -> list[dict]:
    return [
        {
            "event_id": 1,
            "event_type": "PushEvent",
            "created_at": "2026-03-20T21:00:00Z",
            "repo_id": 101,
            "repo_full_name": "acme/widgets",
            "actor_id": 10,
            "actor_login": "alice",
            "org_id": 501,
            "org_login": "acme",
            "org_url": "https://api.github.com/orgs/acme",
            "org_avatar_url": "https://example.test/acme.png",
            "is_public": True,
            "payload_action": None,
            "payload_ref_type": None,
            "pull_request_id": None,
            "pull_request_merged": True,
            "issue_id": 9001,
            "comment_id": 9101,
            "release_id": 9201,
            "member_id": None,
            "ingestion_ts": "2026-03-20T21:10:00Z",
            "source_file": "source-1",
        },
        {
            "event_id": 2,
            "event_type": "IssuesEvent",
            "created_at": "2026-03-20T21:05:00Z",
            "repo_id": 101,
            "repo_full_name": "acme/widgets",
            "actor_id": 11,
            "actor_login": "helper[bot]",
            "org_id": 501,
            "org_login": "acme",
            "org_url": "https://api.github.com/orgs/acme",
            "org_avatar_url": "https://example.test/acme.png",
            "is_public": False,
            "payload_action": None,
            "payload_ref_type": None,
            "pull_request_id": None,
            "pull_request_merged": False,
            "issue_id": 9002,
            "comment_id": None,
            "release_id": None,
            "member_id": None,
            "ingestion_ts": "2026-03-20T21:10:00Z",
            "source_file": "source-2",
        },
        {
            "event_id": 3,
            "event_type": "PushEvent",
            "created_at": "2026-03-20T21:07:00Z",
            "repo_id": 202,
            "repo_full_name": "invalid-name",
            "actor_id": 12,
            "actor_login": "bob",
            "org_id": 502,
            "org_login": "invalid-org",
            "org_url": "https://api.github.com/orgs/invalid-org",
            "org_avatar_url": "https://example.test/invalid-org.png",
            "is_public": True,
            "payload_action": None,
            "payload_ref_type": None,
            "pull_request_id": None,
            "pull_request_merged": None,
            "issue_id": None,
            "comment_id": None,
            "release_id": None,
            "member_id": None,
            "ingestion_ts": "2026-03-20T21:10:00Z",
            "source_file": "source-3",
        },
        {
            "event_id": 4,
            "event_type": "PushEvent",
            "created_at": "2026-03-20T21:08:00Z",
            "repo_id": None,
            "repo_full_name": None,
            "actor_id": 13,
            "actor_login": "charlie",
            "org_id": 503,
            "org_login": "null-repo-org",
            "org_url": "https://api.github.com/orgs/null-repo-org",
            "org_avatar_url": "https://example.test/null-repo-org.png",
            "is_public": True,
            "payload_action": None,
            "payload_ref_type": None,
            "pull_request_id": None,
            "pull_request_merged": None,
            "issue_id": None,
            "comment_id": None,
            "release_id": None,
            "member_id": None,
            "ingestion_ts": "2026-03-20T21:10:00Z",
            "source_file": "source-4",
        },
    ]


@pytest.fixture
def org_aggregate_input_rows() -> list[dict]:
    return [
        {
            "event_id": 1,
            "event_type": "PushEvent",
            "created_at": "2026-03-20T21:00:00Z",
            "repo_id": 101,
            "repo_full_name": "acme/widgets",
            "actor_id": 10,
            "actor_login": "alice",
            "org_id": 501,
            "org_login": "acme",
            "org_url": "https://api.github.com/orgs/acme",
            "org_avatar_url": "https://example.test/acme.png",
            "is_public": True,
            "payload_action": None,
            "payload_ref_type": None,
            "pull_request_id": None,
            "pull_request_merged": False,
            "issue_id": None,
            "comment_id": None,
            "release_id": None,
            "member_id": None,
            "ingestion_ts": "2026-03-20T21:10:00Z",
            "source_file": "source-1",
        },
        {
            "event_id": 2,
            "event_type": "WatchEvent",
            "created_at": "2026-03-20T21:01:00Z",
            "repo_id": 102,
            "repo_full_name": "acme/gadgets",
            "actor_id": 11,
            "actor_login": "bob",
            "org_id": 501,
            "org_login": "acme",
            "org_url": "https://api.github.com/orgs/acme",
            "org_avatar_url": "https://example.test/acme.png",
            "is_public": True,
            "payload_action": None,
            "payload_ref_type": None,
            "pull_request_id": None,
            "pull_request_merged": False,
            "issue_id": None,
            "comment_id": None,
            "release_id": None,
            "member_id": None,
            "ingestion_ts": "2026-03-20T21:10:00Z",
            "source_file": "source-2",
        },
        {
            "event_id": 3,
            "event_type": "PushEvent",
            "created_at": "2026-03-20T21:02:00Z",
            "repo_id": 103,
            "repo_full_name": "missing/org",
            "actor_id": 12,
            "actor_login": "carol",
            "org_id": None,
            "org_login": None,
            "org_url": None,
            "org_avatar_url": None,
            "is_public": True,
            "payload_action": None,
            "payload_ref_type": None,
            "pull_request_id": None,
            "pull_request_merged": False,
            "issue_id": None,
            "comment_id": None,
            "release_id": None,
            "member_id": None,
            "ingestion_ts": "2026-03-20T21:10:00Z",
            "source_file": "source-3",
        },
    ]


@pytest.mark.unit
class TestBuildAggregatesServiceUnit:
    def test_camel_to_snake_converts_event_name(self):
        assert camel_to_snake("PullRequestReviewEvent") == "pull_request_review_event"
        assert camel_to_snake("PushEvent") == "push_event"

    def test_to_s3a_converts_s3_scheme(self):
        assert _to_s3a("s3://bucket/key") == "s3a://bucket/key"
        assert _to_s3a("s3a://bucket/key") == "s3a://bucket/key"
        assert _to_s3a("/tmp/input.parquet") == "/tmp/input.parquet"

    def test_post_init_reads_input_parquet(
        self,
        spark,
        tmp_path: Path,
        repo_aggregate_input_rows: list[dict],
    ):
        input_path = _write_input_parquet(
            spark, tmp_path, repo_aggregate_input_rows[:1]
        )

        service = BuildAggregates(
            spark=spark,
            input_path=input_path,
            output_path=str(tmp_path / "output"),
            dt="2026-03-20",
            hr="21",
        )

        assert service.df.count() == 1
        assert set(service.df.columns) >= {"repo_id", "repo_full_name", "event_type"}

    def test_repositories_writes_filtered_aggregates(
        self,
        spark,
        tmp_path: Path,
        repo_aggregate_input_rows: list[dict],
    ):
        input_path = _write_input_parquet(spark, tmp_path, repo_aggregate_input_rows)
        output_path = tmp_path / "repo-output"

        service = BuildAggregates(
            spark=spark,
            input_path=input_path,
            output_path=str(output_path),
            dt="2026-03-20",
            hr="21",
        )

        service.repositories()

        rows = spark.read.parquet(str(output_path)).collect()
        assert len(rows) == 1

        row = rows[0]
        assert row.repo_id == 101
        assert row.repo_full_name == "acme/widgets"
        assert row.total_events == 2
        assert row.public_events_count == 1
        assert row.unique_actors == 2
        assert row.bot_events == 1
        assert row.issues_count == 2
        assert row.comments_count == 1
        assert row.releases_count == 1
        assert row.merged_pr_count == 1
        assert row.push_events == 1
        assert row.issues_events == 1
        assert row.push_events_ratio == pytest.approx(0.5)
        assert row.issues_events_ratio == pytest.approx(0.5)
        assert row.bot_ratio == pytest.approx(0.5)
        assert row.dt == date(2026, 3, 20)
        assert row.hr == 21

    def test_organizations_writes_filtered_aggregates(
        self,
        spark,
        tmp_path: Path,
        org_aggregate_input_rows: list[dict],
    ):
        input_path = _write_input_parquet(spark, tmp_path, org_aggregate_input_rows)
        output_path = tmp_path / "org-output"

        service = BuildAggregates(
            spark=spark,
            input_path=input_path,
            output_path=str(output_path),
            dt="2026-03-20",
            hr="21",
        )

        service.organizations()

        rows = spark.read.parquet(str(output_path)).collect()
        assert len(rows) == 1

        row = rows[0]
        assert row.org_id == 501
        assert row.org_login == "acme"
        assert row.total_events == 2
        assert row.public_events_count == 2
        assert row.asDict()["repos_count"] == 2
        assert row.unique_actors == 2
        assert row.push_events == 1
        assert row.watch_events == 1
        assert row.push_events_ratio == pytest.approx(0.5)
        assert row.watch_events_ratio == pytest.approx(0.5)
        assert row.dt == date(2026, 3, 20)
        assert row.hr == 21

    def test_main_dispatches_repo_aggregates(self, monkeypatch):
        args = Namespace(
            input_path="s3://bronze/input/",
            output_path="s3://silver/output/",
            type="repo",
            dt="2026-03-20",
            hr="21",
        )
        parser_mock = Mock()
        parser_mock.parse_args.return_value = args
        monkeypatch.setattr(
            "gba.services.build_aggregates.argparse.ArgumentParser",
            Mock(return_value=parser_mock),
        )

        spark = Mock()
        builder = Mock()
        builder.appName.return_value.getOrCreate.return_value = spark
        monkeypatch.setattr(
            "gba.services.build_aggregates.SparkSession",
            Mock(builder=builder),
        )

        service = Mock(spec=BuildAggregates)
        service_ctor = Mock(return_value=service)
        monkeypatch.setattr(
            "gba.services.build_aggregates.BuildAggregates", service_ctor
        )

        main()

        service_ctor.assert_called_once_with(
            spark=spark,
            input_path="s3a://bronze/input/",
            output_path="s3a://silver/output/",
            dt="2026-03-20",
            hr="21",
        )
        service.repositories.assert_called_once_with()
        service.organizations.assert_not_called()
        spark.stop.assert_called_once_with()

    def test_main_dispatches_org_aggregates(self, monkeypatch):
        args = Namespace(
            input_path="s3://bronze/input/",
            output_path="s3://silver/output/",
            type=CandidatesType.ORG.value,
            dt="2026-03-20",
            hr="21",
        )
        parser_mock = Mock()
        parser_mock.parse_args.return_value = args
        monkeypatch.setattr(
            "gba.services.build_aggregates.argparse.ArgumentParser",
            Mock(return_value=parser_mock),
        )

        spark = Mock()
        builder = Mock()
        builder.appName.return_value.getOrCreate.return_value = spark
        monkeypatch.setattr(
            "gba.services.build_aggregates.SparkSession",
            Mock(builder=builder),
        )

        service = Mock(spec=BuildAggregates)
        monkeypatch.setattr(
            "gba.services.build_aggregates.BuildAggregates",
            Mock(return_value=service),
        )

        main()

        service.organizations.assert_called_once_with()
        service.repositories.assert_not_called()
        spark.stop.assert_called_once_with()
