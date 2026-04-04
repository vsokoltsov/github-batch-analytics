from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest

from gba.services.build_candidates import BuildCandidates

COUNT_METRICS = [
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
]

RATIO_METRICS = [
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


def _write_parquet(spark, tmp_path: Path, rows: list[dict]) -> str:
    input_path = tmp_path / "input"
    spark.createDataFrame(rows).write.mode("overwrite").parquet(str(input_path))
    return str(input_path)


def _base_repo_row(
    repo_id: int | None,
    repo_full_name: str | None,
    base_score: int,
    **overrides,
) -> dict:
    row = {
        "repo_id": repo_id,
        "repo_full_name": repo_full_name,
        "last_event_at": datetime(2026, 3, 30, 10, 0, 0),
        "dt": date(2026, 3, 30),
        "hr": 10,
    }
    row.update({metric: base_score for metric in COUNT_METRICS})
    row.update({metric: float(base_score) for metric in RATIO_METRICS})
    row.update(overrides)
    return row


def _base_org_row(
    org_id: int | None,
    org_login: str | None,
    base_score: int,
    **overrides,
) -> dict:
    row = {
        "org_id": org_id,
        "org_login": org_login,
        "last_event_at": datetime(2026, 3, 30, 10, 0, 0),
        "dt": date(2026, 3, 30),
        "hr": 10,
        "repos_count": base_score,
    }
    row.update({metric: base_score for metric in COUNT_METRICS})
    row.update({metric: float(base_score) for metric in RATIO_METRICS})
    row.update(overrides)
    return row


@pytest.mark.unit
class TestBuildCandidatesServiceUnit:
    def test_repositories_selects_top_candidates_and_keeps_metrics(
        self, spark, tmp_path: Path
    ):
        input_path = _write_parquet(
            spark,
            tmp_path,
            [
                _base_repo_row(1, "acme/core", 10),
                _base_repo_row(
                    2,
                    "acme/ui",
                    5,
                    watch_events=20,
                    watch_events_ratio=20.0,
                ),
                _base_repo_row(
                    3,
                    "acme/data",
                    1,
                    fork_events=30,
                    fork_events_ratio=30.0,
                ),
                _base_repo_row(
                    4,
                    "invalid-name",
                    1000,
                    total_events=1000,
                    composite_score=1000.0,
                ),
                _base_repo_row(
                    None,
                    None,
                    2000,
                    total_events=2000,
                    composite_score=2000.0,
                ),
            ],
        )
        output_path = tmp_path / "repo-candidates"

        service = BuildCandidates(
            spark=spark,
            input_path=input_path,
            output_path=str(output_path),
            top_n=1,
        )

        service.repositories()

        rows = spark.read.parquet(str(output_path)).orderBy("repo_id").collect()

        assert [row.repo_id for row in rows] == [1, 2, 3]

        by_repo = {row.repo_id: row.asDict(recursive=True) for row in rows}

        assert by_repo[1]["repo_full_name"] == "acme/core"
        assert by_repo[1]["total_events"] == 10
        assert "total_events" in by_repo[1]["selection_reasons"]
        assert "composite_score" in by_repo[1]["selection_reasons"]

        assert by_repo[2]["watch_events"] == 20
        assert by_repo[2]["watch_events_ratio"] == pytest.approx(20.0)
        assert by_repo[2]["selection_reasons"] == [
            "watch_events",
            "watch_events_ratio",
        ]

        assert by_repo[3]["fork_events"] == 30
        assert by_repo[3]["fork_events_ratio"] == pytest.approx(30.0)
        assert by_repo[3]["selection_reasons"] == [
            "fork_events",
            "fork_events_ratio",
        ]

    def test_organizations_selects_top_candidates_and_keeps_repos_count(
        self, spark, tmp_path: Path
    ):
        input_path = _write_parquet(
            spark,
            tmp_path,
            [
                _base_org_row(10, "acme", 10),
                _base_org_row(
                    20,
                    "globex",
                    5,
                    repos_count=50,
                    watch_events=20,
                    watch_events_ratio=20.0,
                ),
                _base_org_row(
                    30,
                    "initech",
                    1,
                    fork_events=30,
                    fork_events_ratio=30.0,
                ),
                _base_org_row(
                    None,
                    None,
                    1000,
                    repos_count=1000,
                    composite_score=1000.0,
                ),
            ],
        )
        output_path = tmp_path / "org-candidates"

        service = BuildCandidates(
            spark=spark,
            input_path=input_path,
            output_path=str(output_path),
            top_n=1,
        )

        service.organizations()

        rows = spark.read.parquet(str(output_path)).orderBy("org_id").collect()

        assert [row.org_id for row in rows] == [10, 20, 30]

        by_org = {row.org_id: row.asDict(recursive=True) for row in rows}

        assert by_org[10]["org_login"] == "acme"
        assert by_org[10]["total_events"] == 10
        assert "total_events" in by_org[10]["selection_reasons"]

        assert by_org[20]["repos_count"] == 50
        assert by_org[20]["watch_events"] == 20
        assert by_org[20]["selection_reasons"] == [
            "repos_count",
            "watch_events",
            "watch_events_ratio",
        ]

        assert by_org[30]["fork_events"] == 30
        assert by_org[30]["selection_reasons"] == [
            "fork_events",
            "fork_events_ratio",
        ]

    def test_organizations_skips_missing_metric_columns(
        self,
        spark,
        tmp_path: Path,
    ):
        input_path = _write_parquet(
            spark,
            tmp_path,
            [
                {
                    "org_id": 10,
                    "org_login": "acme",
                    "total_events": 10,
                    "public_events_count": 8,
                    "unique_actors": 4,
                    "bot_events": 1,
                    "push_events": 7,
                    "push_events_ratio": 0.7,
                    "composite_score": 2.5,
                    "bot_ratio": 0.1,
                    "repos_count": 3,
                    "last_event_at": datetime(2026, 3, 30, 10, 0, 0),
                    "dt": date(2026, 3, 30),
                    "hr": 10,
                }
            ],
        )
        output_path = tmp_path / "org-candidates-sparse"

        service = BuildCandidates(
            spark=spark,
            input_path=input_path,
            output_path=str(output_path),
            top_n=1,
        )

        service.organizations()

        rows = spark.read.parquet(str(output_path)).collect()

        assert len(rows) == 1
        assert rows[0].org_id == 10
        assert rows[0].selection_reasons == [
            "bot_events",
            "bot_ratio",
            "composite_score",
            "public_events_count",
            "push_events",
            "push_events_ratio",
            "repos_count",
            "total_events",
            "unique_actors",
        ]
