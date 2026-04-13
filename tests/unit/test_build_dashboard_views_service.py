from __future__ import annotations

from pathlib import Path

import pytest

from gba.services.build_dashboard_views import (
    OrgDashboardView,
    RepoDashboardView,
    RepoOrgDashboardView,
)


def _write_parquet(spark, path: Path, rows: list[dict]) -> str:
    spark.createDataFrame(rows).write.mode("overwrite").parquet(str(path))
    return str(path)


def test_repo_dashboard_view_builds_summary_dataset(spark, tmp_path: Path) -> None:
    repo_input = _write_parquet(
        spark,
        tmp_path / "repo_input",
        [
            {
                "repo_id": 1,
                "repo_full_name": "acme/repo-one",
                "repo_name": "repo-one",
                "owner_login": "acme",
                "owner_type": "Organization",
                "language": "Python",
                "visibility": "public",
                "is_fork": False,
                "is_archived": False,
                "is_disabled": False,
                "stargazers_count": 10,
                "forks_count": 2,
                "watchers_count": 10,
                "subscribers_count": 5,
                "open_issues_count": 1,
                "total_events": 12,
                "push_events": 7,
                "pull_request_events": 3,
                "issue_comment_events": 1,
                "fork_events": 1,
                "composite_score": 3.5,
                "bot_ratio": 0.1,
            },
            {
                "repo_id": 1,
                "repo_full_name": "acme/repo-one",
                "repo_name": "repo-one",
                "owner_login": "acme",
                "owner_type": "Organization",
                "language": "Python",
                "visibility": "public",
                "is_fork": False,
                "is_archived": False,
                "is_disabled": False,
                "stargazers_count": 10,
                "forks_count": 2,
                "watchers_count": 10,
                "subscribers_count": 5,
                "open_issues_count": 1,
                "total_events": 8,
                "push_events": 5,
                "pull_request_events": 2,
                "issue_comment_events": 0,
                "fork_events": 0,
                "composite_score": 4.5,
                "bot_ratio": 0.3,
            },
        ],
    )
    output_path = tmp_path / "repo_summary"

    RepoDashboardView(
        spark=spark,
        input_path=repo_input,
        output_path=str(output_path),
        sql_file="repositories/summary.sql",
    ).build()

    result = spark.read.parquet(str(output_path))
    row = result.collect()[0].asDict()

    assert result.count() == 1
    assert row["repo_id"] == 1
    assert row["repo_full_name"] == "acme/repo-one"
    assert row["repo_name"] == "repo-one"
    assert row["total_events"] == 20
    assert row["push_events"] == 12
    assert row["pull_request_events"] == 5
    assert row["issue_comment_events"] == 1
    assert row["fork_events"] == 1
    assert row["avg_composite_score"] == pytest.approx(4.0)
    assert row["avg_bot_ratio"] == pytest.approx(0.2)


def test_org_dashboard_view_builds_summary_dataset(spark, tmp_path: Path) -> None:
    org_input = _write_parquet(
        spark,
        tmp_path / "org_input",
        [
            {
                "org_id": 10,
                "org_login": "acme",
                "org_name": "Acme Org",
                "location": "Berlin",
                "company": "Acme",
                "blog": "https://acme.example",
                "email": "hello@acme.example",
                "twitter_username": "acme",
                "is_verified": True,
                "has_organization_projects": True,
                "has_repository_projects": False,
                "public_repos": 10,
                "public_gists": 1,
                "followers": 100,
                "following": 5,
                "total_events": 20,
                "push_events": 8,
                "pull_request_events": 4,
                "composite_score": 2.0,
                "bot_ratio": 0.2,
            },
            {
                "org_id": 10,
                "org_login": "acme",
                "org_name": "Acme Org",
                "location": "Berlin",
                "company": "Acme",
                "blog": "https://acme.example",
                "email": "hello@acme.example",
                "twitter_username": "acme",
                "is_verified": True,
                "has_organization_projects": True,
                "has_repository_projects": False,
                "public_repos": 10,
                "public_gists": 1,
                "followers": 100,
                "following": 5,
                "total_events": 5,
                "push_events": 2,
                "pull_request_events": 1,
                "composite_score": 4.0,
                "bot_ratio": 0.4,
            },
        ],
    )
    output_path = tmp_path / "org_summary"

    OrgDashboardView(
        spark=spark,
        input_path=org_input,
        output_path=str(output_path),
        sql_file="organizations/summary.sql",
    ).build()

    result = spark.read.parquet(str(output_path))
    row = result.collect()[0].asDict()

    assert result.count() == 1
    assert row["org_id"] == 10
    assert row["org_login"] == "acme"
    assert row["org_name"] == "Acme Org"
    assert row["total_events"] == 25
    assert row["push_events"] == 10
    assert row["pull_request_events"] == 5
    assert row["avg_composite_score"] == pytest.approx(3.0)
    assert row["avg_bot_ratio"] == pytest.approx(0.3)


def test_repo_org_dashboard_view_builds_common_rollup(spark, tmp_path: Path) -> None:
    repo_input = _write_parquet(
        spark,
        tmp_path / "repo_input",
        [
            {
                "repo_id": 1,
                "repo_full_name": "acme/repo-one",
                "repo_name": "repo-one",
                "owner_login": "acme",
                "language": "Python",
                "total_events": 15,
                "composite_score": 3.0,
                "stargazers_count": 20,
            },
            {
                "repo_id": 1,
                "repo_full_name": "acme/repo-one",
                "repo_name": "repo-one",
                "owner_login": "acme",
                "language": "Python",
                "total_events": 5,
                "composite_score": 5.0,
                "stargazers_count": 20,
            },
        ],
    )
    org_input = _write_parquet(
        spark,
        tmp_path / "org_input",
        [
            {
                "org_id": 10,
                "org_login": "acme",
                "org_name": "Acme Org",
                "location": "Berlin",
                "company": "Acme",
                "followers": 100,
                "public_repos": 10,
                "is_verified": True,
            }
        ],
    )
    output_path = tmp_path / "common_rollup"

    RepoOrgDashboardView(
        spark=spark,
        repo_path=repo_input,
        org_path=org_input,
        output_path=str(output_path),
        sql_file="common/rollup.sql",
    ).build()

    result = spark.read.parquet(str(output_path))
    row = result.collect()[0].asDict()

    assert result.count() == 1
    assert row["repo_id"] == 1
    assert row["repo_full_name"] == "acme/repo-one"
    assert row["repo_total_events"] == 20
    assert row["repo_avg_composite_score"] == pytest.approx(4.0)
    assert row["org_id"] == 10
    assert row["org_name"] == "Acme Org"
    assert row["org_location"] == "Berlin"
    assert row["org_company"] == "Acme"
    assert row["org_followers"] == 100
    assert row["org_public_repos"] == 10
    assert row["org_is_verified"] is True
