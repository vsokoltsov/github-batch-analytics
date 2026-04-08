from __future__ import annotations

from pathlib import Path

from gba.services.build_curated_marts import BuildMarts


def _write_parquet(spark, path: Path, rows: list[dict]) -> str:
    spark.createDataFrame(rows).write.mode("overwrite").parquet(str(path))
    return str(path)


def test_build_repo_marts_joins_and_deduplicates_overlapping_columns(
    spark, tmp_path: Path
) -> None:
    candidates_path = _write_parquet(
        spark,
        tmp_path / "repo_candidates",
        [
            {
                "repo_id": 1,
                "repo_full_name": "acme/repo-one",
                "selection_reasons": ["merged_pr_count"],
                "dt": "2026-04-06",
                "hr": 13,
            }
        ],
    )
    snapshot_path = _write_parquet(
        spark,
        tmp_path / "repo_snapshot",
        [
            {
                "repo_id": 1,
                "repo_full_name": "acme/repo-one",
                "repo_name": "repo-one",
                "owner_login": "acme",
                "language": "Python",
                "dt": "2026-04-06",
                "hr": 13,
            }
        ],
    )
    output_path = tmp_path / "repo_output"

    BuildMarts(
        spark=spark,
        candidates_path=candidates_path,
        github_snapshot_path=snapshot_path,
        output_path=str(output_path),
    ).repositories()

    result = spark.read.parquet(str(output_path))

    assert result.columns.count("repo_id") == 1
    assert result.columns.count("repo_full_name") == 1
    assert result.columns.count("dt") == 1
    assert result.columns.count("hr") == 1
    assert set(result.columns) >= {
        "repo_id",
        "repo_full_name",
        "selection_reasons",
        "repo_name",
        "owner_login",
        "language",
        "dt",
        "hr",
    }

    row = result.collect()[0].asDict()
    assert row["repo_id"] == 1
    assert row["repo_full_name"] == "acme/repo-one"
    assert row["repo_name"] == "repo-one"
    assert row["owner_login"] == "acme"
    assert row["language"] == "Python"
    assert row["dt"] == "2026-04-06"
    assert row["hr"] == 13


def test_build_org_marts_keeps_candidate_partition_columns_and_left_join_rows(
    spark, tmp_path: Path
) -> None:
    candidates_path = _write_parquet(
        spark,
        tmp_path / "org_candidates",
        [
            {
                "org_id": 10,
                "org_login": "acme",
                "selection_reasons": ["public_repos"],
                "dt": "2026-04-06",
                "hr": 13,
            },
            {
                "org_id": 11,
                "org_login": "missing-org",
                "selection_reasons": ["public_repos"],
                "dt": "2026-04-06",
                "hr": 13,
            },
        ],
    )
    snapshot_path = _write_parquet(
        spark,
        tmp_path / "org_snapshot",
        [
            {
                "org_id": 10,
                "org_login": "acme",
                "org_name": "Acme Org",
                "description": "Acme",
                "dt": "2026-04-06",
                "hr": 13,
            }
        ],
    )
    output_path = tmp_path / "org_output"

    BuildMarts(
        spark=spark,
        candidates_path=candidates_path,
        github_snapshot_path=snapshot_path,
        output_path=str(output_path),
    ).organizations()

    result = spark.read.parquet(str(output_path)).orderBy("org_id")

    assert result.columns.count("org_id") == 1
    assert result.columns.count("org_login") == 1
    assert result.columns.count("dt") == 1
    assert result.columns.count("hr") == 1

    rows = [row.asDict() for row in result.collect()]

    assert rows[0]["org_id"] == 10
    assert rows[0]["org_name"] == "Acme Org"
    assert rows[0]["dt"] == "2026-04-06"
    assert rows[0]["hr"] == 13

    assert rows[1]["org_id"] == 11
    assert rows[1]["org_login"] == "missing-org"
    assert rows[1]["org_name"] is None
