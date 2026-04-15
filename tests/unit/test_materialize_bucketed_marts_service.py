from __future__ import annotations

from unittest.mock import MagicMock, patch

from gba.services.materialize_bucketed_marts import (
    BucketedMartConfig,
    build_bucketed_ctas_query,
    build_drop_table_query,
    normalize_bucketed_mart_config,
    submit_bucketed_mart_ctas,
)


def test_build_bucketed_ctas_query_for_repositories() -> None:
    config = BucketedMartConfig(
        database_name="github_analytics",
        source_table_name="repositories",
        target_table_name="repositories",
        query_results_bucket_name="athena-results",
        marts_bucket_name="marts-bucket",
        dt="2026-04-15",
        hr="10",
        bucket_column="repo_id",
        bucket_count=16,
    )

    query = build_bucketed_ctas_query(
        config=config,
        temp_table_name="tmp_repositories_20260415_10_deadbeef",
        select_columns=["repo_id", "repo_full_name", "selection_reasons"],
    )

    assert '"github_analytics"."tmp_repositories_20260415_10_deadbeef"' in query
    assert '"github_analytics"."repositories_stage"' in query
    assert "external_location = 's3://marts-bucket/repositories/'" in query
    assert "bucketed_by = ARRAY['repo_id']" in query
    assert "bucket_count = 16" in query
    assert "DATE '2026-04-15' AS dt" in query
    assert "10 AS hr" in query


def test_submit_bucketed_mart_ctas_accepts_dict_config() -> None:
    session = MagicMock()
    athena_client = MagicMock()
    glue_client = MagicMock()
    s3_client = MagicMock()

    session.client.side_effect = lambda name: {
        "athena": athena_client,
        "glue": glue_client,
        "s3": s3_client,
    }[name]
    glue_client.get_table.return_value = {
        "Table": {
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "org_id"},
                    {"Name": "org_name"},
                ]
            }
        }
    }
    athena_client.start_query_execution.return_value = {"QueryExecutionId": "query-123"}
    s3_client.get_paginator.return_value.paginate.return_value = []

    with patch("boto3.session.Session", return_value=session):
        result = submit_bucketed_mart_ctas(
            aws_region="eu-central-1",
            workgroup_name="github-batch-analytics",
            config={
                "database_name": "github_analytics",
                "source_table_name": "organizations",
                "target_table_name": "organizations",
                "query_results_bucket_name": "athena-results",
                "marts_bucket_name": "marts-bucket",
                "dt": "2026-04-15",
                "hr": "10",
                "bucket_column": "org_id",
                "bucket_count": 8,
            },
        )

    assert result["query_execution_id"] == "query-123"
    assert (
        result["output_path"] == "s3a://marts-bucket/organizations/dt=2026-04-15/hr=10/"
    )


def test_build_drop_table_query_uses_plain_qualified_name() -> None:
    assert (
        build_drop_table_query(
            "github_analytics", "tmp_organizations_20260415_12_deadbeef"
        )
        == "DROP TABLE IF EXISTS github_analytics.tmp_organizations_20260415_12_deadbeef"
    )


def test_normalize_bucketed_mart_config_from_dict() -> None:
    config = normalize_bucketed_mart_config(
        {
            "database_name": "github_analytics",
            "source_table_name": "organizations",
            "target_table_name": "organizations",
            "query_results_bucket_name": "athena-results",
            "marts_bucket_name": "marts-bucket",
            "dt": "2026-04-15",
            "hr": 10,
            "bucket_column": "org_id",
            "bucket_count": "8",
        }
    )

    assert config.hr == "10"
    assert config.bucket_count == 8
