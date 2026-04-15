from __future__ import annotations

import os


def get_dashboard_config() -> dict[str, str]:
    config = {
        "aws_region": os.getenv("AWS_REGION", "eu-central-1"),
        "aws_profile": os.getenv("AWS_PROFILE", ""),
        "athena_database_name": os.getenv("ATHENA_DATABASE_NAME", "github_analytics"),
        "athena_workgroup_name": os.getenv(
            "ATHENA_WORKGROUP_NAME", "github-batch-analytics"
        ),
        "athena_query_results_bucket_name": os.getenv(
            "ATHENA_QUERY_RESULTS_BUCKET_NAME"
        ),
        "athena_repository_summary_table_name": os.getenv(
            "ATHENA_REPOSITORY_SUMMARY_TABLE_NAME",
            "repository_dashboard_summary",
        ),
    }

    missing = [key for key in ["athena_query_results_bucket_name"] if not config[key]]
    if missing:
        raise RuntimeError(
            "Missing dashboard environment variables: " + ", ".join(missing)
        )

    return {key: value for key, value in config.items() if value is not None}
