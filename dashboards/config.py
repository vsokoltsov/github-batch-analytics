from __future__ import annotations

import os

import streamlit as st


def _read_setting(name: str, default: str | None = None) -> str | None:
    if name in st.secrets:
        return str(st.secrets[name])
    return os.getenv(name, default)


def get_dashboard_config() -> dict[str, str]:
    config = {
        "aws_region": _read_setting("AWS_REGION", "eu-central-1"),
        "aws_profile": _read_setting("AWS_PROFILE", ""),
        "aws_access_key_id": _read_setting("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": _read_setting("AWS_SECRET_ACCESS_KEY"),
        "aws_session_token": _read_setting("AWS_SESSION_TOKEN", ""),
        "athena_database_name": _read_setting(
            "ATHENA_DATABASE_NAME", "github_analytics"
        ),
        "athena_workgroup_name": _read_setting(
            "ATHENA_WORKGROUP_NAME", "github-batch-analytics"
        ),
        "athena_query_results_bucket_name": _read_setting(
            "ATHENA_QUERY_RESULTS_BUCKET_NAME"
        ),
        "athena_repository_summary_table_name": _read_setting(
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
