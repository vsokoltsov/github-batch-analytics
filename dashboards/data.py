from __future__ import annotations

from typing import Any
from functools import lru_cache

import pandas as pd
from pyathena import connect

from dashboards.config import get_dashboard_config


def _connect_to_athena() -> tuple[dict[str, str], Any]:
    config = get_dashboard_config()
    connection_kwargs: dict[str, Any] = {
        "region_name": config["aws_region"],
        "s3_staging_dir": (
            f"s3://{config['athena_query_results_bucket_name']}/dash-query-results/"
        ),
        "schema_name": config["athena_database_name"],
        "work_group": config["athena_workgroup_name"],
    }

    if config.get("aws_access_key_id") and config.get("aws_secret_access_key"):
        connection_kwargs["aws_access_key_id"] = config["aws_access_key_id"]
        connection_kwargs["aws_secret_access_key"] = config["aws_secret_access_key"]
        if config.get("aws_session_token"):
            connection_kwargs["aws_session_token"] = config["aws_session_token"]
    elif config.get("aws_profile"):
        connection_kwargs["profile_name"] = config["aws_profile"]

    return config, connect(**connection_kwargs)


def _normalize_dashboard_time_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe["dt"] = pd.to_datetime(dataframe["dt"]).dt.date
    dataframe["hr"] = dataframe["hr"].astype(int)
    dataframe["timestamp"] = pd.to_datetime(
        dataframe["dt"].astype(str)
        + " "
        + dataframe["hr"].astype(str).str.zfill(2)
        + ":00:00"
    )
    return dataframe


@lru_cache(maxsize=32)
def load_repository_summary(start_date: str, end_date: str) -> pd.DataFrame:
    config, connection = _connect_to_athena()

    query = f"""
    SELECT
        dt,
        hr,
        repo_id,
        repo_full_name,
        repo_name,
        owner_login,
        owner_type,
        language,
        visibility,
        is_fork,
        is_archived,
        is_disabled,
        stargazers_count,
        forks_count,
        watchers_count,
        subscribers_count,
        open_issues_count,
        total_events,
        push_events,
        pull_request_events,
        issue_comment_events,
        fork_events,
        avg_composite_score,
        avg_bot_ratio
    FROM "{config["athena_database_name"]}"."{config["athena_repository_summary_table_name"]}"
    WHERE dt BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    """
    dataframe = pd.read_sql(query, connection)
    dataframe = _normalize_dashboard_time_columns(dataframe)
    dataframe["language"] = dataframe["language"].fillna("Unknown")
    dataframe["repo_full_name"] = dataframe["repo_full_name"].fillna(
        "Unknown repository"
    )
    return dataframe


@lru_cache(maxsize=32)
def load_organization_summary(start_date: str, end_date: str) -> pd.DataFrame:
    config, connection = _connect_to_athena()

    query = f"""
    SELECT
        dt,
        hr,
        org_id,
        org_login,
        org_name,
        location,
        company,
        blog,
        email,
        twitter_username,
        is_verified,
        has_organization_projects,
        has_repository_projects,
        public_repos,
        public_gists,
        followers,
        following,
        total_events,
        push_events,
        pull_request_events,
        avg_composite_score,
        avg_bot_ratio
    FROM "{config["athena_database_name"]}"."{config["athena_organization_summary_table_name"]}"
    WHERE dt BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    """
    dataframe = pd.read_sql(query, connection)
    dataframe = _normalize_dashboard_time_columns(dataframe)
    dataframe["org_login"] = dataframe["org_login"].fillna("Unknown organization")
    dataframe["org_name"] = dataframe["org_name"].fillna("Unknown organization")
    dataframe["location"] = dataframe["location"].fillna("Unknown")
    dataframe["company"] = dataframe["company"].fillna("Unknown")
    return dataframe
