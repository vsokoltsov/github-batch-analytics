from __future__ import annotations

from functools import lru_cache

import pandas as pd
from pyathena import connect

from dashboards.config import get_dashboard_config


@lru_cache(maxsize=32)
def load_repository_summary(start_date: str, end_date: str) -> pd.DataFrame:
    config = get_dashboard_config()
    connection = connect(
        region_name=config["aws_region"],
        s3_staging_dir=(
            f"s3://{config['athena_query_results_bucket_name']}/dash-query-results/"
        ),
        schema_name=config["athena_database_name"],
        work_group=config["athena_workgroup_name"],
        profile_name=config.get("aws_profile") or None,
    )

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
    dataframe["dt"] = pd.to_datetime(dataframe["dt"]).dt.date
    dataframe["hr"] = dataframe["hr"].astype(int)
    dataframe["timestamp"] = pd.to_datetime(
        dataframe["dt"].astype(str)
        + " "
        + dataframe["hr"].astype(str).str.zfill(2)
        + ":00:00"
    )
    dataframe["language"] = dataframe["language"].fillna("Unknown")
    dataframe["repo_full_name"] = dataframe["repo_full_name"].fillna(
        "Unknown repository"
    )
    return dataframe
