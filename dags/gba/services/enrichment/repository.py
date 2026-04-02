from __future__ import annotations
from typing import Any, Dict, Iterator, cast

import logging

import dlt
import numpy as np
import pandas as pd
from dlt.destinations import filesystem
from dlt.extract.state import resource_state
from gba.services.enrichment.client import get_github_client
from gba.services.utils import to_s3
from gba.services.enrichment.errors import GitHubRateLimitExceeded

logger = logging.getLogger(__name__)


def _normalize_candidate_value(value: object) -> object:
    if isinstance(value, np.ndarray):
        return cast(Any, value).tolist()
    if isinstance(value, np.generic):
        return value.item()
    return value


@dlt.resource(
    name="repo_candidates",
    write_disposition="replace",
    primary_key="repo_full_name",
)
def repo_candidates_resource(candidate_path: str) -> Iterator[dict]:
    candidates = pd.read_parquet(to_s3(candidate_path))
    rows = candidates.to_dict(orient="records")
    total = len(rows)
    for idx, row in enumerate(rows, start=1):
        if idx == 1 or idx % 25 == 0 or idx == total:
            logger.info(
                "Repo enrichment progress: %d/%d processed, %d remaining",
                idx,
                total,
                total - idx,
            )
        yield {key: _normalize_candidate_value(value) for key, value in row.items()}


@dlt.transformer(
    data_from=repo_candidates_resource,
    name="github_repo_api_response",
    selected=False,
)
def github_repo_api_response(
    candidate: dict,
    github_token: str,
    dt: str,
    hr: int,
) -> Iterator[dict]:
    state = resource_state()
    repo_full_name = candidate["repo_full_name"]
    seen_key = f"repo::{repo_full_name}::{dt}::{hr}"
    if state.get(seen_key):
        return

    client = get_github_client(github_token)
    owner, repo = repo_full_name.split("/", 1)
    fetched_at = pd.Timestamp.utcnow().isoformat()

    response = client.get(f"/repos/{owner}/{repo}")
    payload = response.json()

    if response.status_code == 200:
        state[seen_key] = True

    yield {
        "candidate": candidate,
        "repo_full_name": repo_full_name,
        "status_code": response.status_code,
        "payload": payload,
        "fetched_at": fetched_at,
        "retry_after": response.headers.get("retry-after"),
        "reset_at": response.headers.get("x-ratelimit-reset"),
        "remaining": response.headers.get("x-ratelimit-remaining"),
        "response_headers": dict(response.headers),
        "response_text": response.text,
        "dt": dt,
        "hr": hr,
    }


@dlt.transformer(
    data_from=github_repo_api_response,
    name="github_repo_snapshot",
    write_disposition="merge",
    primary_key="repo_id",
)
def github_repo_snapshot(
    repo_response: dict,
) -> Iterator[Dict[str, Any]]:
    if repo_response["status_code"] != 200:
        return

    payload = repo_response["payload"]

    yield {
        "repo_id": payload.get("id"),
        "repo_full_name": payload.get("full_name"),
        "repo_name": payload.get("name"),
        "owner_login": payload.get("owner", {}).get("login"),
        "owner_id": payload.get("owner", {}).get("id"),
        "owner_type": payload.get("owner", {}).get("type"),
        "description": payload.get("description"),
        "language": payload.get("language"),
        "topics": payload.get("topics"),
        "license_spdx_id": (payload.get("license") or {}).get("spdx_id"),
        "default_branch": payload.get("default_branch"),
        "visibility": payload.get("visibility"),
        "is_fork": payload.get("fork"),
        "is_archived": payload.get("archived"),
        "is_disabled": payload.get("disabled"),
        "stargazers_count": payload.get("stargazers_count"),
        "forks_count": payload.get("forks_count"),
        "watchers_count": payload.get("watchers_count"),
        "subscribers_count": payload.get("subscribers_count"),
        "open_issues_count": payload.get("open_issues_count"),
        "created_at": payload.get("created_at"),
        "updated_at": payload.get("updated_at"),
        "pushed_at": payload.get("pushed_at"),
        "api_fetched_at": repo_response["fetched_at"],
        "dt": repo_response["dt"],
        "hr": repo_response["hr"],
    }


@dlt.transformer(
    data_from=github_repo_api_response,
    name="github_repo_snapshot_errors",
    write_disposition="append",
    primary_key=("repo_full_name", "dt", "hr", "error_status"),
)
def github_repo_snapshot_errors(repo_response: dict) -> Iterator[dict]:
    status_code = repo_response["status_code"]
    repo_full_name = repo_response["repo_full_name"]
    payload = repo_response["payload"]

    match status_code:
        case 200:
            return
        case 404:
            logger.warning("Repo not found, skipping: %s", repo_full_name)
            yield {
                "repo_full_name": repo_full_name,
                "error_status": status_code,
                "error_message": payload.get("message"),
                "api_fetched_at": repo_response["fetched_at"],
                "dt": repo_response["dt"],
                "hr": repo_response["hr"],
            }
        case 401:
            raise RuntimeError("GitHub API authentication failed")
        case 403 | 429:
            logger.info("Overall headers: %s", repo_response["response_headers"])
            raise GitHubRateLimitExceeded(
                f"GitHub API rate limit exceeded for {repo_full_name}; "
                f"remaining={repo_response['remaining']}, "
                f"retry_after={repo_response['retry_after']}, "
                f"reset_at={repo_response['reset_at']}"
            )
        case _:
            raise RuntimeError(
                f"GitHub API failed for repo {repo_full_name}: "
                f"status={status_code}, body={repo_response['response_text']}"
            )


@dlt.transformer(
    data_from=github_repo_snapshot,
    name="github_repo_topics",
    write_disposition="replace",
    primary_key=("repo_id", "topic"),
)
def github_repo_topics(snapshot: dict) -> Iterator[dict]:
    for topic in snapshot.get("topics") or []:
        yield {
            "repo_id": snapshot["repo_id"],
            "repo_full_name": snapshot["repo_full_name"],
            "topic": topic,
            "dt": snapshot["dt"],
            "hr": snapshot["hr"],
        }


@dlt.source
def github_repo_enrichment_source(
    candidate_path: str,
    github_token: str,
    dt: str,
    hr: int,
):
    candidates = repo_candidates_resource(candidate_path)
    responses = candidates | github_repo_api_response(
        github_token=github_token,
        dt=dt,
        hr=hr,
    )
    snapshots = responses | github_repo_snapshot()
    errors = responses | github_repo_snapshot_errors()
    topics = snapshots | github_repo_topics()
    return candidates, responses, snapshots, errors, topics


def run_repo_enrichment_pipeline(
    output_bucket: str, input_path: str, dt: str, hr: int
) -> Dict[str, str]:
    pipeline = dlt.pipeline(
        pipeline_name="github_repo_enrichment",
        pipelines_dir="/tmp/dlt/pipelines",
        destination=filesystem(
            bucket_url=f"s3://{output_bucket}",
            layout="{table_name}/dt={dt}/hr={hr}/{load_id}.{file_id}.{ext}",
            extra_placeholders={
                "dt": dt,
                "hr": str(hr),
            },
        ),
        dataset_name="github_enrichment",
    )

    source = github_repo_enrichment_source(
        candidate_path=input_path,
        github_token=dlt.secrets["sources.github_enrichment.github_token"],
        dt=dt,
        hr=hr,
    )

    extract_info = pipeline.extract(source, loader_file_format="parquet")
    logger.info("extract packages: %s", extract_info)

    normalize_info = pipeline.normalize(workers=1)
    logger.info("normalized: %s", normalize_info)

    load_info = pipeline.load(workers=1)
    logger.info("loaded packages: %s", load_info.load_packages)

    trace = pipeline.last_trace
    if trace:
        pipeline.run([trace], table_name="_trace")

    for package in load_info.load_packages:
        failed_jobs = pipeline.list_failed_jobs_in_package(package.load_id)
        if failed_jobs:
            logger.error("failed jobs in %s: %s", package.load_id, failed_jobs)

    return {
        "repo_candidates_path": f"s3a://{output_bucket}/repo_candidates/dt={dt}/hr={hr}/",
        "repo_snapshot_path": f"s3a://{output_bucket}/github_repo_snapshot/dt={dt}/hr={hr}/",
        "repo_topics_path": f"s3a://{output_bucket}/github_repo_topics/dt={dt}/hr={hr}/",
        "repo_errors_path": f"s3a://{output_bucket}/github_repo_snapshot_errors/dt={dt}/hr={hr}/",
    }
