from __future__ import annotations

import logging
from typing import Any, Dict, Iterator, cast

import dlt
import numpy as np
import pandas as pd
from dlt.destinations import filesystem
from dlt.extract.items import DataItemWithMeta
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
    name="org_candidates",
    write_disposition="replace",
    primary_key="org_login",
)
def org_candidates_resource(candidate_path: str) -> Iterator[dict]:
    candidates = pd.read_parquet(to_s3(candidate_path))
    rows = candidates.to_dict(orient="records")
    total = len(rows)

    for idx, row in enumerate(rows, start=1):
        if idx == 1 or idx % 25 == 0 or idx == total:
            logger.info(
                "Organization enrichment progress: %d/%d processed, %d remaining",
                idx,
                total,
                total - idx,
            )
        yield {key: _normalize_candidate_value(value) for key, value in row.items()}


@dlt.transformer(
    data_from=org_candidates_resource,
    name="github_org_api_response",
    selected=False,
)
def github_org_api_response(
    candidate: dict,
    github_token: str,
    dt: str,
    hr: int,
) -> Iterator[dict]:
    state = resource_state()
    org_login = candidate["org_login"]
    seen_key = f"org::{org_login}::{dt}::{hr}"
    if state.get(seen_key):
        return

    client = get_github_client(github_token)
    fetched_at = pd.Timestamp.utcnow().isoformat()

    response = client.get(f"/orgs/{org_login}")
    payload = response.json()

    if response.status_code == 200:
        state[seen_key] = True

    yield {
        "candidate": candidate,
        "org_login": org_login,
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
    data_from=github_org_api_response,
    name="github_org_snapshot",
    write_disposition="merge",
    primary_key="org_id",
)
def github_org_snapshot(org_response: dict) -> Iterator[DataItemWithMeta]:
    if org_response["status_code"] != 200:
        return

    payload = org_response["payload"]

    yield dlt.mark.with_table_name(
        {
            "org_id": payload.get("id"),
            "org_login": payload.get("login"),
            "org_name": payload.get("name"),
            "company": payload.get("company"),
            "blog": payload.get("blog"),
            "location": payload.get("location"),
            "email": payload.get("email"),
            "description": payload.get("description"),
            "twitter_username": payload.get("twitter_username"),
            "is_verified": payload.get("is_verified"),
            "has_organization_projects": payload.get("has_organization_projects"),
            "has_repository_projects": payload.get("has_repository_projects"),
            "public_repos": payload.get("public_repos"),
            "public_gists": payload.get("public_gists"),
            "followers": payload.get("followers"),
            "following": payload.get("following"),
            "html_url": payload.get("html_url"),
            "created_at": payload.get("created_at"),
            "updated_at": payload.get("updated_at"),
            "api_fetched_at": org_response["fetched_at"],
            "dt": org_response["dt"],
            "hr": org_response["hr"],
        },
        "github_org_snapshot",
    )


@dlt.transformer(
    data_from=github_org_api_response,
    name="github_org_snapshot_errors",
    write_disposition="append",
    primary_key=("org_login", "dt", "hr", "error_status"),
)
def github_org_snapshot_errors(org_response: dict) -> Iterator[DataItemWithMeta]:
    status_code = org_response["status_code"]
    org_login = org_response["org_login"]
    payload = org_response["payload"]

    match status_code:
        case 200:
            return
        case 404:
            logger.warning("Organization not found, skipping: %s", org_login)
            yield dlt.mark.with_table_name(
                {
                    "org_login": org_login,
                    "error_status": status_code,
                    "error_message": payload.get("message"),
                    "api_fetched_at": org_response["fetched_at"],
                    "dt": org_response["dt"],
                    "hr": org_response["hr"],
                },
                "github_org_snapshot_errors",
            )
        case 401:
            raise RuntimeError("GitHub API authentication failed")
        case 403 | 429:
            logger.info("Overall headers: %s", org_response["response_headers"])
            raise GitHubRateLimitExceeded(
                f"GitHub API rate limit exceeded for {org_login}; "
                f"remaining={org_response['remaining']}, "
                f"retry_after={org_response['retry_after']}, "
                f"reset_at={org_response['reset_at']}"
            )
        case _:
            raise RuntimeError(
                f"GitHub API failed for organization {org_login}: "
                f"status={status_code}, body={org_response['response_text']}"
            )


@dlt.source
def github_org_enrichment_source(
    candidate_path: str,
    github_token: str,
    dt: str,
    hr: int,
):
    candidates = org_candidates_resource(candidate_path)
    responses = candidates | github_org_api_response(
        github_token=github_token,
        dt=dt,
        hr=hr,
    )
    snapshots = responses | github_org_snapshot()
    errors = responses | github_org_snapshot_errors()
    return candidates, responses, snapshots, errors


def run_org_enrichment_pipeline(
    output_bucket: str,
    input_path: str,
    dt: str,
    hr: int,
) -> Dict[str, str]:
    pipeline = dlt.pipeline(
        pipeline_name="github_org_enrichment",
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

    source = github_org_enrichment_source(
        candidate_path=input_path,
        github_token=dlt.secrets["sources.github_enrichment.github_token"],
        dt=dt,
        hr=hr,
    )

    extract_info = pipeline.extract(data=source, loader_file_format="parquet")
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
        "org_candidates_path": (
            f"s3a://{output_bucket}/org_candidates/dt={dt}/hr={hr}/"
        ),
        "org_snapshot_path": (
            f"s3a://{output_bucket}/github_org_snapshot/dt={dt}/hr={hr}/"
        ),
        "org_errors_path": (
            f"s3a://{output_bucket}/github_org_snapshot_errors/dt={dt}/hr={hr}/"
        ),
    }
