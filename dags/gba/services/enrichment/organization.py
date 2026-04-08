from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, Protocol, cast

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


class DltPipelineRunner(Protocol):
    last_trace: object | None

    def extract(self, data: object, *, loader_file_format: str) -> object: ...

    def normalize(self, workers: int) -> object: ...

    def load(self, workers: int) -> Any: ...

    def run(self, data: object, *, table_name: str) -> object: ...

    def list_failed_jobs_in_package(self, load_id: str) -> list[object]: ...


PipelineFactory = Callable[..., DltPipelineRunner]
SourceFactory = Callable[[str, str, str, int], object]


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
    selected=False,
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
    write_disposition="append",
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


@dataclass
class OrganizationEnrichmentPipeline:
    output_bucket: str
    input_path: str
    dt: str
    hr: int
    github_token: str
    pipeline_name: str = "github_org_enrichment"
    pipelines_dir: str = "/tmp/dlt/pipelines"
    dataset_name: str = "github_enrichment"
    loader_file_format: str = "parquet"
    normalize_workers: int = 1
    load_workers: int = 1
    pipeline_factory: PipelineFactory = field(default=dlt.pipeline, repr=False)
    source_factory: SourceFactory = field(
        default=github_org_enrichment_source,
        repr=False,
    )

    def build_pipeline(self) -> DltPipelineRunner:
        return self.pipeline_factory(
            pipeline_name=self.pipeline_name,
            pipelines_dir=self.pipelines_dir,
            destination=filesystem(
                bucket_url=f"s3://{self.output_bucket}",
                layout="{table_name}/dt={dt}/hr={hr}/{load_id}.{file_id}.{ext}",
                extra_placeholders={
                    "dt": self.dt,
                    "hr": str(self.hr),
                },
            ),
            dataset_name=self.dataset_name,
        )

    def build_source(self) -> object:
        return self.source_factory(
            self.input_path,
            self.github_token,
            self.dt,
            self.hr,
        )

    def output_paths(self) -> Dict[str, str]:
        return {
            "org_snapshot_path": (
                f"s3a://{self.output_bucket}/github_enrichment/github_org_snapshot/"
                f"dt={self.dt}/hr={self.hr}/"
            ),
            "org_errors_path": (
                f"s3a://{self.output_bucket}/github_enrichment/github_org_snapshot_errors/"
                f"dt={self.dt}/hr={self.hr}/"
            ),
        }

    def save_trace(self, pipeline: DltPipelineRunner) -> None:
        if pipeline.last_trace:
            pipeline.run([pipeline.last_trace], table_name="_trace")

    def log_failed_jobs(self, pipeline: DltPipelineRunner, load_info: Any) -> None:
        for package in load_info.load_packages:
            failed_jobs = pipeline.list_failed_jobs_in_package(package.load_id)
            if failed_jobs:
                logger.error("failed jobs in %s: %s", package.load_id, failed_jobs)

    def run(self) -> Dict[str, str]:
        pipeline = self.build_pipeline()
        source = self.build_source()

        extract_info = pipeline.extract(
            data=source,
            loader_file_format=self.loader_file_format,
        )
        logger.info("extract packages: %s", extract_info)

        normalize_info = pipeline.normalize(workers=self.normalize_workers)
        logger.info("normalized: %s", normalize_info)

        load_info = pipeline.load(workers=self.load_workers)
        logger.info("loaded packages: %s", load_info.load_packages)

        self.save_trace(pipeline)
        self.log_failed_jobs(pipeline, load_info)

        return self.output_paths()


def run_org_enrichment_pipeline(
    output_bucket: str,
    input_path: str,
    dt: str,
    hr: int,
) -> Dict[str, str]:
    return OrganizationEnrichmentPipeline(
        output_bucket=output_bucket,
        input_path=input_path,
        dt=dt,
        hr=hr,
        github_token=dlt.secrets["sources.github_enrichment.github_token"],
    ).run()
