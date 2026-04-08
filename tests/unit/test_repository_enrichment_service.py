from __future__ import annotations

from dataclasses import dataclass

from gba.services.enrichment import repository


@dataclass
class FakeLoadPackage:
    load_id: str


@dataclass
class FakeLoadInfo:
    load_packages: list[FakeLoadPackage]


class FakePipeline:
    def __init__(self) -> None:
        self.last_trace = {"trace_id": "trace-1"}
        self.calls: list[tuple[str, object]] = []

    def extract(self, data: object, *, loader_file_format: str) -> str:
        self.calls.append(("extract", (data, loader_file_format)))
        return "extract-info"

    def normalize(self, workers: int) -> str:
        self.calls.append(("normalize", workers))
        return "normalize-info"

    def load(self, workers: int) -> FakeLoadInfo:
        self.calls.append(("load", workers))
        return FakeLoadInfo(load_packages=[FakeLoadPackage(load_id="load-1")])

    def run(self, data: object, *, table_name: str) -> None:
        self.calls.append(("run", (data, table_name)))

    def list_failed_jobs_in_package(self, load_id: str) -> list[object]:
        self.calls.append(("list_failed_jobs_in_package", load_id))
        return []


def test_repository_enrichment_pipeline_build_source_uses_injected_source_factory() -> (
    None
):
    def source_factory(
        input_path: str,
        github_token: str,
        dt: str,
        hr: int,
    ) -> tuple[str, str, str, int]:
        return input_path, github_token, dt, hr

    pipeline = repository.RepositoryEnrichmentPipeline(
        output_bucket="bronze-bucket",
        input_path="s3a://silver/repo_candidates/dt=2026-04-01/hr=7/",
        dt="2026-04-01",
        hr=7,
        github_token="ghp_test",
        source_factory=source_factory,
    )

    assert pipeline.build_source() == (
        "s3a://silver/repo_candidates/dt=2026-04-01/hr=7/",
        "ghp_test",
        "2026-04-01",
        7,
    )


def test_repository_enrichment_pipeline_output_paths_are_partitioned() -> None:
    pipeline = repository.RepositoryEnrichmentPipeline(
        output_bucket="bronze-bucket",
        input_path="s3a://silver/repo_candidates/dt=2026-04-01/hr=7/",
        dt="2026-04-01",
        hr=7,
        github_token="ghp_test",
    )

    assert pipeline.output_paths() == {
        "repo_snapshot_path": (
            "s3a://bronze-bucket/github_enrichment/github_repo_snapshot/"
            "dt=2026-04-01/hr=7/"
        ),
        "repo_topics_path": (
            "s3a://bronze-bucket/github_enrichment/github_repo_topics/"
            "dt=2026-04-01/hr=7/"
        ),
        "repo_errors_path": (
            "s3a://bronze-bucket/github_enrichment/github_repo_snapshot_errors/"
            "dt=2026-04-01/hr=7/"
        ),
    }


def test_repository_enrichment_pipeline_run_uses_pipeline_lifecycle_and_returns_paths() -> (
    None
):
    fake_pipeline = FakePipeline()
    pipeline_kwargs: dict[str, object] = {}

    def pipeline_factory(**kwargs: object) -> FakePipeline:
        pipeline_kwargs.update(kwargs)
        return fake_pipeline

    def source_factory(
        input_path: str,
        github_token: str,
        dt: str,
        hr: int,
    ) -> tuple[str, str, str, int]:
        return input_path, github_token, dt, hr

    enrichment_pipeline = repository.RepositoryEnrichmentPipeline(
        output_bucket="bronze-bucket",
        input_path="s3a://silver/repo_candidates/dt=2026-04-01/hr=7/",
        dt="2026-04-01",
        hr=7,
        github_token="ghp_test",
        pipeline_name="repo-pipeline",
        pipelines_dir="/tmp/custom-dlt",
        dataset_name="repo_dataset",
        loader_file_format="parquet",
        normalize_workers=1,
        load_workers=1,
        pipeline_factory=pipeline_factory,
        source_factory=source_factory,
    )

    output_paths = enrichment_pipeline.run()

    assert pipeline_kwargs["pipeline_name"] == "repo-pipeline"
    assert pipeline_kwargs["pipelines_dir"] == "/tmp/custom-dlt"
    assert pipeline_kwargs["dataset_name"] == "repo_dataset"
    assert fake_pipeline.calls == [
        (
            "extract",
            (
                (
                    "s3a://silver/repo_candidates/dt=2026-04-01/hr=7/",
                    "ghp_test",
                    "2026-04-01",
                    7,
                ),
                "parquet",
            ),
        ),
        ("normalize", 1),
        ("load", 1),
        ("run", ([{"trace_id": "trace-1"}], "_trace")),
        ("list_failed_jobs_in_package", "load-1"),
    ]
    assert output_paths["repo_snapshot_path"] == (
        "s3a://bronze-bucket/github_enrichment/github_repo_snapshot/"
        "dt=2026-04-01/hr=7/"
    )
