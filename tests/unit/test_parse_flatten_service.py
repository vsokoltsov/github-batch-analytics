from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from gba.services.parse_flatten import ParseService, _has_nested_field, _to_s3a


def _write_json_lines(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


@pytest.fixture
def parse_flatten_full_event_rows() -> list[dict]:
    return [
        {
            "id": "1",
            "type": "PullRequestEvent",
            "created_at": "2026-03-08T00:00:00Z",
            "repo": {"id": 1, "name": "org/repo"},
            "actor": {"id": 2, "login": "user"},
            "org": {
                "id": 3,
                "login": "org",
                "url": "https://api.github.com/orgs/org",
                "avatar_url": "https://avatars.githubusercontent.com/u/3",
            },
            "public": True,
            "payload": {
                "action": "opened",
                "ref_type": "branch",
                "pull_request": {"id": 123, "merged": True},
                "issue": {"id": 456},
                "comment": {"id": 789},
                "release": {"id": 987},
                "member": {"id": 654},
            },
        }
    ]


@pytest.fixture
def parse_flatten_event_rows_without_payload() -> list[dict]:
    return [
        {
            "id": "1",
            "type": "PushEvent",
            "created_at": "2026-03-08T00:00:00Z",
            "repo": {"id": 1, "name": "org/repo"},
            "actor": {"id": 2, "login": "user"},
            "org": {
                "id": 3,
                "login": "org",
                "url": "https://api.github.com/orgs/org",
                "avatar_url": "https://avatars.githubusercontent.com/u/3",
            },
            "public": True,
        }
    ]


@pytest.mark.unit
class TestParseFlattenServiceUnit:
    def test_to_s3a_converts_s3_scheme(self):
        assert _to_s3a("s3://bucket/key") == "s3a://bucket/key"
        assert _to_s3a("s3a://bucket/key") == "s3a://bucket/key"
        assert _to_s3a("/tmp/local.json") == "/tmp/local.json"

    def test_has_nested_field_handles_sparse_structs(self):
        schema = StructType(
            [
                StructField(
                    "payload",
                    StructType(
                        [
                            StructField(
                                "pull_request",
                                StructType(
                                    [
                                        StructField("id", LongType(), True),
                                        StructField("url", StringType(), True),
                                    ]
                                ),
                                True,
                            )
                        ]
                    ),
                    True,
                )
            ]
        )

        assert _has_nested_field(schema, "payload.pull_request.id") is True
        assert _has_nested_field(schema, "payload.pull_request.merged") is False

    def test_flat_writes_expected_flattened_columns(
        self,
        spark,
        tmp_path: Path,
        parse_flatten_full_event_rows: list[dict],
    ):
        input_path = tmp_path / "events.json"
        output_path = tmp_path / "out"
        _write_json_lines(input_path, parse_flatten_full_event_rows)

        service = ParseService(
            spark=spark,
            input_path=str(input_path),
            output_path=str(output_path),
        )

        service.flat()

        row = spark.read.parquet(str(output_path)).first()
        assert row is not None
        assert row.event_id == 1
        assert row.event_type == "PullRequestEvent"
        assert row.repo_id == 1
        assert row.repo_full_name == "org/repo"
        assert row.actor_id == 2
        assert row.actor_login == "user"
        assert row.org_id == 3
        assert row.org_login == "org"
        assert row.org_url == "https://api.github.com/orgs/org"
        assert row.org_avatar_url == "https://avatars.githubusercontent.com/u/3"
        assert row.is_public is True
        assert row.payload_action == "opened"
        assert row.payload_ref_type == "branch"
        assert row.pull_request_id == 123
        assert row.pull_request_merged is True
        assert row.issue_id == 456
        assert row.comment_id == 789
        assert row.release_id == 987
        assert row.member_id == 654
        assert row.ingestion_ts is not None
        assert row.source_file is not None

    def test_flat_sets_missing_optional_payload_fields_to_null(
        self,
        spark,
        tmp_path: Path,
        parse_flatten_event_rows_without_payload: list[dict],
    ):
        input_path = tmp_path / "events.json"
        output_path = tmp_path / "out"
        _write_json_lines(input_path, parse_flatten_event_rows_without_payload)

        service = ParseService(
            spark=spark,
            input_path=str(input_path),
            output_path=str(output_path),
        )

        service.flat()

        row = (
            spark.read.parquet(str(output_path))
            .select(
                "payload_action",
                "payload_ref_type",
                "pull_request_id",
                "pull_request_merged",
                "issue_id",
                "comment_id",
                "release_id",
                "member_id",
            )
            .first()
        )
        assert row is not None
        assert row.payload_action is None
        assert row.payload_ref_type is None
        assert row.pull_request_id is None
        assert row.pull_request_merged is None
        assert row.issue_id is None
        assert row.comment_id is None
        assert row.release_id is None
        assert row.member_id is None
