from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from gba.services.parse_flatten import ParseService


@pytest.mark.integration
class TestParseFlattenServiceIntegration:
    def test_flat_writes_parquet_from_gzip_json(self, tmp_path):
        pyspark_sql = pytest.importorskip("pyspark.sql")
        spark_session_cls = pyspark_sql.SparkSession

        input_path = tmp_path / "events.json.gz"
        output_path = tmp_path / "out"

        payload = (
            b'{"id":"1","type":"PushEvent","created_at":"2026-03-08T00:00:00Z",'
            b'"repo":{"id":1,"name":"org/repo"},'
            b'"actor":{"id":2,"login":"user"},'
            b'"org":{"id":3,"login":"org","url":"https://api.github.com/orgs/org","avatar_url":"https://avatars.githubusercontent.com/u/3"},'
            b'"public":true}\n'
        )
        input_path.write_bytes(gzip.compress(payload))

        spark = (
            spark_session_cls.builder.master("local[1]")
            .appName("parse-flatten-test")
            .getOrCreate()
        )

        try:
            service = ParseService(
                spark=spark,
                input_path=str(input_path),
                output_path=str(output_path),
            )
            service.flat()
        finally:
            spark.stop()

        part_files = list(Path(output_path).glob("part-*.parquet"))
        assert part_files, "Expected parquet part files to be written"

    def test_flat_tolerates_missing_nested_payload_fields(self, tmp_path):
        pyspark_sql = pytest.importorskip("pyspark.sql")
        spark_session_cls = pyspark_sql.SparkSession

        input_path = tmp_path / "events.json.gz"
        output_path = tmp_path / "out"

        payload = (
            b'{"id":"1","type":"PullRequestEvent","created_at":"2026-03-08T00:00:00Z",'
            b'"repo":{"id":1,"name":"org/repo"},'
            b'"actor":{"id":2,"login":"user"},'
            b'"org":{"id":3,"login":"org","url":"https://api.github.com/orgs/org","avatar_url":"https://avatars.githubusercontent.com/u/3"},'
            b'"public":true,'
            b'"payload":{"pull_request":{"id":123,"url":"https://example.test/pr/123"}}}\n'
        )
        input_path.write_bytes(gzip.compress(payload))

        spark = (
            spark_session_cls.builder.master("local[1]")
            .appName("parse-flatten-sparse-payload-test")
            .getOrCreate()
        )

        try:
            service = ParseService(
                spark=spark,
                input_path=str(input_path),
                output_path=str(output_path),
            )
            service.flat()

            result_df = spark.read.parquet(str(output_path))
            row = result_df.select("pull_request_id", "pull_request_merged").first()
        finally:
            spark.stop()

        assert row is not None
        assert row.pull_request_id == 123
        assert row.pull_request_merged is None
