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

        payload = b'{"id":"1","type":"PushEvent","created_at":"2026-03-08T00:00:00Z","repo":{"id":1,"name":"org/repo"},"actor":{"id":2,"login":"user"}}\n'
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
