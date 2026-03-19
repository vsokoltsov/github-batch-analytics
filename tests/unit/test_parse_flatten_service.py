from __future__ import annotations

from unittest.mock import Mock

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from gba.services.parse_flatten import (
    ParseService,
    _has_nested_field,
    _to_s3a,
)


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

    def test_flat_reads_and_writes_paths(self, monkeypatch):
        spark = Mock()
        source_df = Mock()
        selected_df = Mock()
        enriched_once_df = Mock()
        enriched_df = Mock()

        spark.read.json.return_value = source_df
        source_df.schema = StructType([])
        source_df.select.return_value = selected_df
        selected_df.withColumn.return_value = enriched_once_df
        enriched_once_df.withColumn.return_value = enriched_df
        enriched_df.write.mode.return_value = enriched_df.write

        monkeypatch.setattr("gba.services.parse_flatten.col", Mock(return_value=Mock()))
        lit_mock = Mock()
        lit_mock.return_value.cast.return_value.alias.return_value = Mock()
        monkeypatch.setattr("gba.services.parse_flatten.lit", lit_mock)
        monkeypatch.setattr(
            "gba.services.parse_flatten.current_timestamp", Mock(return_value=Mock())
        )
        monkeypatch.setattr(
            "gba.services.parse_flatten.input_file_name", Mock(return_value=Mock())
        )

        service = ParseService(
            spark=spark,
            input_path="s3a://landing/raw/events.json.gz",
            output_path="s3a://bronze/gh_events_flat/",
        )
        service.flat()

        spark.read.json.assert_called_once_with("s3a://landing/raw/events.json.gz")
        enriched_df.write.mode.assert_called_once_with("overwrite")
        enriched_df.write.parquet.assert_called_once_with(
            "s3a://bronze/gh_events_flat/"
        )
