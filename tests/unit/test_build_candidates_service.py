from __future__ import annotations

from typing import Any
from argparse import Namespace
from unittest.mock import Mock, PropertyMock

import pytest

from gba.services.build_candidates import (
    BuildCandidates,
    CandidatesType,
    _to_s3a,
    camel_to_snake,
    main,
)


class FakeExpression:
    def __init__(self, name: str):
        self.name = name

    def __eq__(self, other) -> Any:
        return FakeExpression(f"{self.name}=={other}")

    def __gt__(self, other):
        return FakeExpression(f"{self.name}>{other}")

    def __truediv__(self, other):
        right = other.name if isinstance(other, FakeExpression) else str(other)
        return FakeExpression(f"{self.name}/{right}")

    def alias(self, name: str):
        return FakeExpression(name)

    def cast(self, data_type: str):
        return FakeExpression(f"{self.name}:{data_type}")


@pytest.mark.unit
class TestBuildCandidatesServiceUnit:
    def test_camel_to_snake_converts_event_name(self):
        assert camel_to_snake("PullRequestReviewEvent") == "pull_request_review_event"
        assert camel_to_snake("PushEvent") == "push_event"

    def test_to_s3a_converts_s3_scheme(self):
        assert _to_s3a("s3://bucket/key") == "s3a://bucket/key"
        assert _to_s3a("s3a://bucket/key") == "s3a://bucket/key"
        assert _to_s3a("/tmp/input.parquet") == "/tmp/input.parquet"

    def test_post_init_reads_parquet_and_builds_event_columns(self, monkeypatch):
        spark = Mock()
        df = Mock()
        spark.read.parquet.return_value = df

        col_mock = Mock(side_effect=lambda name: FakeExpression(name))
        when_expression = Mock()
        when_expression.otherwise.return_value = FakeExpression("when_result")
        when_mock = Mock(return_value=when_expression)
        sum_expression = Mock()
        sum_expression.alias.return_value = FakeExpression("aliased_sum")
        sum_mock = Mock(return_value=sum_expression)
        lit_mock = Mock(return_value=FakeExpression("lit_expr"))

        functions_mock = Mock()
        functions_mock.col = col_mock
        functions_mock.when = when_mock
        functions_mock.sum = sum_mock
        functions_mock.lit = lit_mock
        monkeypatch.setattr("gba.services.build_candidates.F", functions_mock)
        monkeypatch.setattr(
            BuildCandidates,
            "event_types_list",
            PropertyMock(return_value=["PushEvent", "IssuesEvent"]),
        )

        service = BuildCandidates(
            spark=spark,
            input_path="s3a://bronze/input/",
            output_path="s3a://silver/output/",
            dt="2026-03-20",
            hr="21",
        )

        spark.read.parquet.assert_called_once_with("s3a://bronze/input/")
        assert service.df is df
        assert len(service.event_columns) == 2
        assert set(service.ratio_columns) == {
            "push_events_ratio",
            "issues_events_ratio",
        }

    def test_repositories_writes_output(self):
        service = object.__new__(BuildCandidates)
        service.output_path = "s3a://silver/repo_candidates/"
        aggregated_df = Mock()
        aggregated_df.write.mode.return_value = aggregated_df.write
        service._common_metrics = Mock(return_value=aggregated_df)

        BuildCandidates.repositories(service)

        service._common_metrics.assert_called_once_with(["repo_id", "repo_full_name"])
        aggregated_df.write.mode.assert_called_once_with("overwrite")
        aggregated_df.write.parquet.assert_called_once_with(
            "s3a://silver/repo_candidates/"
        )

    def test_organizations_writes_output(self, monkeypatch):
        service = object.__new__(BuildCandidates)
        service.output_path = "s3a://silver/org_candidates/"
        aggregated_df = Mock()
        aggregated_df.write.mode.return_value = aggregated_df.write
        service._common_metrics = Mock(return_value=aggregated_df)

        count_distinct_expression = Mock()
        aliased_expression = Mock(name="repos_count_expr")
        count_distinct_expression.alias.return_value = aliased_expression
        monkeypatch.setattr(
            "gba.services.build_candidates.F.countDistinct",
            Mock(return_value=count_distinct_expression),
        )

        BuildCandidates.organizations(service)

        args, _ = service._common_metrics.call_args
        assert args[0] == ["org_id", "org_login"]
        assert args[1] is aliased_expression
        aggregated_df.write.mode.assert_called_once_with("overwrite")
        aggregated_df.write.parquet.assert_called_once_with(
            "s3a://silver/org_candidates/"
        )

    def test_main_dispatches_repo_candidates(self, monkeypatch):
        args = Namespace(
            input_path="s3://bronze/input/",
            output_path="s3://silver/output/",
            type="repo",
            dt="2026-03-20",
            hr="21",
        )
        parser_mock = Mock()
        parser_mock.parse_args.return_value = args
        monkeypatch.setattr(
            "gba.services.build_candidates.argparse.ArgumentParser",
            Mock(return_value=parser_mock),
        )

        spark = Mock()
        builder = Mock()
        builder.appName.return_value.getOrCreate.return_value = spark
        monkeypatch.setattr(
            "gba.services.build_candidates.SparkSession",
            Mock(builder=builder),
        )

        service = Mock(spec=BuildCandidates)
        service_ctor = Mock(return_value=service)
        monkeypatch.setattr(
            "gba.services.build_candidates.BuildCandidates", service_ctor
        )

        main()

        service_ctor.assert_called_once_with(
            spark=spark,
            input_path="s3a://bronze/input/",
            output_path="s3a://silver/output/",
            dt="2026-03-20",
            hr="21",
        )
        service.repositories.assert_called_once_with()
        service.organizations.assert_not_called()
        spark.stop.assert_called_once_with()

    def test_main_dispatches_org_candidates(self, monkeypatch):
        args = Namespace(
            input_path="s3://bronze/input/",
            output_path="s3://silver/output/",
            type=CandidatesType.ORG.value,
            dt="2026-03-20",
            hr="21",
        )
        parser_mock = Mock()
        parser_mock.parse_args.return_value = args
        monkeypatch.setattr(
            "gba.services.build_candidates.argparse.ArgumentParser",
            Mock(return_value=parser_mock),
        )

        spark = Mock()
        builder = Mock()
        builder.appName.return_value.getOrCreate.return_value = spark
        monkeypatch.setattr(
            "gba.services.build_candidates.SparkSession",
            Mock(builder=builder),
        )

        service = Mock(spec=BuildCandidates)
        monkeypatch.setattr(
            "gba.services.build_candidates.BuildCandidates", Mock(return_value=service)
        )

        main()

        service.organizations.assert_called_once_with()
        service.repositories.assert_not_called()
        spark.stop.assert_called_once_with()
