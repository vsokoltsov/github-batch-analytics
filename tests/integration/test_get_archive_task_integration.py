from __future__ import annotations

import gzip
from unittest.mock import Mock

import boto3
from moto import mock_aws
import pytest

from gba.settings.get_archive import get_download_archive_settings
from gba.tasks.get_archive import get_github_events_archive


@pytest.mark.integration
class TestGetArchiveTaskIntegration:
    @mock_aws
    def test_get_github_events_archive_integration(self, monkeypatch):
        bucket_name = "gba-landing-zone-task-test"
        monkeypatch.setenv("S3_LANDING_ZONE_BUCKET_NAME", bucket_name)
        get_download_archive_settings.cache_clear()

        s3_client = boto3.client("s3", region_name="eu-central-1")
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )

        body = gzip.compress(b'{"id": 1}\n')
        response = Mock()
        response.content = body
        response.raise_for_status = Mock()
        monkeypatch.setattr(
            "gba.services.download_github_archive.requests.get",
            Mock(return_value=response),
        )

        result = get_github_events_archive.function(
            landing_date="2026-03-07T18:30:09+00:00",
            archive_url="https://data.gharchive.org/ignored-by-service.json.gz",
        )

        expected_key = "raw/gharchive/dt=2026-03-07/hr=18/events.json.gz"
        assert result == f"s3a://{bucket_name}/{expected_key}"

        uploaded = s3_client.get_object(Bucket=bucket_name, Key=expected_key)
        assert uploaded["Body"].read() == body
