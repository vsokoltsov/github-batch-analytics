from __future__ import annotations

import gzip
from unittest.mock import Mock

import boto3
from moto import mock_aws
import pytest

from gba.services.download_github_archive import DownloadGithubArchive


@pytest.mark.integration
class TestDownloadGithubArchiveIntegration:
    @mock_aws
    def test_download_and_push_to_s3_integration(self, monkeypatch):
        bucket_name = "gba-landing-zone-test"
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

        service = DownloadGithubArchive(s3_client=s3_client, bucket_name=bucket_name)
        result = service.download_and_push_to_s3(
            logical_date="2026-03-07T18:30:09+00:00",
            url="https://example.org/ignored",
        )

        expected_key = "raw/gharchive/dt=2026-03-07/hr=18/events.json.gz"
        assert result == f"s3://{bucket_name}/{expected_key}"

        uploaded = s3_client.get_object(Bucket=bucket_name, Key=expected_key)
        assert uploaded["Body"].read() == body
