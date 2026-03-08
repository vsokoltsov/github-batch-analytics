from __future__ import annotations

import gzip
from unittest.mock import Mock

import pytest

from gba.services.download_github_archive import DownloadGithubArchive


@pytest.mark.unit
class TestDownloadGithubArchiveUnit:
    def test_download_and_push_to_s3_success(self, monkeypatch):
        logical_date = "2026-03-07T18:30:09+00:00"
        s3_client_mock = Mock()
        archive = DownloadGithubArchive(
            s3_client=s3_client_mock, bucket_name="gba-landing-zone-test"
        )

        body = gzip.compress(b'{"id": 1}\n')
        response = Mock()
        response.content = body
        response.raise_for_status = Mock()
        requests_get = Mock(return_value=response)
        monkeypatch.setattr(
            "gba.services.download_github_archive.requests.get", requests_get
        )

        result = archive.download_and_push_to_s3(
            logical_date=logical_date, url="https://example.org/ignored"
        )

        requests_get.assert_called_once_with(
            "https://data.gharchive.org/2026-03-07-18.json.gz", timeout=120
        )
        s3_client_mock.put_object.assert_called_once_with(
            Bucket="gba-landing-zone-test",
            Key="raw/gharchive/dt=2026-03-07/hr=18/events.json.gz",
            Body=body,
            ContentType="application/gzip",
        )
        assert (
            result
            == "s3://gba-landing-zone-test/raw/gharchive/dt=2026-03-07/hr=18/events.json.gz"
        )

    def test_download_and_push_to_s3_http_error(self, monkeypatch):
        logical_date = "2026-03-07T18:30:09+00:00"
        s3_client_mock = Mock()
        archive = DownloadGithubArchive(
            s3_client=s3_client_mock, bucket_name="gba-landing-zone-test"
        )

        response = Mock()
        response.raise_for_status.side_effect = RuntimeError("upstream-failure")
        requests_get = Mock(return_value=response)
        monkeypatch.setattr(
            "gba.services.download_github_archive.requests.get", requests_get
        )

        with pytest.raises(RuntimeError, match="upstream-failure"):
            archive.download_and_push_to_s3(
                logical_date=logical_date, url="https://example.org/ignored"
            )

        s3_client_mock.put_object.assert_not_called()

    def test_download_and_push_to_s3_invalid_gzip(self, monkeypatch):
        logical_date = "2026-03-07T18:30:09+00:00"
        s3_client_mock = Mock()
        archive = DownloadGithubArchive(
            s3_client=s3_client_mock, bucket_name="gba-landing-zone-test"
        )

        response = Mock()
        response.content = b"not-a-gzip-stream"
        response.raise_for_status = Mock()
        requests_get = Mock(return_value=response)
        monkeypatch.setattr(
            "gba.services.download_github_archive.requests.get", requests_get
        )

        with pytest.raises(gzip.BadGzipFile):
            archive.download_and_push_to_s3(
                logical_date=logical_date, url="https://example.org/ignored"
            )

        s3_client_mock.put_object.assert_not_called()
