from __future__ import annotations

from unittest.mock import Mock

from gba.tasks.get_archive import get_github_events_archive


def test_get_github_events_archive_wires_dependencies(monkeypatch):
    landing_date = "2026-03-07T18:30:09+00:00"
    archive_url = "https://data.gharchive.org/2026-03-07-18.json.gz"

    fake_settings = Mock()
    fake_settings.S3_LANDING_ZONE_BUCKET_NAME = "gba-landing-zone-test"
    settings_getter = Mock(return_value=fake_settings)
    monkeypatch.setattr(
        "gba.tasks.get_archive.get_download_archive_settings", settings_getter
    )

    fake_s3_client = Mock()
    boto_client = Mock(return_value=fake_s3_client)
    monkeypatch.setattr("gba.tasks.get_archive.boto3.client", boto_client)

    fake_service = Mock()
    fake_service.download_and_push_to_s3.return_value = (
        "s3://gba-landing-zone-test/raw/path/events.json.gz"
    )
    service_ctor = Mock(return_value=fake_service)
    monkeypatch.setattr("gba.tasks.get_archive.DownloadGithubArchive", service_ctor)

    result = get_github_events_archive.function(
        landing_date=landing_date, archive_url=archive_url
    )

    settings_getter.assert_called_once_with()
    boto_client.assert_called_once_with("s3")
    service_ctor.assert_called_once_with(
        s3_client=fake_s3_client,
        bucket_name="gba-landing-zone-test",
    )
    fake_service.download_and_push_to_s3.assert_called_once_with(
        logical_date=landing_date,
        url=archive_url,
    )
    assert result == "s3://gba-landing-zone-test/raw/path/events.json.gz"
