from airflow.sdk import task
from gba.services.download_github_archive import DownloadGithubArchive
from gba.settings.get_archive import get_download_archive_settings
import boto3


@task
def get_github_events_archive(landing_date: str, archive_url: str) -> str:
    settings = get_download_archive_settings()
    s3_client = boto3.client('s3')
    service = DownloadGithubArchive(
        s3_client=s3_client,
        bucket_name=settings.S3_LANDING_ZONE_BUCKET_NAME
    )
    return service.download_and_push_to_s3(
        logical_date=landing_date,
        url=archive_url
    )