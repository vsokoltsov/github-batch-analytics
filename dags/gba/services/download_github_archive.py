from typing import TYPE_CHECKING
from dataclasses import dataclass
import gzip
from botocore.client import BaseClient
from datetime import datetime
import requests

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
else:
    S3Client = BaseClient


@dataclass
class DownloadGithubArchive:
    s3_client: S3Client
    bucket_name: str

    def download_and_push_to_s3(self, logical_date: str, url: str) -> str:
        dt = dt = datetime.fromisoformat(logical_date)
        yyyy_mm_dd = dt.strftime("%Y-%m-%d")
        hour = dt.hour
        url = f"https://data.gharchive.org/{yyyy_mm_dd}-{hour}.json.gz"

        # Download from GH Archive
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()

        # Optional quick validation that it is a valid gzip
        _ = gzip.decompress(resp.content)[:1]

        # Upload as-is to S3 raw zone
        key = f"raw/gharchive/dt={yyyy_mm_dd}/hr={hour}/events.json.gz"

        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=resp.content,
            ContentType="application/gzip",
        )
        return f"s3://{self.bucket_name}/{key}"
