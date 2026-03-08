from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class GetArchiveSettings(BaseSettings):
    S3_LANDING_ZONE_BUCKET_NAME: str

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


@lru_cache(maxsize=1)
def get_download_archive_settings() -> GetArchiveSettings:
    return GetArchiveSettings()  # type: ignore[call-arg]
