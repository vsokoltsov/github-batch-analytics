from functools import lru_cache
from pydantic_settings import BaseSettings


class BuildDashboardViewsSettings(BaseSettings):
    S3_MARTS_BUCKET_NAME: str
    AWS_PROFILE: str
    AWS_CONFIG_FILE: str
    AWS_SHARED_CREDENTIALS_FILE: str
    SPARK_MASTER_URL: str


@lru_cache(maxsize=1)
def get_dashboard_views_settings() -> BuildDashboardViewsSettings:
    return BuildDashboardViewsSettings()  # type: ignore[call-arg]
