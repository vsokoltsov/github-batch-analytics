from functools import lru_cache
from pydantic_settings import BaseSettings


class BuildMartsSettings(BaseSettings):
    S3_MARTS_BUCKET_NAME: str
    AWS_PROFILE: str = ""
    AWS_CONFIG_FILE: str = ""
    AWS_SHARED_CREDENTIALS_FILE: str = ""
    SPARK_MASTER_URL: str


@lru_cache(maxsize=1)
def get_build_candidates_settings() -> BuildMartsSettings:
    return BuildMartsSettings()  # type: ignore[call-arg]
