from functools import lru_cache
from pydantic_settings import BaseSettings


class BuildCandidatesSettings(BaseSettings):
    S3_SILVER_ZONE_BUCKET_NAME: str
    AWS_PROFILE: str
    AWS_CONFIG_FILE: str
    AWS_SHARED_CREDENTIALS_FILE: str
    SPARK_MASTER_URL: str
    CANDIDATES_SIZE: int


@lru_cache(maxsize=1)
def get_build_candidates_settings() -> BuildCandidatesSettings:
    return BuildCandidatesSettings()  # type: ignore[call-arg]
