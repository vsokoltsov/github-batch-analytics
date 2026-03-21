from functools import lru_cache
from pydantic_settings import BaseSettings
from enum import Enum


class CandidatesType(Enum):
    REPO = "repo"
    ORG = "org"


class BuildCandidateSettings(BaseSettings):
    S3_SILVER_ZONE_BUCKET_NAME: str
    AWS_PROFILE: str
    AWS_CONFIG_FILE: str
    AWS_SHARED_CREDENTIALS_FILE: str
    SPARK_MASTER_URL: str


@lru_cache(maxsize=1)
def get_build_candidates_settings() -> BuildCandidateSettings:
    return BuildCandidateSettings()  # type: ignore[call-arg]
