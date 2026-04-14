from functools import lru_cache
from pydantic_settings import BaseSettings


class EnrichCandidatesSettings(BaseSettings):
    S3_BRONZE_ZONE_BUCKET_NAME: str
    AWS_PROFILE: str = ""
    AWS_CONFIG_FILE: str = ""
    AWS_SHARED_CREDENTIALS_FILE: str = ""


@lru_cache(maxsize=1)
def get_enrich_candidates_settings() -> EnrichCandidatesSettings:
    return EnrichCandidatesSettings()  # type: ignore[call-arg]
