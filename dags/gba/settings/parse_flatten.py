from functools import lru_cache
from pydantic_settings import BaseSettings


class ParseFlattenSettings(BaseSettings):
    S3_BRONZE_ZONE_BUCKET_NAME: str
    AWS_PROFILE: str = ""
    AWS_CONFIG_FILE: str = ""
    AWS_SHARED_CREDENTIALS_FILE: str = ""
    SPARK_MASTER_URL: str


@lru_cache(maxsize=1)
def get_parse_flatten_settings() -> ParseFlattenSettings:
    return ParseFlattenSettings()  # type: ignore[call-arg]
