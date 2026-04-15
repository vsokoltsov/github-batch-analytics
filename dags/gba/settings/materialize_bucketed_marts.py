from functools import lru_cache

from pydantic_settings import BaseSettings


class MaterializeBucketedMartsSettings(BaseSettings):
    AWS_REGION: str = "eu-central-1"
    ATHENA_DATABASE_NAME: str = "github_analytics"
    ATHENA_WORKGROUP_NAME: str = "github-batch-analytics"
    ATHENA_QUERY_RESULTS_BUCKET_NAME: str
    ATHENA_REPOSITORY_TABLE_NAME: str = "repositories"
    ATHENA_ORGANIZATION_TABLE_NAME: str = "organizations"
    ATHENA_REPOSITORY_BUCKET_COUNT: int = 16
    ATHENA_ORGANIZATION_BUCKET_COUNT: int = 8
    S3_MARTS_BUCKET_NAME: str


@lru_cache(maxsize=1)
def get_materialize_bucketed_marts_settings() -> MaterializeBucketedMartsSettings:
    return MaterializeBucketedMartsSettings()  # type: ignore[call-arg]
