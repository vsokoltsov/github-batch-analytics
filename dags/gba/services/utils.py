from typing import cast, Any
from pyspark.sql import SparkSession

def spark_session(name: str) -> SparkSession:
    builder = cast(Any, SparkSession.builder)
    return builder.appName(name).getOrCreate()

def to_s3a(path: str) -> str:
    if path.startswith("s3://"):
        return "s3a://" + path[len("s3://") :]
    return path