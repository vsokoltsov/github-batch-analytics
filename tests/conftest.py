from __future__ import annotations

import sys
from pathlib import Path

import boto3
import pytest


def _ensure_dags_on_path() -> None:
    dags_dir = Path(__file__).resolve().parents[1] / "dags"
    dags_path = str(dags_dir)
    if dags_path not in sys.path:
        sys.path.insert(0, dags_path)


_ensure_dags_on_path()


@pytest.fixture(autouse=True)
def _isolate_aws_env(monkeypatch):
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_CONFIG_FILE", raising=False)
    monkeypatch.delenv("AWS_SHARED_CREDENTIALS_FILE", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    boto3.DEFAULT_SESSION = None
