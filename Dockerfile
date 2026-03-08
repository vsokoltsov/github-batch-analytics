FROM python:3.12-slim AS deps
WORKDIR /app

RUN pip install --no-cache-dir uv==0.9.13

COPY pyproject.toml uv.lock ./
# Export runtime deps from lock to requirements.txt
RUN ["uv", "export", "--no-dev", "--format", "requirements-txt", "-o", "requirements.txt"]

# Stage 2: Airflow runtime image
FROM apache/airflow:3.0.0
USER airflow

COPY --from=deps /app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
