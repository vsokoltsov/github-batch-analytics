FROM python:3.12-slim AS deps
WORKDIR /app

RUN pip install --no-cache-dir uv==0.9.13

COPY pyproject.toml uv.lock ./
# Export runtime deps from lock to requirements.txt
RUN ["uv", "export", "--no-dev", "--format", "requirements-txt", "-o", "requirements.txt"]

FROM apache/spark:3.5.4-scala2.12-java17-python3-ubuntu AS spark

# Stage 2: Airflow runtime image
FROM apache/airflow:3.0.0
USER root

ENV JAVA_HOME=/opt/java/openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"

COPY --from=spark /opt/java/openjdk /opt/java/openjdk
COPY --from=spark /opt/spark /opt/spark

USER airflow

COPY --from=deps /app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark
RUN command -v spark-submit >/dev/null

# Include the repository DAGs inside the Airflow image so the scheduler sees them.
COPY dags/ /opt/airflow/dags/
