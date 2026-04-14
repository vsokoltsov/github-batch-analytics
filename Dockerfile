FROM python:3.12-slim AS deps
WORKDIR /app

RUN pip install --no-cache-dir uv==0.9.13

COPY pyproject.toml uv.lock ./
# Export runtime deps from lock to requirements.txt
RUN ["uv", "export", "--no-dev", "--format", "requirements-txt", "-o", "requirements.txt"]

# Stage 2: Airflow runtime image
FROM apache/airflow:3.0.0
USER root

ARG SPARK_VERSION=3.5.4
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN apt-get update \
  && apt-get install -y --no-install-recommends curl openjdk-17-jre-headless \
  && rm -rf /var/lib/apt/lists/* \
  && echo "Downloading Spark ${SPARK_VERSION} distribution" \
  && curl -fL --retry 5 --retry-all-errors --connect-timeout 30 \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    -o /tmp/spark.tgz \
  && echo "Extracting Spark ${SPARK_VERSION} distribution" \
  && tar -xzf /tmp/spark.tgz -C /opt \
  && rm -f /tmp/spark.tgz \
  && ln -s "/opt/spark-${SPARK_VERSION}-bin-hadoop3" "${SPARK_HOME}"

USER airflow

COPY --from=deps /app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark

# Include the repository DAGs inside the Airflow image so the scheduler sees them.
COPY dags/ /opt/airflow/dags/
