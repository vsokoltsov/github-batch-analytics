#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
TF_DIR="$ROOT_DIR/infra/terraform"
CHART_DIR="$ROOT_DIR/infra/helm/github-batch-analytics"
RELEASE_NAME=${RELEASE_NAME:-github-batch-analytics}
IMAGE_TAG=${1:-$(git -C "$ROOT_DIR" rev-parse --short HEAD)}

read_github_token() {
  if [[ -n "${GITHUB_ENRICHMENT_TOKEN:-}" ]]; then
    printf '%s' "$GITHUB_ENRICHMENT_TOKEN"
    return
  fi

  python3 - <<'PY'
import tomllib
from pathlib import Path
secrets = tomllib.loads(Path('.dlt/secrets.toml').read_text())
print(secrets['sources']['github_enrichment']['github_token'], end='')
PY
}

AWS_REGION=$(terraform -chdir="$TF_DIR" output -raw aws_region)
NAMESPACE=$(terraform -chdir="$TF_DIR" output -raw kubernetes_namespace)
SERVICE_ACCOUNT_NAME=$(terraform -chdir="$TF_DIR" output -raw kubernetes_service_account_name)
SERVICE_ACCOUNT_ROLE_ARN=$(terraform -chdir="$TF_DIR" output -raw airflow_runtime_role_arn)
ECR_REPOSITORY_URL=$(terraform -chdir="$TF_DIR" output -raw ecr_repository_url)
AIRFLOW_DB_HOST=$(terraform -chdir="$TF_DIR" output -raw airflow_db_host)
AIRFLOW_DB_PORT=$(terraform -chdir="$TF_DIR" output -raw airflow_db_port)
AIRFLOW_DB_NAME=$(terraform -chdir="$TF_DIR" output -raw airflow_db_name)
AIRFLOW_DB_USERNAME=$(terraform -chdir="$TF_DIR" output -raw airflow_db_username)
AIRFLOW_DB_PASSWORD=$(terraform -chdir="$TF_DIR" output -raw airflow_db_password)
LOGGING_BUCKET=$(terraform -chdir="$TF_DIR" output -raw logging_bucket_name)
LANDING_BUCKET=$(terraform -chdir="$TF_DIR" output -raw landing_zone_bucket_name)
BRONZE_BUCKET=$(terraform -chdir="$TF_DIR" output -raw bronze_zone_bucket_name)
SILVER_BUCKET=$(terraform -chdir="$TF_DIR" output -raw silver_zone_bucket_name)
MARTS_BUCKET=$(terraform -chdir="$TF_DIR" output -raw marts_bucket_name)
DLT_STATE_BUCKET=$(terraform -chdir="$TF_DIR" output -raw dlt_state_bucket_name)
GITHUB_TOKEN=$(read_github_token)

OVERRIDE_VALUES=$(mktemp)
cat > "$OVERRIDE_VALUES" <<EOF
serviceAccount:
  annotations:
    eks.amazonaws.com/role-arn: "$SERVICE_ACCOUNT_ROLE_ARN"
EOF
trap 'rm -f "$OVERRIDE_VALUES"' EXIT

if kubectl -n "$NAMESPACE" get deployment/${RELEASE_NAME}-airflow-scheduler >/dev/null 2>&1; then
  kubectl -n "$NAMESPACE" delete deployment/${RELEASE_NAME}-airflow-scheduler --wait=true
fi

if kubectl -n "$NAMESPACE" get job/${RELEASE_NAME}-airflow-init >/dev/null 2>&1; then
  kubectl -n "$NAMESPACE" delete job/${RELEASE_NAME}-airflow-init --wait=true
fi

helm upgrade --install "$RELEASE_NAME" "$CHART_DIR" \
  -f "$OVERRIDE_VALUES" \
  --namespace "$NAMESPACE" \
  --create-namespace \
  --set image.repository="$ECR_REPOSITORY_URL" \
  --set image.tag="$IMAGE_TAG" \
  --set aws.region="$AWS_REGION" \
  --set serviceAccount.name="$SERVICE_ACCOUNT_NAME" \
  --set airflow.database.host="$AIRFLOW_DB_HOST" \
  --set airflow.database.port="$AIRFLOW_DB_PORT" \
  --set airflow.database.name="$AIRFLOW_DB_NAME" \
  --set airflow.database.username="$AIRFLOW_DB_USERNAME" \
  --set-string airflow.database.password="$AIRFLOW_DB_PASSWORD" \
  --set airflow.buckets.landingZone="$LANDING_BUCKET" \
  --set airflow.buckets.bronzeZone="$BRONZE_BUCKET" \
  --set airflow.buckets.silverZone="$SILVER_BUCKET" \
  --set airflow.buckets.marts="$MARTS_BUCKET" \
  --set airflow.buckets.dltState="$DLT_STATE_BUCKET" \
  --set airflow.buckets.logging="$LOGGING_BUCKET" \
  --set-string airflow.dlt.githubToken="$GITHUB_TOKEN"

kubectl -n "$NAMESPACE" wait --for=condition=complete job/${RELEASE_NAME}-airflow-init --timeout=10m
kubectl -n "$NAMESPACE" rollout status deployment/${RELEASE_NAME}-airflow-api-server --timeout=10m
kubectl -n "$NAMESPACE" rollout status deployment/${RELEASE_NAME}-airflow-scheduler --timeout=10m
kubectl -n "$NAMESPACE" rollout status deployment/${RELEASE_NAME}-airflow-dag-processor --timeout=10m
kubectl -n "$NAMESPACE" rollout status deployment/${RELEASE_NAME}-spark-master --timeout=10m
kubectl -n "$NAMESPACE" rollout status deployment/${RELEASE_NAME}-spark-worker --timeout=10m

LB_VALUE=$(kubectl -n "$NAMESPACE" get svc ${RELEASE_NAME}-airflow-api-server -o jsonpath='{.status.loadBalancer.ingress[0].hostname}{.status.loadBalancer.ingress[0].ip}')
AIRFLOW_IP="$LB_VALUE"
if [[ "$LB_VALUE" != *[0-9]*.* || "$LB_VALUE" == *amazonaws.com* ]]; then
  RESOLVED_IP=$(dig +short "$LB_VALUE" | head -n1 || true)
  if [[ -n "$RESOLVED_IP" ]]; then
    AIRFLOW_IP="$RESOLVED_IP"
  fi
fi

echo "Airflow UI address: $AIRFLOW_IP"
echo "Raw load balancer value: $LB_VALUE"
