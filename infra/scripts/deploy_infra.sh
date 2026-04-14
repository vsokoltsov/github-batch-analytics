#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
TF_DIR="$ROOT_DIR/infra/terraform"

terraform -chdir="$TF_DIR" init
terraform -chdir="$TF_DIR" apply -auto-approve "$@"

AWS_REGION=$(terraform -chdir="$TF_DIR" output -raw aws_region)
AWS_PROFILE=$(terraform -chdir="$TF_DIR" output -raw aws_profile)
CLUSTER_NAME=$(terraform -chdir="$TF_DIR" output -raw eks_cluster_name)

aws eks update-kubeconfig \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE" \
  --name "$CLUSTER_NAME"

echo "Configured kubectl for cluster $CLUSTER_NAME in $AWS_REGION"
