#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
TF_DIR="$ROOT_DIR/infra/terraform"
TIMESTAMP=$(date -u +%Y%m%d%H%M%S)
if [[ -n "${1:-}" ]]; then
  TAG=$1
elif GIT_SHA=$(git -C "$ROOT_DIR" rev-parse --short=12 HEAD 2>/dev/null); then
  TAG=$GIT_SHA
else
  TAG=$TIMESTAMP
fi

AWS_REGION=$(terraform -chdir="$TF_DIR" output -raw aws_region)
AWS_PROFILE=$(terraform -chdir="$TF_DIR" output -raw aws_profile)
ECR_REPOSITORY_URL=$(terraform -chdir="$TF_DIR" output -raw ecr_repository_url)

aws ecr get-login-password --region "$AWS_REGION" --profile "$AWS_PROFILE" \
  | docker login --username AWS --password-stdin "${ECR_REPOSITORY_URL%/*}"

docker buildx build \
  --platform linux/amd64 \
  --push \
  --tag "$ECR_REPOSITORY_URL:$TAG" \
  "$ROOT_DIR"

echo "$TAG"
