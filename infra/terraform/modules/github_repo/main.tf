resource "github_actions_secret" "aws_role_to_assume" {
  repository      = var.github_repository
  secret_name     = "AWS_ROLE_TO_ASSUME"
  plaintext_value = var.github_actions_role_arn
}

resource "github_actions_secret" "github_enrichment_token" {
  repository      = var.github_repository
  secret_name     = "ENRICHMENT_GITHUB_TOKEN"
  plaintext_value = var.github_token
}

resource "github_actions_secret" "terraform_github_token" {
  repository      = var.github_repository
  secret_name     = "TERRAFORM_GITHUB_TOKEN"
  plaintext_value = var.github_token
}

resource "github_actions_variable" "aws_region" {
  repository    = var.github_repository
  variable_name = "AWS_REGION"
  value         = var.aws_region
}

resource "github_actions_variable" "ecr_repository" {
  repository    = var.github_repository
  variable_name = "ECR_REPOSITORY"
  value         = var.ecr_repository_name
}

resource "github_actions_variable" "eks_cluster_name" {
  repository    = var.github_repository
  variable_name = "EKS_CLUSTER_NAME"
  value         = var.eks_cluster_name
}

resource "github_actions_variable" "kubernetes_namespace" {
  repository    = var.github_repository
  variable_name = "KUBERNETES_NAMESPACE"
  value         = var.kubernetes_namespace
}

resource "github_actions_variable" "kubernetes_service_account_name" {
  repository    = var.github_repository
  variable_name = "KUBERNETES_SERVICE_ACCOUNT_NAME"
  value         = var.kubernetes_service_account_name
}

resource "github_actions_variable" "airflow_runtime_role_arn" {
  repository    = var.github_repository
  variable_name = "AIRFLOW_RUNTIME_ROLE_ARN"
  value         = var.airflow_runtime_role_arn
}

resource "github_actions_variable" "airflow_db_host" {
  repository    = var.github_repository
  variable_name = "AIRFLOW_DB_HOST"
  value         = var.airflow_db_host
}

resource "github_actions_variable" "airflow_db_port" {
  repository    = var.github_repository
  variable_name = "AIRFLOW_DB_PORT"
  value         = tostring(var.airflow_db_port)
}

resource "github_actions_variable" "airflow_db_name" {
  repository    = var.github_repository
  variable_name = "AIRFLOW_DB_NAME"
  value         = var.airflow_db_name
}

resource "github_actions_variable" "airflow_db_username" {
  repository    = var.github_repository
  variable_name = "AIRFLOW_DB_USERNAME"
  value         = var.airflow_db_username
}

resource "github_actions_secret" "airflow_db_password" {
  repository      = var.github_repository
  secret_name     = "AIRFLOW_DB_PASSWORD"
  plaintext_value = var.airflow_db_password
}

resource "github_actions_variable" "landing_zone_bucket_name" {
  repository    = var.github_repository
  variable_name = "LANDING_ZONE_BUCKET_NAME"
  value         = var.landing_zone_bucket_name
}

resource "github_actions_variable" "bronze_zone_bucket_name" {
  repository    = var.github_repository
  variable_name = "BRONZE_ZONE_BUCKET_NAME"
  value         = var.bronze_zone_bucket_name
}

resource "github_actions_variable" "silver_zone_bucket_name" {
  repository    = var.github_repository
  variable_name = "SILVER_ZONE_BUCKET_NAME"
  value         = var.silver_zone_bucket_name
}

resource "github_actions_variable" "marts_bucket_name" {
  repository    = var.github_repository
  variable_name = "MARTS_BUCKET_NAME"
  value         = var.marts_bucket_name
}

resource "github_actions_variable" "dlt_state_bucket_name" {
  repository    = var.github_repository
  variable_name = "DLT_STATE_BUCKET_NAME"
  value         = var.dlt_state_bucket_name
}

resource "github_actions_variable" "logging_bucket_name" {
  repository    = var.github_repository
  variable_name = "LOGGING_BUCKET_NAME"
  value         = var.logging_bucket_name
}
