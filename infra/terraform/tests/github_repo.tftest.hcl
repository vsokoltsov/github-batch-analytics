run "plan_github_repo_module" {
  command = plan

  module {
    source = "./modules/github_repo"
  }

  variables {
    github_repository               = "github-batch-analytics"
    github_actions_role_arn         = "arn:aws:iam::123456789012:role/github-actions-ecr-push"
    github_token                    = "ghp_example"
    aws_region                      = "eu-central-1"
    ecr_repository_name             = "github-batch-analytics-airflow"
    eks_cluster_name                = "github-batch-analytics"
    kubernetes_namespace            = "github-batch-analytics"
    kubernetes_service_account_name = "github-batch-analytics"
    airflow_runtime_role_arn        = "arn:aws:iam::123456789012:role/github-batch-analytics-airflow-runtime"
    airflow_db_host                 = "example.cluster-abcdefghijkl.eu-central-1.rds.amazonaws.com"
    airflow_db_port                 = 5432
    airflow_db_name                 = "airflow"
    airflow_db_username             = "airflow"
    airflow_db_password             = "super-secret-password"
    landing_zone_bucket_name        = "landing-bucket"
    bronze_zone_bucket_name         = "bronze-bucket"
    silver_zone_bucket_name         = "silver-bucket"
    marts_bucket_name               = "marts-bucket"
    dlt_state_bucket_name           = "dlt-state-bucket"
    logging_bucket_name             = "logging-bucket"
  }

  assert {
    condition     = github_actions_secret.github_enrichment_token.secret_name == "ENRICHMENT_GITHUB_TOKEN"
    error_message = "GitHub enrichment secret should keep the expected repository secret name."
  }

  assert {
    condition     = github_actions_variable.eks_cluster_name.value == "github-batch-analytics"
    error_message = "EKS cluster variable should match the configured cluster name."
  }

  assert {
    condition     = github_actions_variable.logging_bucket_name.variable_name == "LOGGING_BUCKET_NAME"
    error_message = "Logging bucket GitHub variable should keep the expected variable name."
  }
}
