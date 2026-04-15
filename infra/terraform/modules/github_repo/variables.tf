variable "github_repository" { type = string }
variable "github_actions_role_arn" { type = string }
variable "github_token" {
  type      = string
  sensitive = true
}
variable "aws_region" { type = string }
variable "ecr_repository_name" { type = string }
variable "eks_cluster_name" { type = string }
variable "kubernetes_namespace" { type = string }
variable "kubernetes_service_account_name" { type = string }
variable "airflow_runtime_role_arn" { type = string }
variable "airflow_db_host" { type = string }
variable "airflow_db_port" { type = number }
variable "airflow_db_name" { type = string }
variable "airflow_db_username" { type = string }
variable "airflow_db_password" {
  type      = string
  sensitive = true
}
variable "athena_database_name" { type = string }
variable "athena_workgroup_name" { type = string }
variable "athena_query_results_bucket_name" { type = string }
variable "athena_repository_table_name" { type = string }
variable "athena_organization_table_name" { type = string }
variable "athena_repository_bucket_count" { type = number }
variable "athena_organization_bucket_count" { type = number }
variable "landing_zone_bucket_name" { type = string }
variable "bronze_zone_bucket_name" { type = string }
variable "silver_zone_bucket_name" { type = string }
variable "marts_bucket_name" { type = string }
variable "dlt_state_bucket_name" { type = string }
variable "logging_bucket_name" { type = string }
variable "terraform_state_bucket_name" { type = string }
