output "landing_zone_bucket_name" {
  description = "S3 bucket name for landing zone."
  value       = module.storage.landing_zone_bucket_name
}

output "aws_region" {
  description = "AWS region used for this stack."
  value       = var.aws_region
}

output "aws_profile" {
  description = "AWS profile used for this stack."
  value       = var.aws_profile
}

output "landing_zone_bucket_arn" {
  description = "S3 bucket ARN for landing zone."
  value       = module.storage.landing_zone_bucket_arn
}

output "bronze_zone_bucket_name" {
  description = "S3 bucket name for bronze zone."
  value       = module.storage.bronze_zone_bucket_name
}

output "bronze_zone_bucket_arn" {
  description = "S3 bucket ARN for bronze zone."
  value       = module.storage.bronze_zone_bucket_arn
}

output "silver_zone_bucket_name" {
  description = "S3 bucket name for silver zone."
  value       = module.storage.silver_zone_bucket_name
}

output "silver_zone_bucket_arn" {
  description = "S3 bucket ARN for silver zone."
  value       = module.storage.silver_zone_bucket_arn
}

output "marts_bucket_name" {
  description = "S3 bucket name for curated dashboard marts."
  value       = module.storage.marts_bucket_name
}

output "marts_bucket_arn" {
  description = "S3 bucket ARN for curated dashboard marts."
  value       = module.storage.marts_bucket_arn
}

output "dlt_state_bucket_name" {
  description = "S3 bucket name for dlt pipeline state."
  value       = module.storage.dlt_state_bucket_name
}

output "dlt_state_bucket_arn" {
  description = "S3 bucket ARN for dlt pipeline state."
  value       = module.storage.dlt_state_bucket_arn
}

output "logging_bucket_name" {
  description = "S3 bucket name for Airflow remote logs."
  value       = module.storage.logging_bucket_name
}

output "dlt_state_bucket_access_policy_arn" {
  description = "Managed IAM policy ARN granting access to the dlt state bucket."
  value       = module.storage.dlt_state_bucket_access_policy_arn
}

output "ecr_repository_name" {
  description = "ECR repository name for Airflow image."
  value       = module.registry.repository_name
}

output "ecr_repository_url" {
  description = "ECR repository URL for Airflow image pushes."
  value       = module.registry.repository_url
}

output "athena_query_results_bucket_name" {
  description = "S3 bucket name used for Athena query results."
  value       = module.catalog.athena_query_results_bucket_name
}

output "athena_query_results_bucket_arn" {
  description = "S3 bucket ARN used for Athena query results."
  value       = module.catalog.athena_query_results_bucket_arn
}

output "athena_database_name" {
  description = "Glue/Athena database name."
  value       = module.catalog.athena_database_name
}

output "athena_workgroup_name" {
  description = "Athena workgroup name."
  value       = module.catalog.athena_workgroup_name
}

output "athena_repository_table_name" {
  description = "Athena/Glue table name for repository marts."
  value       = module.catalog.athena_repository_table_name
}

output "athena_organization_table_name" {
  description = "Athena/Glue table name for organization marts."
  value       = module.catalog.athena_organization_table_name
}

output "athena_dashboard_table_names" {
  description = "Athena/Glue table names for dashboard datasets."
  value       = module.catalog.athena_dashboard_table_names
}

output "github_actions_role_arn" {
  description = "IAM role ARN that GitHub Actions assumes via OIDC."
  value       = module.identity.github_actions_role_arn
}

output "eks_cluster_name" {
  description = "EKS cluster name."
  value       = module.eks_cluster.cluster_name
}

output "eks_cluster_endpoint" {
  description = "EKS cluster API endpoint."
  value       = module.eks_cluster.cluster_endpoint
}

output "eks_cluster_oidc_issuer_url" {
  description = "OIDC issuer URL for the EKS cluster."
  value       = module.eks_cluster.cluster_oidc_issuer_url
}

output "kubernetes_namespace" {
  description = "Kubernetes namespace used for the application."
  value       = var.kubernetes_namespace
}

output "kubernetes_service_account_name" {
  description = "Service account name used for the application."
  value       = var.kubernetes_service_account_name
}

output "airflow_runtime_role_arn" {
  description = "IAM role ARN used by Airflow and Spark pods via IRSA."
  value       = module.identity.airflow_runtime_role_arn
}

output "airflow_db_host" {
  description = "RDS hostname for Airflow."
  value       = module.database.host
}

output "airflow_db_port" {
  description = "RDS port for Airflow."
  value       = module.database.port
}

output "airflow_db_name" {
  description = "Database name for Airflow."
  value       = module.database.name
}

output "airflow_db_username" {
  description = "Database username for Airflow."
  value       = module.database.username
}

output "airflow_db_password" {
  description = "Database password for Airflow."
  value       = module.database.password
  sensitive   = true
}
