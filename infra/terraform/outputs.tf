output "landing_zone_bucket_name" {
  description = "S3 bucket name for landing zone."
  value       = aws_s3_bucket.landing_zone.bucket
}

output "landing_zone_bucket_arn" {
  description = "S3 bucket ARN for landing zone."
  value       = aws_s3_bucket.landing_zone.arn
}

output "bronze_zone_bucket_name" {
  description = "S3 bucket name for bronze zone."
  value       = aws_s3_bucket.bronze_zone.bucket
}

output "bronze_zone_bucket_arn" {
  description = "S3 bucket ARN for bronze zone."
  value       = aws_s3_bucket.bronze_zone.arn
}

output "silver_zone_bucket_name" {
  description = "S3 bucket name for silver zone."
  value       = aws_s3_bucket.silver_zone.bucket
}

output "silver_zone_bucket_arn" {
  description = "S3 bucket ARN for silver zone."
  value       = aws_s3_bucket.silver_zone.arn
}

output "marts_bucket_name" {
  description = "S3 bucket name for curated dashboard marts."
  value       = aws_s3_bucket.marts.bucket
}

output "marts_bucket_arn" {
  description = "S3 bucket ARN for curated dashboard marts."
  value       = aws_s3_bucket.marts.arn
}

output "dlt_state_bucket_name" {
  description = "S3 bucket name for dlt pipeline state."
  value       = aws_s3_bucket.dlt_state.bucket
}

output "dlt_state_bucket_arn" {
  description = "S3 bucket ARN for dlt pipeline state."
  value       = aws_s3_bucket.dlt_state.arn
}

output "dlt_state_bucket_access_policy_arn" {
  description = "Managed IAM policy ARN granting access to the dlt state bucket."
  value       = aws_iam_policy.dlt_state_bucket_access.arn
}

output "ecr_repository_name" {
  description = "ECR repository name for Airflow image."
  value       = aws_ecr_repository.airflow.name
}

output "ecr_repository_url" {
  description = "ECR repository URL for Airflow image pushes."
  value       = aws_ecr_repository.airflow.repository_url
}

output "athena_query_results_bucket_name" {
  description = "S3 bucket name used for Athena query results."
  value       = aws_s3_bucket.athena_query_results.bucket
}

output "athena_query_results_bucket_arn" {
  description = "S3 bucket ARN used for Athena query results."
  value       = aws_s3_bucket.athena_query_results.arn
}

output "athena_database_name" {
  description = "Glue/Athena database name."
  value       = aws_glue_catalog_database.analytics.name
}

output "athena_workgroup_name" {
  description = "Athena workgroup name."
  value       = aws_athena_workgroup.analytics.name
}

output "athena_repository_table_name" {
  description = "Athena/Glue table name for repository marts."
  value       = aws_glue_catalog_table.repository_marts.name
}

output "athena_organization_table_name" {
  description = "Athena/Glue table name for organization marts."
  value       = aws_glue_catalog_table.organization_marts.name
}

output "athena_dashboard_table_names" {
  description = "Athena/Glue table names for dashboard datasets."
  value       = { for key, table in aws_glue_catalog_table.dashboard_tables : key => table.name }
}

output "github_actions_role_arn" {
  description = "IAM role ARN that GitHub Actions assumes via OIDC."
  value       = aws_iam_role.github_actions.arn
}
