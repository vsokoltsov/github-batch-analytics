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

output "github_actions_role_arn" {
  description = "IAM role ARN that GitHub Actions assumes via OIDC."
  value       = aws_iam_role.github_actions.arn
}
