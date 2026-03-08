output "landing_zone_bucket_name" {
  description = "S3 bucket name for landing zone."
  value       = aws_s3_bucket.landing_zone.bucket
}

output "landing_zone_bucket_arn" {
  description = "S3 bucket ARN for landing zone."
  value       = aws_s3_bucket.landing_zone.arn
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
