output "airflow_code_bucket_name" {
  description = "S3 bucket name for Airflow code."
  value       = aws_s3_bucket.airflow_code.bucket
}

output "airflow_code_bucket_arn" {
  description = "S3 bucket ARN for Airflow code."
  value       = aws_s3_bucket.airflow_code.arn
}
