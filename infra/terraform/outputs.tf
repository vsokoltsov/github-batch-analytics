output "landing_zone_bucket_name" {
  description = "S3 bucket name for landing zone."
  value       = aws_s3_bucket.landing_zone.bucket
}

output "landing_zone_bucket_arn" {
  description = "S3 bucket ARN for landing zone."
  value       = aws_s3_bucket.landing_zone.arn
}
