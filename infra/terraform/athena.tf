resource "aws_s3_bucket" "athena_query_results" {
  bucket = var.athena_query_results_bucket_name

  tags = merge(
    var.tags,
    {
      Name    = var.athena_query_results_bucket_name
      Purpose = "athena-query-results"
    }
  )
}

resource "aws_s3_bucket_versioning" "athena_query_results" {
  bucket = aws_s3_bucket.athena_query_results.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_query_results" {
  bucket = aws_s3_bucket.athena_query_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "athena_query_results" {
  bucket = aws_s3_bucket.athena_query_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "athena_query_results" {
  bucket = aws_s3_bucket.athena_query_results.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_glue_catalog_database" "analytics" {
  name = var.athena_database_name

  description = "Analytics metadata database for GitHub batch analytics"
}

resource "aws_athena_workgroup" "analytics" {
  name = var.athena_workgroup_name

  configuration {
    enforce_workgroup_configuration    = var.athena_enforce_workgroup_configuration
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_query_results.bucket}/results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  state = "ENABLED"

  tags = merge(
    var.tags,
    {
      Name    = var.athena_workgroup_name
      Purpose = "athena-workgroup"
    }
  )
}
