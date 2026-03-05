resource "aws_s3_bucket" "airflow_code" {
  bucket = var.airflow_code_bucket_name

  tags = merge(
    var.tags,
    {
      Name    = var.airflow_code_bucket_name
      Purpose = "airflow-code-storage"
    }
  )
}

resource "aws_s3_bucket_versioning" "airflow_code" {
  bucket = aws_s3_bucket.airflow_code.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "airflow_code" {
  bucket = aws_s3_bucket.airflow_code.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "airflow_code" {
  bucket = aws_s3_bucket.airflow_code.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "airflow_code" {
  bucket = aws_s3_bucket.airflow_code.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}
