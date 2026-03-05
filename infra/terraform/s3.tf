resource "aws_s3_bucket" "landing_zone" {
  bucket = var.landing_zone_bucket_name

  tags = merge(
    var.tags,
    {
      Name    = var.landing_zone_bucket_name
      Purpose = "landing-zone-storage"
    }
  )
}

resource "aws_s3_bucket_versioning" "landing_zone" {
  bucket = aws_s3_bucket.landing_zone.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "landing_zone" {
  bucket = aws_s3_bucket.landing_zone.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "landing_zone" {
  bucket = aws_s3_bucket.landing_zone.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "landing_zone" {
  bucket = aws_s3_bucket.landing_zone.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}
