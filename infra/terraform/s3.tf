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

resource "aws_s3_bucket" "bronze_zone" {
  bucket = var.bronze_zone_bucket_name

  tags = merge(
    var.tags,
    {
      Name    = var.bronze_zone_bucket_name
      Purpose = "bronze-zone-storage"
    }
  )
}

resource "aws_s3_bucket_versioning" "bronze_zone" {
  bucket = aws_s3_bucket.bronze_zone.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze_zone" {
  bucket = aws_s3_bucket.bronze_zone.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "bronze_zone" {
  bucket = aws_s3_bucket.bronze_zone.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "bronze_zone" {
  bucket = aws_s3_bucket.bronze_zone.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket" "silver_zone" {
  bucket = var.silver_zone_bucket_name

  tags = merge(
    var.tags,
    {
      Name    = var.silver_zone_bucket_name
      Purpose = "silver-zone-storage"
    }
  )
}

resource "aws_s3_bucket_versioning" "silver_zone" {
  bucket = aws_s3_bucket.silver_zone.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver_zone" {
  bucket = aws_s3_bucket.silver_zone.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "silver_zone" {
  bucket = aws_s3_bucket.silver_zone.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "silver_zone" {
  bucket = aws_s3_bucket.silver_zone.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket" "dlt_state" {
  bucket = var.dlt_state_bucket_name

  tags = merge(
    var.tags,
    {
      Name    = var.dlt_state_bucket_name
      Purpose = "dlt-pipeline-state-storage"
    }
  )
}

resource "aws_s3_bucket_versioning" "dlt_state" {
  bucket = aws_s3_bucket.dlt_state.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "dlt_state" {
  bucket = aws_s3_bucket.dlt_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "dlt_state" {
  bucket = aws_s3_bucket.dlt_state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "dlt_state" {
  bucket = aws_s3_bucket.dlt_state.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

data "aws_iam_policy_document" "dlt_state_bucket_access" {
  statement {
    sid    = "ListAndLocateBucket"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [aws_s3_bucket.dlt_state.arn]
  }

  statement {
    sid    = "ManageStateObjects"
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]
    resources = ["${aws_s3_bucket.dlt_state.arn}/*"]
  }
}

resource "aws_iam_policy" "dlt_state_bucket_access" {
  name   = "dlt-state-bucket-access"
  policy = data.aws_iam_policy_document.dlt_state_bucket_access.json

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "dlt_state_bucket_access" {
  for_each = toset(var.dlt_state_access_role_names)

  role       = each.value
  policy_arn = aws_iam_policy.dlt_state_bucket_access.arn
}

data "aws_iam_policy_document" "dlt_state_bucket_policy" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.dlt_state.arn,
      "${aws_s3_bucket.dlt_state.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  dynamic "statement" {
    for_each = length(var.dlt_state_access_principal_arns) > 0 ? [1] : []

    content {
      sid    = "AllowConfiguredPrincipals"
      effect = "Allow"
      actions = [
        "s3:GetBucketLocation",
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads",
      ]
      resources = [aws_s3_bucket.dlt_state.arn]

      principals {
        type        = "AWS"
        identifiers = var.dlt_state_access_principal_arns
      }
    }
  }

  dynamic "statement" {
    for_each = length(var.dlt_state_access_principal_arns) > 0 ? [1] : []

    content {
      sid    = "AllowConfiguredPrincipalsObjectAccess"
      effect = "Allow"
      actions = [
        "s3:AbortMultipartUpload",
        "s3:DeleteObject",
        "s3:GetObject",
        "s3:ListMultipartUploadParts",
        "s3:PutObject",
      ]
      resources = ["${aws_s3_bucket.dlt_state.arn}/*"]

      principals {
        type        = "AWS"
        identifiers = var.dlt_state_access_principal_arns
      }
    }
  }
}

resource "aws_s3_bucket_policy" "dlt_state" {
  bucket = aws_s3_bucket.dlt_state.id
  policy = data.aws_iam_policy_document.dlt_state_bucket_policy.json
}
