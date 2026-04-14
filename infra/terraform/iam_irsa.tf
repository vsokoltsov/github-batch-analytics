data "aws_iam_policy_document" "airflow_runtime_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:sub"
      values   = ["system:serviceaccount:${var.kubernetes_namespace}:${var.kubernetes_service_account_name}"]
    }
  }
}

resource "aws_iam_role" "airflow_runtime" {
  name               = var.airflow_runtime_iam_role_name
  assume_role_policy = data.aws_iam_policy_document.airflow_runtime_assume_role.json

  tags = merge(
    var.tags,
    {
      Name    = var.airflow_runtime_iam_role_name
      Purpose = "airflow-irsa"
    }
  )
}

locals {
  airflow_runtime_bucket_arns = [
    aws_s3_bucket.landing_zone.arn,
    aws_s3_bucket.bronze_zone.arn,
    aws_s3_bucket.silver_zone.arn,
    aws_s3_bucket.marts.arn,
    aws_s3_bucket.dlt_state.arn,
    aws_s3_bucket.athena_query_results.arn,
    aws_s3_bucket.logging.arn,
  ]

  airflow_runtime_object_arns = [for arn in local.airflow_runtime_bucket_arns : "${arn}/*"]
}

data "aws_iam_policy_document" "airflow_runtime" {
  statement {
    sid    = "ListBuckets"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]
    resources = local.airflow_runtime_bucket_arns
  }

  statement {
    sid    = "ManageObjects"
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]
    resources = local.airflow_runtime_object_arns
  }
}

resource "aws_iam_policy" "airflow_runtime" {
  name        = "${var.eks_cluster_name}-airflow-runtime"
  description = "Runtime access for Airflow and Spark pods"
  policy      = data.aws_iam_policy_document.airflow_runtime.json

  tags = merge(
    var.tags,
    {
      Name    = "${var.eks_cluster_name}-airflow-runtime"
      Purpose = "airflow-s3-access"
    }
  )
}

resource "aws_iam_role_policy_attachment" "airflow_runtime" {
  role       = aws_iam_role.airflow_runtime.name
  policy_arn = aws_iam_policy.airflow_runtime.arn
}
