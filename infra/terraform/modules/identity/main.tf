data "aws_iam_policy_document" "airflow_runtime_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.eks_oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${replace(var.eks_cluster_oidc_issuer_url, "https://", "")}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${replace(var.eks_cluster_oidc_issuer_url, "https://", "")}:sub"
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
    var.landing_zone_bucket_arn,
    var.bronze_zone_bucket_arn,
    var.silver_zone_bucket_arn,
    var.marts_bucket_arn,
    var.dlt_state_bucket_arn,
    var.athena_query_results_bucket_arn,
    var.logging_bucket_arn,
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

  statement {
    sid    = "AthenaAndGlueReadWrite"
    effect = "Allow"
    actions = [
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetWorkGroup",
      "athena:StartQueryExecution",
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
    ]
    resources = ["*"]
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

data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "github_actions_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    actions = ["sts:AssumeRoleWithWebIdentity"]

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${var.github_owner}/${var.github_repository}:ref:refs/heads/${var.github_actions_branch}",
      ]
    }
  }
}

data "aws_iam_policy_document" "github_actions_ecr_push" {
  statement {
    effect = "Allow"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:BatchGetImage",
      "ecr:CompleteLayerUpload",
      "ecr:DescribeRepositories",
      "ecr:InitiateLayerUpload",
      "ecr:PutImage",
      "ecr:UploadLayerPart",
    ]
    resources = [var.ecr_repository_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "github_actions_eks_access" {
  statement {
    effect = "Allow"
    actions = [
      "eks:DescribeCluster",
    ]
    resources = [var.eks_cluster_arn]
  }
}

data "aws_iam_policy_document" "github_actions_terraform_state_access" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]
    resources = [var.terraform_state_bucket_arn]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:PutObject",
    ]
    resources = ["${var.terraform_state_bucket_arn}/*"]
  }
}

resource "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"

  client_id_list = ["sts.amazonaws.com"]
  thumbprint_list = [
    "6938fd4d98bab03faadb97b34396831e3780aea1",
  ]

  tags = var.tags
}

resource "aws_iam_role" "github_actions" {
  name               = var.github_actions_role_name
  assume_role_policy = data.aws_iam_policy_document.github_actions_assume_role.json

  tags = merge(
    var.tags,
    {
      Name    = var.github_actions_role_name
      Purpose = "github-actions-ecr-push"
    }
  )
}

resource "aws_iam_policy" "github_actions_ecr_push" {
  name   = "${var.github_actions_role_name}-ecr-push"
  policy = data.aws_iam_policy_document.github_actions_ecr_push.json

  tags = var.tags
}

resource "aws_iam_policy" "github_actions_eks_access" {
  name   = "${var.github_actions_role_name}-eks-access"
  policy = data.aws_iam_policy_document.github_actions_eks_access.json

  tags = var.tags
}

resource "aws_iam_policy" "github_actions_terraform_state_access" {
  name   = "${var.github_actions_role_name}-terraform-state-access"
  policy = data.aws_iam_policy_document.github_actions_terraform_state_access.json

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "github_actions_ecr_push" {
  role       = aws_iam_role.github_actions.name
  policy_arn = aws_iam_policy.github_actions_ecr_push.arn
}

resource "aws_iam_role_policy_attachment" "github_actions_eks_access" {
  role       = aws_iam_role.github_actions.name
  policy_arn = aws_iam_policy.github_actions_eks_access.arn
}

resource "aws_iam_role_policy_attachment" "github_actions_terraform_state_access" {
  role       = aws_iam_role.github_actions.name
  policy_arn = aws_iam_policy.github_actions_terraform_state_access.arn
}

resource "aws_iam_role_policy_attachment" "github_actions_read_only_access" {
  role       = aws_iam_role.github_actions.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

resource "aws_eks_access_entry" "github_actions" {
  cluster_name  = var.eks_cluster_name
  principal_arn = aws_iam_role.github_actions.arn
  type          = "STANDARD"
}

resource "aws_eks_access_policy_association" "github_actions_admin" {
  cluster_name  = var.eks_cluster_name
  principal_arn = aws_iam_role.github_actions.arn
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"

  access_scope {
    type = "cluster"
  }
}
