data "aws_iam_policy_document" "streamlit_dashboard_access" {
  statement {
    sid    = "AthenaRead"
    effect = "Allow"
    actions = [
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetQueryResultsStream",
      "athena:GetWorkGroup",
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "GlueRead"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:GetTable",
      "glue:GetTables",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "ListDashboardBuckets"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]
    resources = [
      var.marts_bucket_arn,
      var.athena_query_results_bucket_arn,
    ]
  }

  statement {
    sid    = "ReadDashboardData"
    effect = "Allow"
    actions = [
      "s3:GetObject",
    ]
    resources = [
      "${var.marts_bucket_arn}/*",
      "${var.athena_query_results_bucket_arn}/*",
    ]
  }

  statement {
    sid    = "ManageAthenaResults"
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:DeleteObject",
      "s3:PutObject",
    ]
    resources = ["${var.athena_query_results_bucket_arn}/*"]
  }
}

resource "aws_iam_user" "streamlit" {
  name = var.streamlit_iam_user_name

  tags = merge(
    var.tags,
    {
      Name    = var.streamlit_iam_user_name
      Purpose = "streamlit-dashboard"
    }
  )
}

resource "aws_iam_policy" "streamlit_dashboard_access" {
  name        = "${var.streamlit_iam_user_name}-dashboard-access"
  description = "Read-only Athena/Glue/S3 access for the public Streamlit dashboard"
  policy      = data.aws_iam_policy_document.streamlit_dashboard_access.json

  tags = var.tags
}

resource "aws_iam_user_policy_attachment" "streamlit_dashboard_access" {
  user       = aws_iam_user.streamlit.name
  policy_arn = aws_iam_policy.streamlit_dashboard_access.arn
}

resource "aws_iam_access_key" "streamlit" {
  user = aws_iam_user.streamlit.name
}
