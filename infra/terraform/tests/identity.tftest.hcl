mock_provider "aws" {}

override_data {
  target = data.aws_iam_policy_document.airflow_runtime_assume_role
  values = {
    id            = "mock-airflow-runtime-assume-role"
    json          = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
  }
}

override_data {
  target = data.aws_iam_policy_document.airflow_runtime
  values = {
    id            = "mock-airflow-runtime-policy"
    json          = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"s3:GetBucketLocation\"],\"Resource\":[\"arn:aws:s3:::athena-results-bucket\"]}]}"
    minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"s3:GetBucketLocation\"],\"Resource\":[\"arn:aws:s3:::athena-results-bucket\"]}]}"
  }
}

override_data {
  target = data.aws_iam_policy_document.github_actions_assume_role
  values = {
    id            = "mock-github-actions-assume-role"
    json          = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
  }
}

override_data {
  target = data.aws_iam_policy_document.github_actions_ecr_push
  values = {
    id            = "mock-github-actions-ecr-push"
    json          = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
  }
}

override_data {
  target = data.aws_iam_policy_document.github_actions_eks_access
  values = {
    id            = "mock-github-actions-eks-access"
    json          = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
  }
}

run "plan_identity_module" {
  command = plan

  module {
    source = "./modules/identity"
  }

  variables {
    eks_oidc_provider_arn           = "arn:aws:iam::123456789012:oidc-provider/oidc.eks.eu-central-1.amazonaws.com/id/example"
    eks_cluster_oidc_issuer_url     = "https://oidc.eks.eu-central-1.amazonaws.com/id/example"
    eks_cluster_name                = "github-batch-analytics"
    eks_cluster_arn                 = "arn:aws:eks:eu-central-1:123456789012:cluster/github-batch-analytics"
    kubernetes_namespace            = "github-batch-analytics"
    kubernetes_service_account_name = "github-batch-analytics"
    airflow_runtime_iam_role_name   = "github-batch-analytics-airflow-runtime"
    github_owner                    = "vsokoltsov"
    github_repository               = "github-batch-analytics"
    github_actions_branch           = "main"
    github_actions_role_name        = "github-actions-ecr-push"
    ecr_repository_arn              = "arn:aws:ecr:eu-central-1:123456789012:repository/github-batch-analytics-airflow"
    landing_zone_bucket_arn         = "arn:aws:s3:::landing-bucket"
    bronze_zone_bucket_arn          = "arn:aws:s3:::bronze-bucket"
    silver_zone_bucket_arn          = "arn:aws:s3:::silver-bucket"
    marts_bucket_arn                = "arn:aws:s3:::marts-bucket"
    dlt_state_bucket_arn            = "arn:aws:s3:::dlt-state-bucket"
    athena_query_results_bucket_arn = "arn:aws:s3:::athena-results-bucket"
    logging_bucket_arn              = "arn:aws:s3:::logging-bucket"
    terraform_state_bucket_arn      = "arn:aws:s3:::terraform-state-bucket"
    tags = {
      Environment = "test"
      ManagedBy   = "terraform"
    }
  }

  assert {
    condition     = aws_iam_role.airflow_runtime.name == "github-batch-analytics-airflow-runtime"
    error_message = "Airflow runtime role name should match the configured input."
  }

  assert {
    condition     = aws_iam_role.github_actions.name == "github-actions-ecr-push"
    error_message = "GitHub Actions role name should match the configured input."
  }

  assert {
    condition     = strcontains(data.aws_iam_policy_document.airflow_runtime.json, "arn:aws:s3:::athena-results-bucket")
    error_message = "Airflow runtime policy should include the Athena results bucket ARN."
  }

  assert {
    condition     = aws_eks_access_entry.github_actions.cluster_name == "github-batch-analytics"
    error_message = "GitHub Actions EKS access entry should target the configured cluster."
  }

  assert {
    condition     = aws_iam_policy.github_actions_terraform_state_access.name == "github-actions-ecr-push-terraform-state-access"
    error_message = "GitHub Actions role should receive a dedicated Terraform state access policy."
  }
}
