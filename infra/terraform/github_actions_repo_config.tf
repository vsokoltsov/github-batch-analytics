resource "github_actions_secret" "aws_role_to_assume" {
  count = var.manage_github_actions_config ? 1 : 0

  repository      = var.github_repository
  secret_name     = "AWS_ROLE_TO_ASSUME"
  plaintext_value = aws_iam_role.github_actions.arn
}

resource "github_actions_variable" "aws_region" {
  count = var.manage_github_actions_config ? 1 : 0

  repository    = var.github_repository
  variable_name = "AWS_REGION"
  value         = var.aws_region
}

resource "github_actions_variable" "ecr_repository" {
  count = var.manage_github_actions_config ? 1 : 0

  repository    = var.github_repository
  variable_name = "ECR_REPOSITORY"
  value         = var.ecr_repository_name
}
