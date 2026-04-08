resource "github_actions_secret" "aws_role_to_assume" {
  repository      = var.github_repository
  secret_name     = "AWS_ROLE_TO_ASSUME"
  plaintext_value = aws_iam_role.github_actions.arn
}

resource "github_actions_variable" "aws_region" {
  repository    = var.github_repository
  variable_name = "AWS_REGION"
  value         = var.aws_region
}

resource "github_actions_variable" "ecr_repository" {
  repository    = var.github_repository
  variable_name = "ECR_REPOSITORY"
  value         = var.ecr_repository_name
}
