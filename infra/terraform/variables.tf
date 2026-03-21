variable "aws_region" {
  description = "AWS region for resources."
  type        = string
  default     = "eu-central-1"
}

variable "aws_profile" {
  description = "Named AWS profile from ~/.aws/config and ~/.aws/credentials."
  type        = string
  default     = "gba-admin"
}

variable "landing_zone_bucket_name" {
  description = "Globally unique S3 bucket name for landing zone raw data."
  type        = string
}

variable "bronze_zone_bucket_name" {
  description = "Globally unique S3 bucket name for bronze-layer transformed data."
  type        = string
}

variable "silver_zone_bucket_name" {
  description = "Globally unique S3 bucket name for silver-layer aggregated data."
  type        = string
}

variable "ecr_repository_name" {
  description = "ECR repository name for Airflow image artifacts."
  type        = string
  default     = "github-batch-analytics-airflow"
}

variable "github_owner" {
  description = "GitHub owner/org name for this repository."
  type        = string
  default     = ""
}

variable "github_repository" {
  description = "GitHub repository name."
  type        = string
  default     = ""
}

variable "github_token" {
  description = "GitHub token used by Terraform to manage repository secrets/variables."
  type        = string
  sensitive   = true
  default     = null
}

variable "github_actions_branch" {
  description = "Branch allowed to assume the GitHub Actions IAM role."
  type        = string
  default     = "main"
}

variable "manage_github_actions_config" {
  description = "Whether Terraform should create GitHub Actions secret/variables."
  type        = bool
  default     = false
}

variable "github_actions_role_name" {
  description = "IAM role name for GitHub Actions OIDC."
  type        = string
  default     = "github-actions-ecr-push"
}

variable "tags" {
  description = "Common tags for created resources."
  type        = map(string)
  default = {
    Project     = "github-batch-analytics"
    Environment = "dev"
    ManagedBy   = "terraform"
  }
}
