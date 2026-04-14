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

variable "marts_bucket_name" {
  description = "Globally unique S3 bucket name for curated mart data used by dashboards."
  type        = string
}

variable "dlt_state_bucket_name" {
  description = "Globally unique S3 bucket name for persisted dlt pipeline state."
  type        = string
}

variable "logging_bucket_name" {
  description = "S3 bucket for Airflow remote logs."
  type        = string
}

variable "ecr_repository_name" {
  description = "ECR repository name for Airflow image artifacts."
  type        = string
  default     = "github-batch-analytics-airflow"
}

variable "athena_database_name" {
  description = "Glue/Athena database name for analytics tables."
  type        = string
  default     = "github_analytics"
}

variable "athena_workgroup_name" {
  description = "Athena workgroup name."
  type        = string
  default     = "github-batch-analytics"
}

variable "athena_query_results_bucket_name" {
  description = "S3 bucket name used by Athena for query results."
  type        = string
}

variable "terraform_state_bucket_name" {
  description = "S3 bucket name used for the Terraform remote state backend."
  type        = string
  default     = "gba-terraform-state-prod"
}

variable "athena_enforce_workgroup_configuration" {
  description = "Whether Athena should enforce workgroup-level settings."
  type        = bool
  default     = true
}

variable "athena_partition_projection_start_date" {
  description = "Earliest partition date exposed through Athena partition projection."
  type        = string
  default     = "2026-01-01"
}

variable "athena_repository_table_name" {
  description = "Athena/Glue table name for repository marts."
  type        = string
  default     = "repositories"
}

variable "athena_organization_table_name" {
  description = "Athena/Glue table name for organization marts."
  type        = string
  default     = "organizations"
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

variable "dlt_state_access_role_names" {
  description = "IAM role names that should receive the managed policy for the dlt state bucket."
  type        = list(string)
  default     = []
}

variable "dlt_state_access_principal_arns" {
  description = "IAM principal ARNs that should be allowed in the dlt state bucket policy."
  type        = list(string)
  default     = []
}

variable "vpc_cidr" {
  description = "CIDR block for the EKS VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "eks_cluster_name" {
  description = "EKS cluster name."
  type        = string
  default     = "github-batch-analytics"
}

variable "eks_cluster_version" {
  description = "EKS Kubernetes version."
  type        = string
  default     = "1.31"
}

variable "eks_node_instance_type" {
  description = "Instance type for the default EKS managed node group."
  type        = string
  default     = "t3.large"
}

variable "eks_node_desired_size" {
  description = "Desired node count for the default EKS managed node group."
  type        = number
  default     = 2
}

variable "eks_node_min_size" {
  description = "Minimum node count for the default EKS managed node group."
  type        = number
  default     = 1
}

variable "eks_node_max_size" {
  description = "Maximum node count for the default EKS managed node group."
  type        = number
  default     = 3
}

variable "kubernetes_namespace" {
  description = "Kubernetes namespace used for the Airflow deployment."
  type        = string
  default     = "github-batch-analytics"
}

variable "kubernetes_service_account_name" {
  description = "Kubernetes service account name used by Airflow and Spark pods."
  type        = string
  default     = "github-batch-analytics"
}

variable "airflow_runtime_iam_role_name" {
  description = "IAM role name used for IRSA by Airflow and Spark pods."
  type        = string
  default     = "github-batch-analytics-airflow-runtime"
}

variable "airflow_db_name" {
  description = "PostgreSQL database name for Airflow."
  type        = string
  default     = "airflow"
}

variable "airflow_db_username" {
  description = "PostgreSQL username for Airflow."
  type        = string
  default     = "airflow"
}

variable "airflow_db_password" {
  description = "Optional PostgreSQL password for Airflow. When null, Terraform generates one."
  type        = string
  sensitive   = true
  default     = null
}

variable "airflow_db_instance_class" {
  description = "RDS instance class for Airflow PostgreSQL."
  type        = string
  default     = "db.t4g.micro"
}

variable "airflow_db_allocated_storage" {
  description = "Allocated storage in GiB for Airflow PostgreSQL."
  type        = number
  default     = 20
}

variable "airflow_db_max_allocated_storage" {
  description = "Maximum autoscaled storage in GiB for Airflow PostgreSQL."
  type        = number
  default     = 100
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
