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

variable "tags" {
  description = "Common tags for created resources."
  type        = map(string)
  default = {
    Project     = "github-batch-analytics"
    Environment = "dev"
    ManagedBy   = "terraform"
  }
}
