mock_provider "aws" {}

override_data {
  target = data.aws_iam_policy_document.dlt_state_bucket_policy
  values = {
    id            = "mock-dlt-state-bucket-policy"
    json          = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"arn:aws:iam::123456789012:role/github-actions\",\"arn:aws:iam::123456789012:role/airflow-runtime\"]},\"Action\":[\"s3:GetObject\"],\"Resource\":[\"arn:aws:s3:::dlt-state-bucket/*\"]}]}"
    minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"arn:aws:iam::123456789012:role/github-actions\",\"arn:aws:iam::123456789012:role/airflow-runtime\"]},\"Action\":[\"s3:GetObject\"],\"Resource\":[\"arn:aws:s3:::dlt-state-bucket/*\"]}]}"
  }
}

run "plan_storage_module" {
  command = plan

  module {
    source = "./modules/storage"
  }

  variables {
    landing_zone_bucket_name = "landing-bucket"
    bronze_zone_bucket_name  = "bronze-bucket"
    silver_zone_bucket_name  = "silver-bucket"
    marts_bucket_name        = "marts-bucket"
    dlt_state_bucket_name    = "dlt-state-bucket"
    logging_bucket_name      = "logging-bucket"
    dlt_state_access_role_names = [
      "github-actions",
      "airflow-runtime",
    ]
    dlt_state_access_principal_arns = [
      "arn:aws:iam::123456789012:role/github-actions",
      "arn:aws:iam::123456789012:role/airflow-runtime",
    ]
    tags = {
      Environment = "test"
      ManagedBy   = "terraform"
    }
  }

  assert {
    condition     = aws_s3_bucket.landing_zone.bucket == "landing-bucket"
    error_message = "Landing zone bucket name should match the input variable."
  }

  assert {
    condition     = aws_s3_bucket.logging.bucket == "logging-bucket"
    error_message = "Logging bucket name should match the input variable."
  }

  assert {
    condition     = length(aws_iam_role_policy_attachment.dlt_state_bucket_access) == 2
    error_message = "One policy attachment should be planned per configured role name."
  }

}
