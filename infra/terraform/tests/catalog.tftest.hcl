mock_provider "aws" {}

run "plan_catalog_module" {
  command = plan

  module {
    source = "./modules/catalog"
  }

  variables {
    athena_query_results_bucket_name       = "athena-results-bucket"
    athena_database_name                   = "github_analytics"
    athena_workgroup_name                  = "github-batch-analytics"
    athena_enforce_workgroup_configuration = false
    athena_partition_projection_start_date = "2026-01-01"
    athena_repository_table_name           = "repositories"
    athena_organization_table_name         = "organizations"
    athena_repository_bucket_count         = 16
    athena_organization_bucket_count       = 8
    marts_bucket_name                      = "marts-bucket"
    tags = {
      Environment = "test"
      ManagedBy   = "terraform"
    }
  }

  assert {
    condition     = aws_s3_bucket.athena_query_results.bucket == "athena-results-bucket"
    error_message = "Athena query results bucket should match the input variable."
  }

  assert {
    condition     = aws_athena_workgroup.analytics.name == "github-batch-analytics"
    error_message = "Athena workgroup name should match the configured input."
  }

  assert {
    condition     = aws_glue_catalog_table.repository_marts.name == "repositories"
    error_message = "Repository marts table name should match the configured input."
  }

  assert {
    condition     = aws_glue_catalog_table.repository_marts_stage.name == "repositories_stage"
    error_message = "Repository stage table should be present for Athena bucket materialization."
  }

  assert {
    condition     = aws_glue_catalog_table.organization_marts.storage_descriptor[0].number_of_buckets == 8
    error_message = "Organization marts table should keep the configured bucket count."
  }

  assert {
    condition     = aws_glue_catalog_table.dashboard_tables["repository_dashboard_summary"].name == "repository_dashboard_summary"
    error_message = "Repository dashboard summary table should be present."
  }
}
