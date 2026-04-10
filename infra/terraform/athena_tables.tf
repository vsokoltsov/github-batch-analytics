locals {
  athena_partition_keys = [
    {
      name = "dt"
      type = "date"
    },
    {
      name = "hr"
      type = "int"
    },
  ]

  repository_mart_columns = [
    { name = "repo_id", type = "bigint" },
    { name = "repo_full_name", type = "string" },
    { name = "total_events", type = "bigint" },
    { name = "public_events_count", type = "bigint" },
    { name = "unique_actors", type = "bigint" },
    { name = "last_event_at", type = "timestamp" },
    { name = "bot_events", type = "bigint" },
    { name = "issues_count", type = "bigint" },
    { name = "comments_count", type = "bigint" },
    { name = "releases_count", type = "bigint" },
    { name = "merged_pr_count", type = "bigint" },
    { name = "pull_request_review_events", type = "bigint" },
    { name = "push_events", type = "bigint" },
    { name = "gollum_events", type = "bigint" },
    { name = "release_events", type = "bigint" },
    { name = "commit_comment_events", type = "bigint" },
    { name = "create_events", type = "bigint" },
    { name = "pull_request_review_comment_events", type = "bigint" },
    { name = "issue_comment_events", type = "bigint" },
    { name = "delete_events", type = "bigint" },
    { name = "issues_events", type = "bigint" },
    { name = "fork_events", type = "bigint" },
    { name = "public_events", type = "bigint" },
    { name = "member_events", type = "bigint" },
    { name = "watch_events", type = "bigint" },
    { name = "pull_request_events", type = "bigint" },
    { name = "discussion_events", type = "bigint" },
    { name = "pull_request_review_events_ratio", type = "double" },
    { name = "push_events_ratio", type = "double" },
    { name = "gollum_events_ratio", type = "double" },
    { name = "release_events_ratio", type = "double" },
    { name = "commit_comment_events_ratio", type = "double" },
    { name = "create_events_ratio", type = "double" },
    { name = "pull_request_review_comment_events_ratio", type = "double" },
    { name = "issue_comment_events_ratio", type = "double" },
    { name = "delete_events_ratio", type = "double" },
    { name = "issues_events_ratio", type = "double" },
    { name = "fork_events_ratio", type = "double" },
    { name = "public_events_ratio", type = "double" },
    { name = "member_events_ratio", type = "double" },
    { name = "watch_events_ratio", type = "double" },
    { name = "pull_request_events_ratio", type = "double" },
    { name = "discussion_events_ratio", type = "double" },
    { name = "composite_score", type = "double" },
    { name = "bot_ratio", type = "double" },
    { name = "selection_reasons", type = "array<string>" },
    { name = "repo_name", type = "string" },
    { name = "owner_login", type = "string" },
    { name = "owner_id", type = "bigint" },
    { name = "owner_type", type = "string" },
    { name = "description", type = "string" },
    { name = "language", type = "string" },
    { name = "license_spdx_id", type = "string" },
    { name = "default_branch", type = "string" },
    { name = "visibility", type = "string" },
    { name = "is_fork", type = "boolean" },
    { name = "is_archived", type = "boolean" },
    { name = "is_disabled", type = "boolean" },
    { name = "stargazers_count", type = "bigint" },
    { name = "forks_count", type = "bigint" },
    { name = "watchers_count", type = "bigint" },
    { name = "subscribers_count", type = "bigint" },
    { name = "open_issues_count", type = "bigint" },
    { name = "created_at", type = "timestamp" },
    { name = "updated_at", type = "timestamp" },
    { name = "pushed_at", type = "timestamp" },
    { name = "api_fetched_at", type = "timestamp" },
    { name = "_dlt_load_id", type = "string" },
    { name = "_dlt_id", type = "string" },
  ]

  organization_mart_columns = [
    { name = "org_id", type = "bigint" },
    { name = "org_login", type = "string" },
    { name = "total_events", type = "bigint" },
    { name = "public_events_count", type = "bigint" },
    { name = "unique_actors", type = "bigint" },
    { name = "last_event_at", type = "timestamp" },
    { name = "bot_events", type = "bigint" },
    { name = "issues_count", type = "bigint" },
    { name = "comments_count", type = "bigint" },
    { name = "releases_count", type = "bigint" },
    { name = "merged_pr_count", type = "bigint" },
    { name = "pull_request_review_events", type = "bigint" },
    { name = "push_events", type = "bigint" },
    { name = "gollum_events", type = "bigint" },
    { name = "release_events", type = "bigint" },
    { name = "commit_comment_events", type = "bigint" },
    { name = "create_events", type = "bigint" },
    { name = "pull_request_review_comment_events", type = "bigint" },
    { name = "issue_comment_events", type = "bigint" },
    { name = "delete_events", type = "bigint" },
    { name = "issues_events", type = "bigint" },
    { name = "fork_events", type = "bigint" },
    { name = "public_events", type = "bigint" },
    { name = "member_events", type = "bigint" },
    { name = "watch_events", type = "bigint" },
    { name = "pull_request_events", type = "bigint" },
    { name = "discussion_events", type = "bigint" },
    { name = "repos_count", type = "bigint" },
    { name = "pull_request_review_events_ratio", type = "double" },
    { name = "push_events_ratio", type = "double" },
    { name = "gollum_events_ratio", type = "double" },
    { name = "release_events_ratio", type = "double" },
    { name = "commit_comment_events_ratio", type = "double" },
    { name = "create_events_ratio", type = "double" },
    { name = "pull_request_review_comment_events_ratio", type = "double" },
    { name = "issue_comment_events_ratio", type = "double" },
    { name = "delete_events_ratio", type = "double" },
    { name = "issues_events_ratio", type = "double" },
    { name = "fork_events_ratio", type = "double" },
    { name = "public_events_ratio", type = "double" },
    { name = "member_events_ratio", type = "double" },
    { name = "watch_events_ratio", type = "double" },
    { name = "pull_request_events_ratio", type = "double" },
    { name = "discussion_events_ratio", type = "double" },
    { name = "composite_score", type = "double" },
    { name = "bot_ratio", type = "double" },
    { name = "selection_reasons", type = "array<string>" },
    { name = "org_name", type = "string" },
    { name = "blog", type = "string" },
    { name = "email", type = "string" },
    { name = "description", type = "string" },
    { name = "is_verified", type = "boolean" },
    { name = "has_organization_projects", type = "boolean" },
    { name = "has_repository_projects", type = "boolean" },
    { name = "public_repos", type = "bigint" },
    { name = "public_gists", type = "bigint" },
    { name = "followers", type = "bigint" },
    { name = "following", type = "bigint" },
    { name = "html_url", type = "string" },
    { name = "created_at", type = "timestamp" },
    { name = "updated_at", type = "timestamp" },
    { name = "api_fetched_at", type = "timestamp" },
    { name = "_dlt_load_id", type = "string" },
    { name = "_dlt_id", type = "string" },
    { name = "location", type = "string" },
    { name = "twitter_username", type = "string" },
    { name = "company", type = "string" },
  ]
}

resource "aws_glue_catalog_table" "repository_marts" {
  name          = var.athena_repository_table_name
  database_name = aws_glue_catalog_database.analytics.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                      = "TRUE"
    classification                = "parquet"
    "parquet.compression"         = "SNAPPY"
    "projection.enabled"          = "true"
    "projection.dt.type"          = "date"
    "projection.dt.range"         = "${var.athena_partition_projection_start_date},NOW"
    "projection.dt.format"        = "yyyy-MM-dd"
    "projection.dt.interval"      = "1"
    "projection.dt.interval.unit" = "DAYS"
    "projection.hr.type"          = "integer"
    "projection.hr.range"         = "0,23"
    "storage.location.template"   = "s3://${aws_s3_bucket.marts.bucket}/repositories/dt=$${dt}/hr=$${hr}/"
    typeOfData                    = "file"
  }

  dynamic "partition_keys" {
    for_each = local.athena_partition_keys

    content {
      name = partition_keys.value.name
      type = partition_keys.value.type
    }
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.marts.bucket}/repositories/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = local.repository_mart_columns

      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

resource "aws_glue_catalog_table" "organization_marts" {
  name          = var.athena_organization_table_name
  database_name = aws_glue_catalog_database.analytics.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                      = "TRUE"
    classification                = "parquet"
    "parquet.compression"         = "SNAPPY"
    "projection.enabled"          = "true"
    "projection.dt.type"          = "date"
    "projection.dt.range"         = "${var.athena_partition_projection_start_date},NOW"
    "projection.dt.format"        = "yyyy-MM-dd"
    "projection.dt.interval"      = "1"
    "projection.dt.interval.unit" = "DAYS"
    "projection.hr.type"          = "integer"
    "projection.hr.range"         = "0,23"
    "storage.location.template"   = "s3://${aws_s3_bucket.marts.bucket}/organizations/dt=$${dt}/hr=$${hr}/"
    typeOfData                    = "file"
  }

  dynamic "partition_keys" {
    for_each = local.athena_partition_keys

    content {
      name = partition_keys.value.name
      type = partition_keys.value.type
    }
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.marts.bucket}/organizations/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = local.organization_mart_columns

      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}
