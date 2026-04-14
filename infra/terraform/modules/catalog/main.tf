resource "aws_s3_bucket" "athena_query_results" {
  bucket = var.athena_query_results_bucket_name

  tags = merge(
    var.tags,
    {
      Name    = var.athena_query_results_bucket_name
      Purpose = "athena-query-results"
    }
  )
}

resource "aws_s3_bucket_versioning" "athena_query_results" {
  bucket = aws_s3_bucket.athena_query_results.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_query_results" {
  bucket = aws_s3_bucket.athena_query_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "athena_query_results" {
  bucket = aws_s3_bucket.athena_query_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "athena_query_results" {
  bucket = aws_s3_bucket.athena_query_results.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_glue_catalog_database" "analytics" {
  name = var.athena_database_name

  description = "Analytics metadata database for GitHub batch analytics"
}

resource "aws_athena_workgroup" "analytics" {
  name = var.athena_workgroup_name

  configuration {
    enforce_workgroup_configuration    = var.athena_enforce_workgroup_configuration
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_query_results.bucket}/results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  state = "ENABLED"

  tags = merge(
    var.tags,
    {
      Name    = var.athena_workgroup_name
      Purpose = "athena-workgroup"
    }
  )
}

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

  repository_dashboard_summary_columns = [
    { name = "repo_id", type = "bigint" },
    { name = "repo_full_name", type = "string" },
    { name = "repo_name", type = "string" },
    { name = "owner_login", type = "string" },
    { name = "owner_type", type = "string" },
    { name = "language", type = "string" },
    { name = "visibility", type = "string" },
    { name = "is_fork", type = "boolean" },
    { name = "is_archived", type = "boolean" },
    { name = "is_disabled", type = "boolean" },
    { name = "stargazers_count", type = "bigint" },
    { name = "forks_count", type = "bigint" },
    { name = "watchers_count", type = "bigint" },
    { name = "subscribers_count", type = "bigint" },
    { name = "open_issues_count", type = "bigint" },
    { name = "total_events", type = "bigint" },
    { name = "push_events", type = "bigint" },
    { name = "pull_request_events", type = "bigint" },
    { name = "issue_comment_events", type = "bigint" },
    { name = "fork_events", type = "bigint" },
    { name = "avg_composite_score", type = "double" },
    { name = "avg_bot_ratio", type = "double" },
  ]

  repository_dashboard_event_type_columns = [
    { name = "event_type", type = "string" },
    { name = "event_count", type = "bigint" },
  ]

  repository_dashboard_fork_columns = [
    { name = "fork_status", type = "string" },
    { name = "repo_count", type = "bigint" },
    { name = "total_events", type = "bigint" },
    { name = "avg_composite_score", type = "double" },
  ]

  repository_dashboard_freshness_columns = [
    { name = "freshness_bucket", type = "string" },
    { name = "repo_count", type = "bigint" },
    { name = "total_events", type = "bigint" },
  ]

  repository_dashboard_language_columns = [
    { name = "language", type = "string" },
    { name = "repo_count", type = "bigint" },
    { name = "total_events", type = "bigint" },
    { name = "avg_composite_score", type = "double" },
    { name = "total_stargazers", type = "bigint" },
  ]

  repository_dashboard_owner_columns = [
    { name = "owner_type", type = "string" },
    { name = "repo_count", type = "bigint" },
    { name = "total_events", type = "bigint" },
    { name = "avg_composite_score", type = "double" },
  ]

  repository_dashboard_top_100_columns = [
    { name = "repo_id", type = "bigint" },
    { name = "repo_full_name", type = "string" },
    { name = "owner_login", type = "string" },
    { name = "language", type = "string" },
    { name = "total_events", type = "bigint" },
    { name = "unique_actors", type = "bigint" },
    { name = "composite_score", type = "double" },
    { name = "stargazers_count", type = "bigint" },
    { name = "forks_count", type = "bigint" },
  ]

  repository_dashboard_visibility_columns = [
    { name = "visibility", type = "string" },
    { name = "repo_count", type = "bigint" },
    { name = "total_events", type = "bigint" },
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

  organization_dashboard_summary_columns = [
    { name = "org_id", type = "bigint" },
    { name = "org_login", type = "string" },
    { name = "org_name", type = "string" },
    { name = "location", type = "string" },
    { name = "company", type = "string" },
    { name = "blog", type = "string" },
    { name = "email", type = "string" },
    { name = "twitter_username", type = "string" },
    { name = "is_verified", type = "boolean" },
    { name = "has_organization_projects", type = "boolean" },
    { name = "has_repository_projects", type = "boolean" },
    { name = "public_repos", type = "bigint" },
    { name = "public_gists", type = "bigint" },
    { name = "followers", type = "bigint" },
    { name = "following", type = "bigint" },
    { name = "total_events", type = "bigint" },
    { name = "push_events", type = "bigint" },
    { name = "pull_request_events", type = "bigint" },
    { name = "avg_composite_score", type = "double" },
    { name = "avg_bot_ratio", type = "double" },
  ]

  organization_dashboard_company_columns = [
    { name = "company", type = "string" },
    { name = "org_count", type = "bigint" },
    { name = "total_events", type = "bigint" },
  ]

  organization_dashboard_location_columns = [
    { name = "location", type = "string" },
    { name = "org_count", type = "bigint" },
    { name = "total_events", type = "bigint" },
    { name = "avg_followers", type = "double" },
  ]

  organization_dashboard_size_columns = [
    { name = "size_bucket", type = "string" },
    { name = "org_count", type = "bigint" },
    { name = "total_events", type = "bigint" },
    { name = "avg_followers", type = "double" },
  ]

  organization_dashboard_social_columns = [
    { name = "twitter_status", type = "string" },
    { name = "blog_status", type = "string" },
    { name = "email_status", type = "string" },
    { name = "org_count", type = "bigint" },
  ]

  organization_dashboard_top_100_columns = [
    { name = "org_id", type = "bigint" },
    { name = "org_login", type = "string" },
    { name = "org_name", type = "string" },
    { name = "location", type = "string" },
    { name = "public_repos", type = "bigint" },
    { name = "followers", type = "bigint" },
    { name = "repos_count", type = "bigint" },
    { name = "total_events", type = "bigint" },
    { name = "composite_score", type = "double" },
  ]

  organization_dashboard_verified_distribution_columns = [
    { name = "verification_status", type = "string" },
    { name = "org_count", type = "bigint" },
    { name = "total_events", type = "bigint" },
    { name = "avg_composite_score", type = "double" },
  ]

  common_dashboard_rollup_columns = [
    { name = "repo_id", type = "bigint" },
    { name = "repo_full_name", type = "string" },
    { name = "repo_name", type = "string" },
    { name = "owner_login", type = "string" },
    { name = "language", type = "string" },
    { name = "repo_total_events", type = "bigint" },
    { name = "repo_avg_composite_score", type = "double" },
    { name = "stargazers_count", type = "bigint" },
    { name = "org_id", type = "bigint" },
    { name = "org_name", type = "string" },
    { name = "org_location", type = "string" },
    { name = "org_company", type = "string" },
    { name = "org_followers", type = "bigint" },
    { name = "org_public_repos", type = "bigint" },
    { name = "org_is_verified", type = "boolean" },
  ]

  common_dashboard_language_location_columns = [
    { name = "language", type = "string" },
    { name = "org_location", type = "string" },
    { name = "repo_count", type = "bigint" },
    { name = "total_events", type = "bigint" },
  ]

  common_dashboard_verified_columns = [
    { name = "verification_status", type = "string" },
    { name = "repo_count", type = "bigint" },
    { name = "total_events", type = "bigint" },
    { name = "avg_repo_score", type = "double" },
  ]

  dashboard_table_definitions = {
    repository_dashboard_summary = {
      location_prefix = "dashboards/repositories/summary"
      columns         = local.repository_dashboard_summary_columns
    }
    repository_dashboard_event_type = {
      location_prefix = "dashboards/repositories/event_type"
      columns         = local.repository_dashboard_event_type_columns
    }
    repository_dashboard_fork = {
      location_prefix = "dashboards/repositories/fork"
      columns         = local.repository_dashboard_fork_columns
    }
    repository_dashboard_freshness = {
      location_prefix = "dashboards/repositories/freshness"
      columns         = local.repository_dashboard_freshness_columns
    }
    repository_dashboard_language = {
      location_prefix = "dashboards/repositories/language"
      columns         = local.repository_dashboard_language_columns
    }
    repository_dashboard_owner = {
      location_prefix = "dashboards/repositories/owner"
      columns         = local.repository_dashboard_owner_columns
    }
    repository_dashboard_top_100 = {
      location_prefix = "dashboards/repositories/top_100"
      columns         = local.repository_dashboard_top_100_columns
    }
    repository_dashboard_visibility = {
      location_prefix = "dashboards/repositories/visibility"
      columns         = local.repository_dashboard_visibility_columns
    }
    organization_dashboard_summary = {
      location_prefix = "dashboards/organizations/summary"
      columns         = local.organization_dashboard_summary_columns
    }
    organization_dashboard_company = {
      location_prefix = "dashboards/organizations/company"
      columns         = local.organization_dashboard_company_columns
    }
    organization_dashboard_location = {
      location_prefix = "dashboards/organizations/location"
      columns         = local.organization_dashboard_location_columns
    }
    organization_dashboard_size = {
      location_prefix = "dashboards/organizations/size"
      columns         = local.organization_dashboard_size_columns
    }
    organization_dashboard_social = {
      location_prefix = "dashboards/organizations/social"
      columns         = local.organization_dashboard_social_columns
    }
    organization_dashboard_top_100 = {
      location_prefix = "dashboards/organizations/top_100"
      columns         = local.organization_dashboard_top_100_columns
    }
    organization_dashboard_verified_distribution = {
      location_prefix = "dashboards/organizations/verified_distribution"
      columns         = local.organization_dashboard_verified_distribution_columns
    }
    common_dashboard_rollup = {
      location_prefix = "dashboards/common/summary"
      columns         = local.common_dashboard_rollup_columns
    }
    common_dashboard_language_location = {
      location_prefix = "dashboards/common/language_location"
      columns         = local.common_dashboard_language_location_columns
    }
    common_dashboard_verified = {
      location_prefix = "dashboards/common/verified"
      columns         = local.common_dashboard_verified_columns
    }
  }
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
    "storage.location.template"   = "s3://${var.marts_bucket_name}/repositories/dt=$${dt}/hr=$${hr}/"
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
    location      = "s3://${var.marts_bucket_name}/repositories/"
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
    "storage.location.template"   = "s3://${var.marts_bucket_name}/organizations/dt=$${dt}/hr=$${hr}/"
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
    location      = "s3://${var.marts_bucket_name}/organizations/"
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

resource "aws_glue_catalog_table" "dashboard_tables" {
  for_each      = local.dashboard_table_definitions
  name          = each.key
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
    "storage.location.template"   = "s3://${var.marts_bucket_name}/${each.value.location_prefix}/dt=$${dt}/hr=$${hr}/"
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
    location      = "s3://${var.marts_bucket_name}/${each.value.location_prefix}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = each.value.columns

      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}
