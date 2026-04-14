output "athena_query_results_bucket_name" { value = aws_s3_bucket.athena_query_results.bucket }
output "athena_query_results_bucket_arn" { value = aws_s3_bucket.athena_query_results.arn }
output "athena_database_name" { value = aws_glue_catalog_database.analytics.name }
output "athena_workgroup_name" { value = aws_athena_workgroup.analytics.name }
output "athena_repository_table_name" { value = aws_glue_catalog_table.repository_marts.name }
output "athena_organization_table_name" { value = aws_glue_catalog_table.organization_marts.name }
output "athena_dashboard_table_names" { value = { for key, table in aws_glue_catalog_table.dashboard_tables : key => table.name } }
output "athena_query_results_bucket_name_raw" { value = aws_s3_bucket.athena_query_results.bucket }
output "athena_query_results_bucket_arn_raw" { value = aws_s3_bucket.athena_query_results.arn }
