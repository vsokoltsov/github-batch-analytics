output "host" { value = aws_db_instance.airflow.address }
output "port" { value = aws_db_instance.airflow.port }
output "name" { value = aws_db_instance.airflow.db_name }
output "username" { value = aws_db_instance.airflow.username }
output "password" {
  value     = local.airflow_db_password
  sensitive = true
}
