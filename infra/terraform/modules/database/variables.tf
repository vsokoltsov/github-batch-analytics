variable "eks_cluster_name" { type = string }
variable "vpc_id" { type = string }
variable "private_subnets" { type = list(string) }
variable "node_security_group_id" { type = string }
variable "cluster_primary_security_group_id" { type = string }
variable "airflow_db_name" { type = string }
variable "airflow_db_username" { type = string }
variable "airflow_db_password" {
  type      = string
  sensitive = true
  default   = null
}
variable "airflow_db_instance_class" { type = string }
variable "airflow_db_allocated_storage" { type = number }
variable "airflow_db_max_allocated_storage" { type = number }
variable "tags" { type = map(string) }
