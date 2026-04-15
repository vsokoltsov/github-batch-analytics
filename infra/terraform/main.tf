module "network" {
  source = "./modules/network"

  vpc_cidr         = var.vpc_cidr
  eks_cluster_name = var.eks_cluster_name
  tags             = var.tags
}

module "eks_cluster" {
  source = "./modules/eks_cluster"

  eks_cluster_name       = var.eks_cluster_name
  eks_cluster_version    = var.eks_cluster_version
  vpc_id                 = module.network.vpc_id
  subnet_ids             = module.network.private_subnets
  eks_node_instance_type = var.eks_node_instance_type
  eks_node_disk_size     = var.eks_node_disk_size
  eks_node_desired_size  = var.eks_node_desired_size
  eks_node_min_size      = var.eks_node_min_size
  eks_node_max_size      = var.eks_node_max_size
  tags                   = var.tags
}

module "storage" {
  source = "./modules/storage"

  landing_zone_bucket_name        = var.landing_zone_bucket_name
  bronze_zone_bucket_name         = var.bronze_zone_bucket_name
  silver_zone_bucket_name         = var.silver_zone_bucket_name
  marts_bucket_name               = var.marts_bucket_name
  dlt_state_bucket_name           = var.dlt_state_bucket_name
  logging_bucket_name             = var.logging_bucket_name
  dlt_state_access_role_names     = var.dlt_state_access_role_names
  dlt_state_access_principal_arns = var.dlt_state_access_principal_arns
  tags                            = var.tags
}

module "registry" {
  source = "./modules/registry"

  ecr_repository_name = var.ecr_repository_name
  tags                = var.tags
}

module "database" {
  source = "./modules/database"

  eks_cluster_name                  = var.eks_cluster_name
  vpc_id                            = module.network.vpc_id
  private_subnets                   = module.network.private_subnets
  node_security_group_id            = module.eks_cluster.node_security_group_id
  cluster_primary_security_group_id = module.eks_cluster.cluster_primary_security_group_id
  airflow_db_name                   = var.airflow_db_name
  airflow_db_username               = var.airflow_db_username
  airflow_db_password               = var.airflow_db_password
  airflow_db_instance_class         = var.airflow_db_instance_class
  airflow_db_allocated_storage      = var.airflow_db_allocated_storage
  airflow_db_max_allocated_storage  = var.airflow_db_max_allocated_storage
  tags                              = var.tags
}

module "catalog" {
  source = "./modules/catalog"

  athena_query_results_bucket_name       = var.athena_query_results_bucket_name
  athena_database_name                   = var.athena_database_name
  athena_workgroup_name                  = var.athena_workgroup_name
  athena_enforce_workgroup_configuration = var.athena_enforce_workgroup_configuration
  athena_partition_projection_start_date = var.athena_partition_projection_start_date
  athena_repository_table_name           = var.athena_repository_table_name
  athena_organization_table_name         = var.athena_organization_table_name
  athena_repository_bucket_count         = var.athena_repository_bucket_count
  athena_organization_bucket_count       = var.athena_organization_bucket_count
  marts_bucket_name                      = module.storage.marts_bucket_name
  tags                                   = var.tags
}

module "identity" {
  source = "./modules/identity"

  eks_oidc_provider_arn           = module.eks_cluster.oidc_provider_arn
  eks_cluster_oidc_issuer_url     = module.eks_cluster.cluster_oidc_issuer_url
  eks_cluster_name                = module.eks_cluster.cluster_name
  eks_cluster_arn                 = module.eks_cluster.cluster_arn
  kubernetes_namespace            = var.kubernetes_namespace
  kubernetes_service_account_name = var.kubernetes_service_account_name
  airflow_runtime_iam_role_name   = var.airflow_runtime_iam_role_name
  github_owner                    = var.github_owner
  github_repository               = var.github_repository
  github_actions_branch           = var.github_actions_branch
  github_actions_role_name        = var.github_actions_role_name
  ecr_repository_arn              = module.registry.repository_arn
  landing_zone_bucket_arn         = module.storage.landing_zone_bucket_arn
  bronze_zone_bucket_arn          = module.storage.bronze_zone_bucket_arn
  silver_zone_bucket_arn          = module.storage.silver_zone_bucket_arn
  marts_bucket_arn                = module.storage.marts_bucket_arn
  dlt_state_bucket_arn            = module.storage.dlt_state_bucket_arn
  athena_query_results_bucket_arn = module.catalog.athena_query_results_bucket_arn
  logging_bucket_arn              = module.storage.logging_bucket_arn
  terraform_state_bucket_arn      = "arn:aws:s3:::${var.terraform_state_bucket_name}"
  tags                            = var.tags
}

module "github_repo" {
  source = "./modules/github_repo"

  github_repository                = var.github_repository
  github_actions_role_arn          = module.identity.github_actions_role_arn
  github_token                     = var.github_token
  aws_region                       = var.aws_region
  ecr_repository_name              = module.registry.repository_name
  eks_cluster_name                 = module.eks_cluster.cluster_name
  kubernetes_namespace             = var.kubernetes_namespace
  kubernetes_service_account_name  = var.kubernetes_service_account_name
  airflow_runtime_role_arn         = module.identity.airflow_runtime_role_arn
  airflow_db_host                  = module.database.host
  airflow_db_port                  = module.database.port
  airflow_db_name                  = module.database.name
  airflow_db_username              = module.database.username
  airflow_db_password              = module.database.password
  athena_database_name             = module.catalog.athena_database_name
  athena_workgroup_name            = module.catalog.athena_workgroup_name
  athena_query_results_bucket_name = module.catalog.athena_query_results_bucket_name
  athena_repository_table_name     = module.catalog.athena_repository_table_name
  athena_organization_table_name   = module.catalog.athena_organization_table_name
  athena_repository_bucket_count   = var.athena_repository_bucket_count
  athena_organization_bucket_count = var.athena_organization_bucket_count
  landing_zone_bucket_name         = module.storage.landing_zone_bucket_name
  bronze_zone_bucket_name          = module.storage.bronze_zone_bucket_name
  silver_zone_bucket_name          = module.storage.silver_zone_bucket_name
  marts_bucket_name                = module.storage.marts_bucket_name
  dlt_state_bucket_name            = module.storage.dlt_state_bucket_name
  logging_bucket_name              = module.storage.logging_bucket_name
  terraform_state_bucket_name      = var.terraform_state_bucket_name
}

module "streamlit_access" {
  source = "./modules/streamlit_access"

  streamlit_iam_user_name         = var.streamlit_iam_user_name
  marts_bucket_arn                = module.storage.marts_bucket_arn
  athena_query_results_bucket_arn = module.catalog.athena_query_results_bucket_arn
  tags                            = var.tags
}
