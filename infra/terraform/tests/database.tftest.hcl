mock_provider "aws" {}
mock_provider "random" {}

run "plan_database_module" {
  command = plan

  module {
    source = "./modules/database"
  }

  variables {
    eks_cluster_name                  = "test-cluster"
    vpc_id                            = "vpc-12345678"
    private_subnets                   = ["subnet-11111111", "subnet-22222222"]
    node_security_group_id            = "sg-node"
    cluster_primary_security_group_id = "sg-cluster"
    airflow_db_name                   = "airflow"
    airflow_db_username               = "airflow_user"
    airflow_db_password               = "super-secret-password"
    airflow_db_instance_class         = "db.t4g.micro"
    airflow_db_allocated_storage      = 20
    airflow_db_max_allocated_storage  = 100
    tags = {
      Environment = "test"
      ManagedBy   = "terraform"
    }
  }

  assert {
    condition     = aws_security_group.airflow_db.name == "test-cluster-airflow-db"
    error_message = "Database security group name should derive from the cluster name."
  }

  assert {
    condition     = aws_db_subnet_group.airflow.name == "test-cluster-airflow"
    error_message = "DB subnet group name should derive from the cluster name."
  }

  assert {
    condition     = aws_db_instance.airflow.identifier == "test-cluster-airflow"
    error_message = "RDS identifier should derive from the cluster name."
  }

  assert {
    condition     = output.name == "airflow"
    error_message = "Database name output should match the configured database name."
  }

  assert {
    condition     = output.username == "airflow_user"
    error_message = "Database username output should match the configured username."
  }
}
