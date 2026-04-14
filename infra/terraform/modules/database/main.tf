resource "random_password" "airflow_db" {
  length  = 24
  special = false
}

locals {
  airflow_db_password = coalesce(var.airflow_db_password, random_password.airflow_db.result)
}

resource "aws_security_group" "airflow_db" {
  name        = "${var.eks_cluster_name}-airflow-db"
  description = "Allow PostgreSQL from EKS nodes"
  vpc_id      = var.vpc_id

  ingress {
    description     = "PostgreSQL from EKS worker nodes"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [var.node_security_group_id, var.cluster_primary_security_group_id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    var.tags,
    {
      Name    = "${var.eks_cluster_name}-airflow-db"
      Purpose = "airflow-postgres"
    }
  )
}

resource "aws_db_subnet_group" "airflow" {
  name       = "${var.eks_cluster_name}-airflow"
  subnet_ids = var.private_subnets

  tags = merge(
    var.tags,
    {
      Name    = "${var.eks_cluster_name}-airflow"
      Purpose = "airflow-db-subnets"
    }
  )
}

resource "aws_db_instance" "airflow" {
  identifier = "${var.eks_cluster_name}-airflow"

  engine         = "postgres"
  engine_version = "17.6"
  instance_class = var.airflow_db_instance_class

  allocated_storage     = var.airflow_db_allocated_storage
  max_allocated_storage = var.airflow_db_max_allocated_storage
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = var.airflow_db_name
  username = var.airflow_db_username
  password = local.airflow_db_password
  port     = 5432

  db_subnet_group_name    = aws_db_subnet_group.airflow.name
  vpc_security_group_ids  = [aws_security_group.airflow_db.id]
  publicly_accessible     = false
  skip_final_snapshot     = true
  deletion_protection     = false
  backup_retention_period = 0
  apply_immediately       = true

  tags = merge(
    var.tags,
    {
      Name    = "${var.eks_cluster_name}-airflow"
      Purpose = "airflow-postgres"
    }
  )
}
