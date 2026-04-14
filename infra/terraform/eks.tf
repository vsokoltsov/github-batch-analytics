data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  availability_zones = slice(data.aws_availability_zones.available.names, 0, 2)
  private_subnets    = [for index in range(length(local.availability_zones)) : cidrsubnet(var.vpc_cidr, 4, index)]
  public_subnets     = [for index in range(length(local.availability_zones)) : cidrsubnet(var.vpc_cidr, 4, index + 8)]
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${var.eks_cluster_name}-vpc"
  cidr = var.vpc_cidr

  azs             = local.availability_zones
  private_subnets = local.private_subnets
  public_subnets  = local.public_subnets

  enable_nat_gateway = true
  single_nat_gateway = true

  public_subnet_tags = {
    "kubernetes.io/role/elb" = 1
  }

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = 1
  }

  tags = merge(
    var.tags,
    {
      Name    = "${var.eks_cluster_name}-vpc"
      Purpose = "eks-network"
    }
  )
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.eks_cluster_name
  cluster_version = var.eks_cluster_version

  cluster_endpoint_public_access           = true
  enable_cluster_creator_admin_permissions = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_addons = {
    coredns    = {}
    kube-proxy = {}
    vpc-cni    = {}
  }

  eks_managed_node_group_defaults = {
    ami_type       = "AL2_x86_64"
    instance_types = [var.eks_node_instance_type]
  }

  eks_managed_node_groups = {
    default = {
      desired_size = var.eks_node_desired_size
      min_size     = var.eks_node_min_size
      max_size     = var.eks_node_max_size
    }
  }

  tags = merge(
    var.tags,
    {
      Name    = var.eks_cluster_name
      Purpose = "eks-cluster"
    }
  )
}
