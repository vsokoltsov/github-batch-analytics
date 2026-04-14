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
