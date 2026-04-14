variable "vpc_cidr" {
  type = string
}

variable "eks_cluster_name" {
  type = string
}

variable "tags" {
  type = map(string)
}
