output "cluster_name" { value = module.eks.cluster_name }
output "cluster_endpoint" { value = module.eks.cluster_endpoint }
output "cluster_oidc_issuer_url" { value = module.eks.cluster_oidc_issuer_url }
output "cluster_arn" { value = module.eks.cluster_arn }
output "oidc_provider_arn" { value = module.eks.oidc_provider_arn }
output "node_security_group_id" { value = module.eks.node_security_group_id }
output "cluster_primary_security_group_id" { value = module.eks.cluster_primary_security_group_id }
output "module_eks" { value = module.eks }
