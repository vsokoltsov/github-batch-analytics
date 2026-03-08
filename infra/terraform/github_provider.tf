provider "github" {
  owner = var.github_owner != "" ? var.github_owner : null
  token = var.github_token
}
