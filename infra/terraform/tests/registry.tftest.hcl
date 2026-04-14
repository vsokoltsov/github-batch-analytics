mock_provider "aws" {}

run "plan_registry_module" {
  command = plan

  module {
    source = "./modules/registry"
  }

  variables {
    ecr_repository_name = "example-repo"
    tags = {
      Environment = "test"
      ManagedBy   = "terraform"
    }
  }

  assert {
    condition     = aws_ecr_repository.airflow.name == "example-repo"
    error_message = "ECR repository name should come from the module input."
  }

  assert {
    condition     = aws_ecr_lifecycle_policy.airflow.repository == "example-repo"
    error_message = "Lifecycle policy should target the configured repository."
  }
}
