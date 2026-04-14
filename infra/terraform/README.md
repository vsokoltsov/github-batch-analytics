# Terraform

## Remote State

This Terraform configuration supports remote state in S3 via [backend.tf](/Users/vadim.sokoltsov/learning/github-batch-analytics/infra/terraform/backend.tf).

Use a dedicated S3 bucket for Terraform state. Do not manage that bucket from this same Terraform state, otherwise you create a bootstrap cycle.

### 1. Load the repository AWS environment

This repository uses `direnv` to configure AWS credentials and region. Run the commands below from the repository root:

```bash
eval "$(direnv export zsh)"
```

### 2. Create the remote-state bucket with AWS CLI

```bash
aws s3api create-bucket \
  --bucket gba-terraform-state-prod \
  --region eu-central-1 \
  --create-bucket-configuration LocationConstraint=eu-central-1
```

Enable versioning:

```bash
aws s3api put-bucket-versioning \
  --bucket gba-terraform-state-prod \
  --versioning-configuration Status=Enabled
```

Enable server-side encryption:

```bash
aws s3api put-bucket-encryption \
  --bucket gba-terraform-state-prod \
  --server-side-encryption-configuration '{
    "Rules": [
      {
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "AES256"
        }
      }
    ]
  }'
```

Block all public access:

```bash
aws s3api put-public-access-block \
  --bucket gba-terraform-state-prod \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

Optional verification:

```bash
aws s3api head-bucket --bucket gba-terraform-state-prod
aws s3api get-bucket-versioning --bucket gba-terraform-state-prod
```

### 3. Configure the backend

Copy the example backend config:

```bash
cp infra/terraform/backend.hcl.example infra/terraform/backend.hcl
```

Set the real bucket name in `infra/terraform/backend.hcl`:

```hcl
bucket       = "gba-terraform-state-prod"
key          = "github-batch-analytics/infra/terraform.tfstate"
region       = "eu-central-1"
profile      = "gba-admin"
encrypt      = true
```

### 4. Migrate local state to S3

```bash
terraform -chdir=infra/terraform init -backend-config=backend.hcl -migrate-state
```

After migration, all `terraform plan/apply` operations in `infra/terraform` will use the S3 backend.
