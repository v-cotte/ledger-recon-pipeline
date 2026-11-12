# Infrastructure (Terraform)

The AWS lakehouse this pipeline runs on in production. Nothing here is needed to run the project locally.

| File | Creates |
|---|---|
| `storage.tf` | Lake bucket (versioned, encrypted, private) with a lifecycle that moves raw `bronze/` files to Glacier Instant Retrieval after 90 days and expires them after ~7 years; an Athena results bucket that expires after 7 days |
| `catalog.tf` | One Glue database per layer (bronze, silver, gold) and an Athena engine v3 workgroup (needed for Iceberg) with a per-query scan limit as a cost guardrail |
| `iam.tf` | A least-privilege role for Airflow / dbt: only this lake, these Glue databases and this workgroup |
| `outputs.tf` | Resource names plus a ready-to-paste `dbt-athena` prod profile |

```bash
cd infra/terraform
terraform init
terraform validate          # no AWS account needed
terraform plan              # needs AWS credentials
terraform apply
terraform output dbt_athena_profile
```

State is local by default; the commented `backend "s3"` block in `versions.tf` is the team setup (remote state with locking).
