variable "project" {
  description = "Prefix for every resource name."
  type        = string
  default     = "ledger-recon"
}

variable "environment" {
  description = "Deployment environment (dev, prod)."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod."
  }
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "bronze_glacier_after_days" {
  description = "Move raw bronze files to Glacier Instant Retrieval after this many days. They are kept for audit but rarely read."
  type        = number
  default     = 90
}

variable "bronze_retention_days" {
  description = "Delete raw bronze files after this many days (financial records are typically kept for years)."
  type        = number
  default     = 2555 # ~7 years
}

variable "athena_bytes_scanned_cutoff" {
  description = "Cancel any single Athena query that would scan more than this many bytes (cost guardrail)."
  type        = number
  default     = 10737418240 # 10 GB
}

variable "pipeline_trusted_principal_arns" {
  description = "IAM principals allowed to assume the pipeline role (e.g. the Airflow workers' role). Empty = only this account's root."
  type        = list(string)
  default     = []
}
