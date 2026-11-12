terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # In a team setup, state lives in S3 with locking:
  # backend "s3" {
  #   bucket       = "<state-bucket>"
  #   key          = "ledger-recon/terraform.tfstate"
  #   region       = "us-east-1"
  #   use_lockfile = true
  # }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      project     = var.project
      environment = var.environment
      managed_by  = "terraform"
    }
  }
}
