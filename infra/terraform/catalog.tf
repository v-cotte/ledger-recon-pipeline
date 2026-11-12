# --- Glue Data Catalog: one database per medallion layer ---------------------

resource "aws_glue_catalog_database" "layer" {
  for_each = toset(["bronze", "silver", "gold"])

  name         = replace("${local.name}_${each.key}", "-", "_")
  description  = "Ledger reconciliation ${each.key} layer"
  location_uri = "s3://${aws_s3_bucket.lake.bucket}/${each.key}/"
}

# --- Athena workgroup: where dbt-athena runs its queries --------------------

resource "aws_athena_workgroup" "dbt" {
  name          = "${local.name}-dbt"
  force_destroy = var.environment == "dev"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3" # required for Iceberg
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
