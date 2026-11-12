output "lake_bucket" {
  description = "S3 bucket holding bronze/silver/gold."
  value       = aws_s3_bucket.lake.bucket
}

output "athena_workgroup" {
  description = "Workgroup dbt-athena should use."
  value       = aws_athena_workgroup.dbt.name
}

output "glue_databases" {
  description = "Glue database per medallion layer."
  value       = { for k, db in aws_glue_catalog_database.layer : k => db.name }
}

output "pipeline_role_arn" {
  description = "Role for Airflow / dbt to assume."
  value       = aws_iam_role.pipeline.arn
}

output "dbt_athena_profile" {
  description = "Paste into dbt/profiles.yml as the prod target."
  value       = <<-EOT
    prod:
      type: athena
      region_name: ${var.region}
      database: awsdatacatalog
      schema: ${aws_glue_catalog_database.layer["gold"].name}
      work_group: ${aws_athena_workgroup.dbt.name}
      s3_staging_dir: s3://${aws_s3_bucket.athena_results.bucket}/dbt/
      s3_data_dir: s3://${aws_s3_bucket.lake.bucket}/
      table_type: iceberg
      threads: 4
  EOT
}
