# --- Least-privilege role for the pipeline (Airflow workers / dbt) ------------

locals {
  trusted_principals = length(var.pipeline_trusted_principal_arns) > 0 ? var.pipeline_trusted_principal_arns : ["arn:aws:iam::${local.account_id}:root"]
}

data "aws_iam_policy_document" "pipeline_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = local.trusted_principals
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name               = "${local.name}-pipeline"
  assume_role_policy = data.aws_iam_policy_document.pipeline_assume.json
}

data "aws_iam_policy_document" "pipeline" {
  # Read/write the lake and Athena results, nothing else in S3
  statement {
    sid     = "LakeObjects"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/*",
      "${aws_s3_bucket.athena_results.arn}/*",
    ]
  }

  statement {
    sid       = "LakeBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn, aws_s3_bucket.athena_results.arn]
  }

  # Manage tables only in this project's Glue databases
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases",
      "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
      "glue:GetPartition", "glue:GetPartitions", "glue:BatchCreatePartition",
      "glue:BatchDeletePartition", "glue:BatchGetPartition",
    ]
    resources = concat(
      ["arn:aws:glue:${var.region}:${local.account_id}:catalog"],
      [for db in aws_glue_catalog_database.layer : "arn:aws:glue:${var.region}:${local.account_id}:database/${db.name}"],
      [for db in aws_glue_catalog_database.layer : "arn:aws:glue:${var.region}:${local.account_id}:table/${db.name}/*"],
    )
  }

  # Run queries only in the dbt workgroup
  statement {
    sid = "Athena"
    actions = [
      "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
      "athena:StopQueryExecution", "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.dbt.arn]
  }
}

resource "aws_iam_role_policy" "pipeline" {
  name   = "${local.name}-pipeline"
  role   = aws_iam_role.pipeline.id
  policy = data.aws_iam_policy_document.pipeline.json
}
