# Configure AWS provider
provider "aws" {
  region = "us-east-1"
}

# Random suffix for unique S3 bucket name
resource "random_id" "suffix" {
  byte_length = 4
}

# ============ S3 ============
resource "aws_s3_bucket" "data_lake" {
  bucket = "stock-pipeline-data-lake-${random_id.suffix.hex}"

  tags = {
    Project     = "stock-market-pipeline"
    Environment = "dev"
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ============ GLUE IAM ROLE ============
resource "aws_iam_role" "glue_role" {
  name = "stock-pipeline-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Project     = "stock-market-pipeline"
    Environment = "dev"
  }
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# ============ GLUE DATA CATALOG ============
resource "aws_glue_catalog_database" "stock_pipeline" {
  name = "stock_pipeline"
}

# ============ GLUE CRAWLER ============
resource "aws_glue_crawler" "gold_layer" {
  database_name = aws_glue_catalog_database.stock_pipeline.name
  name          = "stock-pipeline-gold-crawler"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/gold/stock_prices"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    }
  })

  tags = {
    Project     = "stock-market-pipeline"
    Environment = "dev"
  }
}

# ============ OUTPUTS ============
output "s3_bucket_name" {
  value = aws_s3_bucket.data_lake.bucket
}

output "glue_role_arn" {
  value = aws_iam_role.glue_role.arn
}

output "glue_database" {
  value = aws_glue_catalog_database.stock_pipeline.name
}