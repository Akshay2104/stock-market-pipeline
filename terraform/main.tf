provider "aws" {
  region = "us-east-1"
}

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

# ============ REDSHIFT ============
resource "aws_redshift_cluster" "warehouse" {
  cluster_identifier = "stock-pipeline-warehouse"
  database_name      = "stockdb"
  master_username    = "admin"
  master_password    = var.redshift_password
  node_type          = "dc2.large"
  number_of_nodes    = 1
  cluster_type       = "single-node"

  skip_final_snapshot = true

  tags = {
    Project     = "stock-market-pipeline"
    Environment = "dev"
  }
}

variable "redshift_password" {
  description = "Master password for Redshift cluster"
  type        = string
  sensitive   = true
}

# ============ OUTPUTS ============
output "s3_bucket_name" {
  value = aws_s3_bucket.data_lake.bucket
}

output "redshift_endpoint" {
  value = aws_redshift_cluster.warehouse.endpoint
}