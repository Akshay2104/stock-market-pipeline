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


# ============ CLOUDWATCH ============

# Log group where our Spark/Glue logs will be sent
resource "aws_cloudwatch_log_group" "pipeline_logs" {
  # Name of the log group in CloudWatch
  name = "/stock-pipeline/data-quality"
  # How long to keep logs — 30 days is enough for dev
  retention_in_days = 30

  tags = {
    Project     = "stock-market-pipeline"
    Environment = "dev"
  }
}

# Metric filter — watches log group for DATA_QUALITY_FAILURE pattern
# Every time this pattern appears in a log, it increments a counter
resource "aws_cloudwatch_log_metric_filter" "dq_failures" {
  # Which log group to watch
  log_group_name = aws_cloudwatch_log_group.pipeline_logs.name
  name           = "DataQualityFailures"
  # Pattern to look for in log messages
  pattern        = "DATA_QUALITY_FAILURE"

  metric_transformation {
    # Name of the metric this filter creates
    name      = "DataQualityFailureCount"
    # Namespace groups related metrics together
    namespace = "StockPipeline"
    # Each log match increments the counter by 1
    value     = "1"
  }
}

# CloudWatch Alarm — fires when DQ failure count exceeds threshold
resource "aws_cloudwatch_metric_alarm" "dq_failure_alarm" {
  # Display name in CloudWatch console
  alarm_name          = "stock-pipeline-dq-failures"
  alarm_description   = "Data quality failures detected in Silver layer"
  # Which metric to watch
  namespace           = "StockPipeline"
  metric_name         = "DataQualityFailureCount"
  # How to evaluate — sum of all failures in the period
  statistic           = "Sum"
  # Check every 5 minutes
  period              = 300
  # Number of periods to evaluate — 1 means check last 5 minutes
  evaluation_periods  = 1
  # Trigger alarm if count > 0 (any failure is a problem)
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  # What to do when alarm fires — notify SNS topic
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
  # Handle case where no data (no logs) — treat as OK
  treat_missing_data  = "notBreaching"
}

# SNS Topic — the notification channel
resource "aws_sns_topic" "pipeline_alerts" {
  name = "stock-pipeline-alerts"

  tags = {
    Project     = "stock-market-pipeline"
    Environment = "dev"
  }
}

# SNS Subscription — where to send notifications (email)
resource "aws_sns_topic_subscription" "email_alert" {
  # Which SNS topic to subscribe to
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  # Delivery protocol — email in our case
  protocol  = "email"
  # Your email address — replace with yours
  endpoint  = "mahajan.akshay21@gmail.com"
}

# Output the SNS topic ARN for reference
output "sns_topic_arn" {
  value = aws_sns_topic.pipeline_alerts.arn
}