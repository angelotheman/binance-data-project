# Configure the AWS provider

provider "aws" {
  region = "us-east-1"
}

variable "bucket_name" {
  type        = string
  description = "This is a bucket which houses the name I want"
  default     = "binance-data-engineering-bucket"
}

resource "aws_s3_bucket" "my_bucket" {
  bucket = var.bucket_name
}
