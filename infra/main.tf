terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "ap-southeast-2"
  profile = "terraform-role"
}

resource "aws_s3_bucket" "fenz-datalake" {
  bucket = "fenz-datalake" # 
  versioning {
    enabled = true
  }
  
  tags = {
    Name        = "Fencing"
    Country     = "New Zealand"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket_public_access_block" "block_public" {
  bucket = aws_s3_bucket.fenz-datalake.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "folder_placeholder" {
  bucket = aws_s3_bucket.fenz-datalake.id
  key    = "fencing-data/"
  content_type = "application/x-directory"
  source = "/dev/null"
}
