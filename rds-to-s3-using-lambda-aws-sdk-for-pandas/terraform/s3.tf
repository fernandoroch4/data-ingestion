locals {
  bucket_name = concat(local.account_id, "-sor")
}

resource "aws_s3_bucket" "bucket" {
  bucket = local.bucket_name
}

resource "aws_s3_bucket_acl" "bucket_acl" {
  bucket = aws_s3_bucket.bucket.id
  acl    = "private"
}