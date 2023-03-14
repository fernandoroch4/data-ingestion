data "aws_caller_identity" "current" {}

resource "random_string" "random" {
  length           = 16
  special          = true
}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region = "us-east-1"
  password = random_string.random.result
}