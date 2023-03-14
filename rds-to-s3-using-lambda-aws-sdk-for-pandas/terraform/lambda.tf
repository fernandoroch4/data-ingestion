data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "iam_for_lambda" {
  name               = "iam_for_lambda"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

data "archive_file" "lambda" {
  type        = "zip"
  source_file = "../code/main.py"
  output_path = "../code/lambda_function_payload.zip"
}

resource "aws_lambda_function" "test_lambda" {
  filename      = "../code/lambda_function_payload.zip"
  function_name = "data-ingestion-aws-sdk-for-pandas"
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "main.handler"

  source_code_hash = data.archive_file.lambda.output_base64sha256

  runtime = "python3.9"

  # layer for AWS SDK for pandas
  layers = ["arn:aws:serverlessrepo:us-east-1:336392948345:applications/aws-sdk-pandas-layer-py3-9"]

  environment {
    variables = {
      rds_username_secret_name = secrets.aws_secretsmanager_secret.username.name
      rds_password_secret_name = secrets.aws_secretsmanager_secret.password.name
      rds_endpoint_secret_name = secrets.aws_secretsmanager_secret.endpoint.name
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.lambda_logs,
    aws_cloudwatch_log_group.example,
  ]
}

resource "aws_cloudwatch_log_group" "example" {
  name              = "/aws/lambda/${var.lambda_function_name}"
  retention_in_days = 14
}

# See also the following AWS managed policy: AWSLambdaBasicExecutionRole
data "aws_iam_policy_document" "lambda_policy" {
  statement {
    effect = "Allow"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]

    resources = ["*"]
  }
}

resource "aws_iam_policy" "lambda_data_ingestion_policy" {
  name        = "lambda_data_ingestion_policy"
  path        = "/"
  description = "IAM policy for lambda"
  policy      = data.aws_iam_policy_document.lambda_policy.json
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.iam_for_lambda.name
  policy_arn = aws_iam_policy.lambda_data_ingestion_policy.arn
}
