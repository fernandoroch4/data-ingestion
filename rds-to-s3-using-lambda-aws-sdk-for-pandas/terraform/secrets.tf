resource "aws_secretsmanager_secret" "username" {
  name = "username"
}

resource "aws_secretsmanager_secret_version" "username_value" {
  secret_id     = aws_secretsmanager_secret.username.id
  secret_string = "rds-user"
}

resource "aws_secretsmanager_secret" "password" {
  name = "password"
}

resource "aws_secretsmanager_secret_version" "password_value" {
  secret_id     = aws_secretsmanager_secret.password.id
  secret_string = local.password
}

resource "aws_secretsmanager_secret" "endpoint" {
  name = "endpoint"
}

resource "aws_secretsmanager_secret_version" "endpoint_value" {
  secret_id     = aws_secretsmanager_secret.endpoint.id
  secret_string = rds.aws_db_instance.rds.endpoint
}