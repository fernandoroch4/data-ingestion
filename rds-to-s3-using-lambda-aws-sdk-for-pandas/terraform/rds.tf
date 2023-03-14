resource "aws_db_instance" "rds" {
  allocated_storage    = 30
  db_name              = "data-ingestion"
  engine               = "mysql"
  engine_version       = "5.7"
  instance_class       = "db.t4g.micro"
  username             = secrets.aws_secretsmanager_secret_version.username_value.secret_string
  password             = secrets.aws_secretsmanager_secret_version.password_value.secret_string
  parameter_group_name = "default.mysql5.7"
  skip_final_snapshot  = true
}
