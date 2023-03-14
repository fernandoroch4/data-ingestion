resource "aws_glue_catalog_database" "aws_glue_catalog_database" {
  name = "data-ingestion-db"

  create_table_default_permission {
    permissions = ["SELECT"]

    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

resource "aws_glue_catalog_table" "aws_glue_catalog_table" {
  name          = "data-ingestion-tb"
  database_name = aws_glue_catalog_database.aws_glue_catalog_database.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${s3.aws_s3_bucket.bucket.id}/data-ingestion-tb"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = 1
      }
    }

    columns {
      name = "transaction_id"
      type = "string"
    }

    columns {
      name = "value"
      type = "double"
    }

    columns {
      name    = "date"
      type    = "date"
      comment = ""
    }

    columns {
      name    = "type"
      type    = "string"
      comment = ""
    }

    columns {
      name    = "from"
      type    = "string"
      comment = ""
    }

    columns {
      name    = "to"
      type    = "string"
      comment = ""
    }
  }
}