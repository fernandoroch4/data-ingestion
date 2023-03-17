import json
import logging
import os

import awswrangler as wr
import boto3
import pandas as pd
import pymysql
from aws_lambda_powertools.utilities.typing import LambdaContext

# from aws_xray_sdk.core import patch_all, xray_recorder

# # Patching all libraries to instrument downstream calls
# xray_recorder.configure(service="data-ingestion-mysql", context_missing="IGNORE_ERROR")
# patch_all()

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASS = os.getenv("MYSQL_PASS")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
DESTINATION_PATH = os.getenv("DESTINATION_PATH")
LIMIT = int(os.getenv("LIMIT", 100))
OFFSET_PARAMETER_NAME = os.getenv("OFFSET_PARAMETER_NAME")
LOG_LEVEL = int(os.getenv("LOG_LEVEL"), logging.WARNING)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_data_source_connection(
    user: str, password: str, host: str, database: str, port: int
) -> pymysql.Connection:
    """Creating data source connection for MySQL"""

    connection = pymysql.connect(
        user=user,
        password=password,
        host=host,
        database=database,
        port=port,
    )
    return connection


def read_data_from_source(
    connection: pymysql.Connection, sql_query: str
) -> pd.DataFrame:
    """Read data from data source"""

    df = wr.mysql.read_sql_query(sql_query, con=connection)
    return df


def save_df_to_destination_source(
    df: pd.DataFrame, path: str, mode: str = "overwrite"
) -> None:
    """Save DataFrame to destination in PARQUET format"""

    wr.s3.to_parquet(df=df, path=path, dataset=True, mode=mode)


def get_last_offset(client, parameter_name: str) -> int:
    """Retrieve the last offset save"""
    response = client.get_parameter(Name=parameter_name)
    return int(response["Parameter"]["Value"])


def update_offset(client, parameter_name: str, new_value: int) -> None:
    """Update the offset"""
    client.put_parameter(Name=parameter_name, Value=new_value, Overwrite=True)


def lambda_handler(event: dict, context: LambdaContext) -> None:
    """
    TODO:
        -> create a table manually
        -> add partition if necessary
        -> add xray to package
        -> fix ssm, get parameter timeout
    """
    try:
        parameter_client = boto3.client("ssm")

        columns = "*"
        offset = get_last_offset(parameter_client, OFFSET_PARAMETER_NAME)
        logger.info(f"OFFSET recuperado: {offset}")
        sql_query = f"SELECT {columns} FROM transactions ORDER BY `date` ASC LIMIT {LIMIT} OFFSET {offset}"

        logger.info(f"Quey: {json.dumps(sql_query)}")
        logger.info("Recuperando conexão com data source")
        connection = get_data_source_connection(
            MYSQL_USER, MYSQL_PASS, MYSQL_HOST, MYSQL_DATABASE, MYSQL_PORT
        )

        logger.info("Recuperando dados da origem")
        df = read_data_from_source(connection, sql_query)

        df_size = len(df.index)
        if df_size == 0:
            logger.info("Origem não retornou dados")
            return None

        logger.info(f"{df_size} linha(s) retornada(s) da origem")

        logger.info(f"Armazenando dados no destino: {DESTINATION_PATH}")
        save_df_to_destination_source(df, DESTINATION_PATH)

        new_offset_value = offset + df_size
        logger.info(f"Novo OFFSET: {new_offset_value}")
        update_offset(parameter_client, OFFSET_PARAMETER_NAME, new_offset_value)

        return None

    except Exception as e:
        logger.info("## EVENT")
        logger.info(event)
        logger.info("## LOG STREAM")
        logger.info(context.log_stream_name)
        logger.info("## OS ENVIRONMENTS")
        logger.info(os.environ)
        logger.error("## ERROR")
        logger.error(e)

        raise e
