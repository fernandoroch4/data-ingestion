import json
import os
import time
import traceback
from datetime import datetime

import awswrangler as wr
import boto3
import pandas as pd
import pymysql
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_xray_sdk.core import patch_all, xray_recorder

from aws_embedded_metrics import MetricsLogger, metric_scope
from aws_embedded_metrics.config import get_config

# Patching all libraries to instrument downstream calls
xray_recorder.configure(context_missing="IGNORE_ERROR")
patch_all()

db_user = os.getenv("MYSQL_USER")
db_pass = os.getenv("MYSQL_PASS")
db_host = os.getenv("MYSQL_HOST")
db_port = int(os.getenv("MYSQL_PORT"))
db_name = os.getenv("MYSQL_DATABASE")
tb = os.getenv("TABLE_NAME")
tb_cols = os.getenv("TABLE_COLUMNS")
query_limit = int(os.getenv("LIMIT", 1000))
destination_path = os.getenv("DESTINATION_PATH")
save_mode = os.getenv("SAVE_MODE")
partition_cols = os.getenv("PARTITION_COLS")
offset_parameter_name = os.getenv("OFFSET_PARAMETER_NAME")
region_name = os.getenv("AWS_REGION")

ssm = boto3.client("ssm", region_name=region_name)


class EmfMetrics:
    """Cloudwatch Embedded Metrics Format (EMF) for custom metrics"""

    @staticmethod
    def setup():
        metrics_config = get_config()
        metrics_config.namespace = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", metrics_config.namespace) + "_emf"

        # This speeds up unit tests; otherwise it auto-detects and tries to connect to HTTP sockets
        metrics_config.environment = os.environ.get("AWS_EXECUTION_ENV", "local")

    @staticmethod
    @metric_scope
    def put_duration(name: str, duration_seconds: float, metrics: MetricsLogger):
        metrics.put_metric(name, duration_seconds, "Seconds")

    @staticmethod
    @metric_scope
    def put_count(name: str, count: int, metrics: MetricsLogger):
        metrics.put_metric(name, count, "Count")


EmfMetrics.setup()


def lambda_handler(event: dict, context: LambdaContext):
    try:
        start = time.perf_counter()
        db_conn = pymysql.connect(
            user=db_user,
            password=db_pass,
            host=db_host,
            database=db_name,
            port=db_port,
        )
        EmfMetrics.put_duration("source.connection", time.perf_counter() - start)
        # put_custom_metric(
        #     context.function_name,
        #     context.function_version,
        #     "Successfully db connection",
        #     "connection",
        #     time.perf_counter() - start,
        #     "Seconds",
        # )

        offset: int = int(ssm.get_parameter(Name=offset_parameter_name)["Parameter"]["Value"])
        query = f"SELECT {tb_cols} FROM {tb} ORDER BY `date` ASC LIMIT {query_limit} OFFSET {offset}"

        start = time.perf_counter()
        df: pd.DataFrame = wr.mysql.read_sql_query(query, con=db_conn)
        EmfMetrics.put_duration("source.duration", time.perf_counter() - start)
        # put_custom_metric(
        #     context.function_name,
        #     context.function_version,
        #     "AWS SDK for panda read sql query",
        #     "readData",
        #     time.perf_counter() - start,
        #     "Seconds",
        # )
        EmfMetrics.put_count("source.count", len(df.index))
        # put_custom_metric(context.function_name, context.function_version, "Total of records", "records", len(df.index), "Count")

        # check if there rows
        if len(df.index) == 0:
            return None

        start = time.perf_counter()
        if partition_cols is None:
            wr.s3.to_parquet(df=df, path=destination_path, mode=save_mode, dataset=True)
        else:
            wr.s3.to_parquet(df=df, path=destination_path, mode=save_mode, partition_cols=partition_cols, dataset=True)
        EmfMetrics.put_duration("destination.duration", time.perf_counter() - start)
        # put_custom_metric(
        #     context.function_name,
        #     context.function_version,
        #     "AWS SDK for pandas s3 parquet",
        #     "saveData",
        #     time.perf_counter() - start,
        #     "Seconds",
        # )

        new_offset = str(offset + len(df.index))
        ssm.put_parameter(Name=offset_parameter_name, Value=new_offset, Overwrite=True)
        return "ok"
    except Exception as e:
        EmfMetrics.put_count(f"error:{e.args[0]}", 1)
        # put_custom_metric(context.function_name, context.function_version, e.args[0], f"Error:{e.args[0]}", 1, "Count")
        print(
            json.dumps(
                {
                    "input_event": event,
                    "log_stream": context.log_stream_name,
                    "function_name": context.function_name,
                    "function_version": context.function_version,
                    "environments": str(os.environ),
                    "error_message": e.args[0],
                }
            )
        )
        print(traceback.print_exc())
        raise Exception("Error ocurred during executions")


# def put_custom_metric(function_name: str, function_version: str, message: str, metric_name: str, value: int, unit: str) -> None:
#     """The CloudWatch embedded metric format is a JSON specification used to instruct CloudWatch Logs
#     to automatically extract metric values embedded in structured log events"""
#     print(
#         json.dumps(
#             {
#                 "_aws": {
#                     "Timestamp": round(datetime.timestamp(datetime.now()) * 1000),
#                     "CloudWatchMetrics": [
#                         {
#                             "Namespace": f"emf-lambda-{function_name}",
#                             "Dimensions": [["functionVersion"]],
#                             "Metrics": [{"Name": metric_name, "Unit": unit}],
#                         }
#                     ],
#                     f"{metric_name}": value,
#                     "functionVersion": function_version,
#                     "message": message,
#                 }
#             }
#         )
#     )
