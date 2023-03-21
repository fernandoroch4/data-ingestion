import json
import os
import time
import traceback

import awswrangler as wr
import boto3
import pandas as pd
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_xray_sdk.core import patch_all, xray_recorder

from db_connection import get_connection
from cloudwatch_emf import EmfMetrics

# Patching all libraries to instrument downstream calls
xray_recorder.configure(context_missing="IGNORE_ERROR")
patch_all()

ssm = boto3.client("ssm", region_name=os.getenv("AWS_REGION"))


def lambda_handler(event: dict, context: LambdaContext):
    try:
        # We use Cloudwatch Embedded Metrics Format (EMF) for custom metrics
        EmfMetrics.setup()

        # Get a database connection
        start = time.perf_counter()
        db_conn = get_connection()
        EmfMetrics.put_duration("connection", time.perf_counter() - start)

        # Retrieve the last offset
        offset_parameter_name = os.getenv("OFFSET_PARAMETER_NAME")
        offset: int = int(ssm.get_parameter(Name=offset_parameter_name)["Parameter"]["Value"])

        # Build database query
        tb = os.getenv("TABLE_NAME")
        tb_cols = os.getenv("TABLE_COLUMNS")
        query_limit = int(os.getenv("LIMIT", 1000))
        query = f"SELECT {tb_cols} FROM {tb} ORDER BY `date` ASC LIMIT {query_limit} OFFSET {offset}"

        # Read data from source using wrangler
        start = time.perf_counter()
        df: pd.DataFrame = wr.mysql.read_sql_query(query, con=db_conn)
        EmfMetrics.put_duration("read", time.perf_counter() - start)
        EmfMetrics.put_count("records", len(df.index))

        # Records ?
        if len(df.index) == 0:
            print("No records returned")
            return None

        # Write data to destination using wrangler
        destination_path = os.getenv("DESTINATION_PATH")
        save_mode = os.getenv("SAVE_MODE")
        partition_cols = os.getenv("PARTITION_COLS")
        start = time.perf_counter()
        if partition_cols is None:
            wr.s3.to_parquet(df=df, path=destination_path, mode=save_mode, dataset=True)
        else:
            wr.s3.to_parquet(df=df, path=destination_path, mode=save_mode, partition_cols=partition_cols, dataset=True)
        EmfMetrics.put_duration("write", time.perf_counter() - start)

        # Update offset
        new_offset = str(offset + len(df.index))
        ssm.put_parameter(Name=offset_parameter_name, Value=new_offset, Overwrite=True)

        return "ok"
    except Exception as e:
        EmfMetrics.put_count(f"error:{e.args[0]}", 1)
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
