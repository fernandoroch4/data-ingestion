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
        last_execution_date_parameter_name = os.getenv("LAST_EXECUTION_DATE_PARAMETER_NAME")
        last_execution_date: int = int(ssm.get_parameter(Name=last_execution_date_parameter_name)["Parameter"]["Value"])

        # Build database query
        tb = os.getenv("TABLE_NAME")
        tb_cols = os.getenv("TABLE_COLUMNS")
        column_date = os.getenv("COLUMN_DATE")
        query_limit = int(os.getenv("LIMIT", 1000))
        query = f""""
            SELECT {tb_cols} 
            FROM {tb} 
            WHERE {column_date} > {last_execution_date}
            ORDER BY {column_date} ASC 
            LIMIT {query_limit}"""

        # Read data from source using wrangler
        start = time.perf_counter()
        df: pd.DataFrame = wr.mysql.read_sql_query(query, con=db_conn)
        EmfMetrics.put_duration("read", time.perf_counter() - start)

        # Records ?
        if len(df.index) == 0:
            EmfMetrics.put_count("records", len(df.index))
            print("No records returned")
            return None

        # Identify a possible last value for date if last records have the same date
        # we walk up the dataframe until find one diff
        # TODO:
        #  -> improvements for identify dataframe that has all date equals
        current_last_date = df.loc[len(df.index) - 1].at[column_date]

        new_last_date = None
        for x in range(2, len(df.index) + 1):
            slice_size = len(df.index) - x

            new_last_date = df.loc[slice_size].at["Column6"]
            if current_last_date != new_last_date:
                # slice dataframe to discard same dates
                df = df.iloc[0 : slice_size + 1]
                break

        EmfMetrics.put_count("records", len(df.index))

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

        # Update last date
        ssm.put_parameter(Name=last_execution_date_parameter_name, Value=new_last_date, Overwrite=True)

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
