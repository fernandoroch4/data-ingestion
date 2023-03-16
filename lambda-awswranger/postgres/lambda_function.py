import os

import awswrangler as wr
import pandas as pd

import mysql.connector as mysql

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASS = os.getenv("MYSQL_PASS")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_PORT = os.getenv("MYSQL_PORT")


def lambda_handler(event: dict, context) -> None:
    # Creating database connection
    connection = mysql.connect(
        user=MYSQL_USER,
        password=MYSQL_PASS,
        host=MYSQL_HOST,
        database=MYSQL_DATABASE,
        port=MYSQL_PORT,
    )

    df = wr.mysql.read_sql_query("SELECT * FROM transactions", con=connection)

    print(df.show())

    connection.close()
