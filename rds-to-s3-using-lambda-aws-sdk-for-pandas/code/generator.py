import concurrent.futures
import json
import os
import random
from datetime import datetime, timedelta
from uuid import uuid4

import psycopg2

IS_LOCAL = bool(os.getenv("IS_LOCAL", False))


def generator(thread_number: int, start_range: int, number_of_rows: int = 1000) -> None:

    # Connect to your postgres DB
    if IS_LOCAL:
        print("Using LOCALHOST connection")
        conn = psycopg2.connect(
            "host=localhost dbname=bank user=postgres password=postgres"
        )
    else:
        conn = psycopg2.connect(
            "host=postgresql-14.chcmbtabfbp7.us-east-1.rds.amazonaws.com dbname=bank user=postgres password=Jhfs8762HSFsfs"
        )

    # Open a cursor to perform database operations
    cur = conn.cursor()

    for x in range(start_range, number_of_rows + 1):

        # transaction_id = str(uuid4())
        transaction_id = x
        source_account = round(random.random() * 100000 * random.random())
        destination_account = round(random.random() * 100000 * random.random())
        amount = round(random.random() * 1000 * random.random(), 2)
        transaction_type = random.choice(["PIX", "TED", "DEPOSIT"])

        decrease_days = timedelta(days=random.choice([1, 2, 3, 4, 5]))
        date = (datetime.now() - decrease_days).isoformat()

        values = (
            transaction_id,
            source_account,
            destination_account,
            amount,
            transaction_type,
            date,
        )
        print(f"Thread: {thread_number} - Generating values: {json.dumps(values)}")

        # Execute a query
        cur.execute(
            """
            INSERT INTO transactions_2 (id, source_account, destination_account, amount, transaction_type, date)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (values),
        )

    conn.commit()


if "__main__" == __name__:
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        start_range = 1
        for num in range(1, 11):
            print(f"Starting thread {num}")
            limit = 1000
            executor.submit(generator, num, start_range, limit)
            start_range = start_range + limit
