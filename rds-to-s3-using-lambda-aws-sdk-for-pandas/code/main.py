import csv
import os

import psycopg2

IS_LOCAL = bool(os.getenv("IS_LOCAL", False))


def sync_data(limit: int, offset: int, loops: int, spamwriter) -> None:
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

    number_of_loops = 0
    while number_of_loops < loops:
        number_of_loops += 1

        cur.execute(
            """
            SELECT *
            FROM transactions_2 t
            ORDER BY t.date ASC
            LIMIT %s OFFSET %s;
            """,
            (limit, offset),
        )

        if cur.rowcount == 0:
            number_of_loops = loops

        records = cur.fetchall()
        for row in records:
            spamwriter.writerow(list(row))
            print(list(row))

        offset += limit

    conn.commit()


if "__main__" == __name__:
    with open("transactions-2.csv", "w", newline="") as csvfile:
        spamwriter = csv.writer(
            csvfile, delimiter=";", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )
        sync_data(100, 0, 10, spamwriter)
