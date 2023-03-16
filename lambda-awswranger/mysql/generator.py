import random
from datetime import datetime, timedelta
from uuid import uuid4
import pymysql


def get_connection() -> pymysql.Connection:
    connection = pymysql.connect(
        user="admin",
        password="Jhfs8762HSFsfs",
        host="mysql-1.chcmbtabfbp7.us-east-1.rds.amazonaws.com",
        database="bank",
        port=3306,
    )
    return connection


def get_values() -> tuple:
    transaction_id = str(uuid4())
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
    return values


def generator(number_of_rows: int = 100) -> None:

    connection = get_connection()

    with connection:
        with connection.cursor() as cursor:
            for _ in range(number_of_rows):
                values = get_values()
                # Create a new record
                sql = "INSERT INTO `transactions` (`id`, `source_account`, `destination_account`, `amount`, `transaction_type`, `date`) VALUES (%s, %s, %s, %s, %s, %s)"
                print(values)
                cursor.execute(sql, values)

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()


if "__main__" == __name__:
    generator()
