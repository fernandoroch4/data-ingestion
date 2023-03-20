import os

db_engine = os.getenv("DB_ENGINE")

if db_engine.upper() == "MYSQL":
    import pymysql as db_client

if db_engine.upper() in ["POSTGRESQL", "POSTGRES"]:
    import pg8000 as db_client


def get_connection():
    """Database connection based on engine ( PostgreSQL or MySQL )"""

    conn = db_client.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        database=os.getenv("DB_DATABASE"),
    )

    return conn
