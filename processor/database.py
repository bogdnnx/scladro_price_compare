# processor/database.py
import os
import psycopg2
from urllib.parse import urlparse
import dotenv
from dotenv import load_dotenv

load_dotenv('.env.suppliers')


def get_db_connection():
    db_url = os.getenv('DATABASE_URL')
    parsed = urlparse(db_url)
    return psycopg2.connect(
        dbname=parsed.path[1:],
        user=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port
    )