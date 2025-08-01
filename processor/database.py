# processor/database.py
import os
import psycopg2
from urllib.parse import urlparse

def get_db_connection():
    db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@db:5436/supplier_data2')
    parsed = urlparse(db_url)
    return psycopg2.connect(
        dbname=parsed.path[1:],
        user=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port
    )