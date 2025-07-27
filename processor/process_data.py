import os
import psycopg2
from urllib.parse import urlparse
from datetime import datetime
from suppliers.altacera import AltaceraProcess
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

def process_supplier():
    processor = AltaceraProcess("/storage")
    result = processor.make_report()
    unified_path = result["unified_path"]
    report_path = result["report_path"]

    conn = get_db_connection()
    cur = conn.cursor()
    today_str = datetime.now().strftime("%Y-%m-%d")
    cur.execute("""
        INSERT INTO file_records (date, unified_path, report_path)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO UPDATE SET
            unified_path = EXCLUDED.unified_path,
            report_path = EXCLUDED.report_path
    """, (today_str, unified_path, report_path))
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Запись для {today_str} добавлена в базу данных")

def main():
    process_supplier()

if __name__ == "__main__":
    main()