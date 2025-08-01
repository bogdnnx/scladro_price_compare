import os
import psycopg2
from urllib.parse import urlparse
from datetime import datetime
from suppliers.altacera import AltaceraProcess
from database import get_db_connection
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def process_any_supplier(processor_class, supplier_name, base_path="/app/storage"):
    """
    Универсальная функция для обработки любого поставщика
    
    Args:
        processor_class: Класс процессора поставщика (например, AltaceraProcess)
        supplier_name: Имя поставщика для записи в БД
        base_path: Базовый путь для хранения данных
    """
    processor = processor_class(base_path)
    
    # Создаем папку с датой и проверяем изменения
    has_changes, current_unified_path, previous_unified_path, current_df, previous_df = \
        processor.create_date_folder_with_changes(supplier_name)
    
    # Создаем отчет
    report_path, unified_path = processor.create_report_in_date_folder(
        supplier_name, has_changes, current_unified_path, 
        previous_unified_path, current_df, previous_df
    )
    
    # Сохраняем информацию в базу данных
    conn = get_db_connection()
    cur = conn.cursor()
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    cur.execute("""
        INSERT INTO file_records (date, current_unified_path, previous_unified_path, report_path, supplier_name)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE SET
            previous_unified_path = EXCLUDED.previous_unified_path,
            current_unified_path = EXCLUDED.current_unified_path,
            report_path = EXCLUDED.report_path,
            supplier_name = EXCLUDED.supplier_name
    """, (today_str, unified_path, previous_unified_path, report_path, supplier_name))
    
    conn.commit()
    cur.close()
    conn.close()
    
    logger.info(f"Запись для {today_str} (поставщик: {supplier_name}) обновлена в базе данных")
    logger.info(f"Актуальный unified: {unified_path}")
    if previous_unified_path:
        logger.info(f"Предыдущий unified: {previous_unified_path}")
    if report_path:
        logger.info(f"Отчет: {report_path}")

def process_supplier():
    """
    Основная функция обработки поставщика Altacera (для обратной совместимости)
    """
    process_any_supplier(AltaceraProcess, "altacera", "/app/storage")

def main():
    # Обработка поставщика Altacera
    process_supplier()
    
    # Пример добавления нового поставщика (раскомментируйте после создания класса)
    # process_any_supplier(ExampleSupplierProcess, "example_supplier")

if __name__ == "__main__":
    main()