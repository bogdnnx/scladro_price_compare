from datetime import datetime
from pathlib import Path
import logging
from database import get_db_connection
import json
import requests
import os
from dotenv import load_dotenv
import pandas as pd
import openpyxl

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).parent.parent.parent  # Поднимаемся на два уровня от suppliers/
env_path = project_root / '.env.suppliers'
load_dotenv(env_path)

class MirKeramiki:
    def __init__(self, base_path):
        self.base_path = base_path
        self.supplier_path = os.path.join(base_path, "mir_keramiki")

    def get_raw_files(self):
        """
        получение данных по апи
        Returns: сырые данные(json)
        """
        req = requests.get(os.getenv("MIR_KERAMIKI_API"), headers={"authorization":os.getenv("MIR_KERAMIKI_KEY")})

        return req.content.decode()

    def create_unified_xlsx(self):
        raw_data = json.loads(self.get_raw_files())
        if not raw_data:
            logger.info("Данные от поставщика не были получены")
            return None
        rows = []

        for item in raw_data:
            row = {
                "Name": item.get("Name", ""),
                "Article": item.get("Article", ""),
                "Unit": item.get("Unit", ""),
                "PriceDiler2": item.get("PriceDiler2", 0)  # Предполагается числовое значение, но может быть строкой
            }
            rows.append(row)

        # Создание DataFrame
        df = pd.DataFrame(rows)

        # Преобразование PriceDiler2 в числовой формат (если возможно)
        df["PriceDiler2"] = pd.to_numeric(df["PriceDiler2"], errors="coerce").fillna(0)

        return df

    def check_for_changes(self, supplier_name):
        """
        Проверяет наличие изменений между текущим и предыдущим unified файлом.
        Возвращает (has_changes, current_unified_path, previous_unified_path, current_df, previous_df)
        """
        today_str = datetime.now().strftime("%Y-%m-%d")

        # Создаем текущий unified файл
        current_df = self.create_unified_xlsx()
        if current_df is None:
            logger.error("Не удалось создать текущий DataFrame")
            return False, None, None, None, None

        # Создаем папку для текущей даты
        today_dir = os.path.join(self.supplier_path, today_str)
        os.makedirs(today_dir, exist_ok=True)

        # Сохраняем текущий unified файл
        current_unified_path = os.path.join(today_dir, "unified.xlsx")
        current_df.to_excel(current_unified_path, index=False)
        logger.info(f"Сохранен текущий unified.xlsx: {current_unified_path}")

        # Получаем предыдущий unified файл из базы данных
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT current_unified_path 
            FROM file_records 
            WHERE supplier_name = %s 
            ORDER BY date DESC 
            LIMIT 1
        """, (supplier_name,))

        result_row = cur.fetchone()
        previous_unified_path = result_row[0] if result_row else None
        cur.close()
        conn.close()

        if previous_unified_path is None or not os.path.exists(previous_unified_path):
            logger.info("Предыдущий unified файл не найден, сравнение невозможно")
            return False, current_unified_path, previous_unified_path, current_df, None

        try:
            previous_df = pd.read_excel(previous_unified_path)
            logger.info(f"Загружен предыдущий файл: {previous_unified_path}")
        except Exception as e:
            logger.error(f"Ошибка загрузки предыдущего файла: {str(e)}")
            return False, current_unified_path, previous_unified_path, current_df, None

        if current_df.equals(previous_df):
            logger.info("Изменений нет")
            return False, current_unified_path, previous_unified_path, current_df, previous_df
        else:
            logger.info("Обнаружены изменения в данных")
            return True, current_unified_path, previous_unified_path, current_df, previous_df

    def create_date_folder_with_changes(self, supplier_name):
        """
        Создает папку с датой и проверяет наличие изменений.
        Возвращает (has_changes, current_unified_path, previous_unified_path, current_df, previous_df)
        """
        today_str = datetime.now().strftime("%Y-%m-%d")
        today_dir = os.path.join(self.supplier_path, today_str)

        # Создаем папку для текущей даты
        os.makedirs(today_dir, exist_ok=True)
        logger.info(f"Создана папка для даты: {today_dir}")

        # Проверяем изменения
        return self.check_for_changes(supplier_name)

    def create_report_in_date_folder(self, supplier_name, has_changes, current_unified_path,
                                     previous_unified_path, current_df, previous_df):
        """
        Создает отчет в папке с датой.
        Возвращает (report_path, unified_path)
        """
        today_str = datetime.now().strftime("%Y-%m-%d")
        today_dir = os.path.join(self.supplier_path, today_str)
        report_path = os.path.join(today_dir, "report.xlsx")

        if not has_changes:
            # Создаем пустой отчет с информацией об отсутствии изменений
            try:
                empty_df = pd.DataFrame({
                    'Информация': ['Изменений в данных не обнаружено'],
                    'Дата проверки': [datetime.now().strftime("%d.%m.%Y %H:%M:%S")],
                    'Поставщик': [supplier_name]
                })
                with pd.ExcelWriter(report_path) as writer:
                    empty_df.to_excel(writer, sheet_name='Статус', index=False)
                logger.info(f"Создан отчет об отсутствии изменений: {report_path}")
            except Exception as e:
                logger.error(f"Ошибка создания отчета: {e}")
                return None, current_unified_path

            return report_path, current_unified_path

        # Создаем детальный отчет с изменениями
        try:
            merged = pd.merge(
                previous_df,
                current_df,
                on='Артикул',
                how='outer',
                suffixes=('_prev', '_curr'),
                indicator=True
            )

            new_items = merged[merged['_merge'] == 'right_only'][
                ['Название_curr', 'Единица измерения_curr', 'Артикул', 'Цена_curr']
            ]
            new_items.columns = ['Название', 'Единица измерения', 'Артикул', 'Цена']

            removed_items = merged[merged['_merge'] == 'left_only'][
                ['Название_prev', 'Единица измерения_prev', 'Артикул', 'Цена_prev']
            ]
            removed_items.columns = ['Название', 'Единица измерения', 'Артикул', 'Цена']

            both = merged[merged['_merge'] == 'both']
            changed = both[
                (both['Название_prev'] != both['Название_curr']) |
                (both['Единица измерения_prev'] != both['Единица измерения_curr']) |
                (both['Цена_prev'] != both['Цена_curr'])
                ]
            changed_items = changed[
                ['Название_curr', 'Единица измерения_curr', 'Артикул', 'Цена_curr']
            ]
            changed_items.columns = ['Название', 'Единица измерения', 'Артикул', 'Цена']

            # Создаем сводную информацию
            summary_data = {
                'Метрика': ['Всего товаров (текущих)', 'Всего товаров (предыдущих)',
                            'Новых товаров', 'Удаленных товаров', 'Измененных товаров'],
                'Количество': [len(current_df), len(previous_df),
                               len(new_items), len(removed_items), len(changed_items)]
            }
            summary_df = pd.DataFrame(summary_data)

            with pd.ExcelWriter(report_path) as writer:
                summary_df.to_excel(writer, sheet_name='Сводка', index=False)
                changed_items.to_excel(writer, sheet_name='Измененные', index=False)
                new_items.to_excel(writer, sheet_name='Добавленные', index=False)
                removed_items.to_excel(writer, sheet_name='Удаленные', index=False)

            logger.info(f"Создан детальный отчет: {report_path}")

        except Exception as e:
            logger.error(f"Ошибка создания отчета: {e}")
            return None, current_unified_path

        return report_path, current_unified_path

    def make_report(self):
        """
        Основная функция для создания отчета (для обратной совместимости)
        """
        supplier_name = "mir_keramiki"

        try:
            # Создаем папку с датой и проверяем изменения
            has_changes, current_unified_path, previous_unified_path, current_df, previous_df = \
                self.create_date_folder_with_changes(supplier_name)

            # Если текущий датафрейм не создан — выходим, ничего не делаем
            if current_df is None:
                logger.error("Прерывание: не удалось получить актуальные данные, отчет не будет создан.")
                return {
                    "unified_path": current_unified_path,
                    "report_path": None,
                    "error": "Failed to create current DataFrame"
                }

            # Создаем отчет
            report_path, unified_path = self.create_report_in_date_folder(
                supplier_name, has_changes, current_unified_path,
                previous_unified_path, current_df, previous_df
            )

            return {"unified_path": unified_path, "report_path": report_path}

        except Exception as e:
            logger.exception(f"Необработанная ошибка в make_report: {e}")
            return {"unified_path": None, "report_path": None, "error": str(e)}



