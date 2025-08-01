import json
import os
import zipfile
from datetime import datetime
import io
import pandas as pd
import requests
import logging
from database import get_db_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AltaceraProcess:
    def __init__(self, base_path):
        self.base_path = base_path
        self.supplier_path = os.path.join(base_path, "altacera")

    def get_raw_files(self):
        """
        Получает сырые данные от поставщика Altacera
        """
        base = 'https://zakaz.altacera.ru/load'
        raw = {}
        try:
            for key, fname in [('nom', 'tovar_json.zip'), ('price', 'price_json.zip')]:
                r = requests.get(f'{base}/{fname}', timeout=10)
                r.raise_for_status()
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    with z.open(z.namelist()[0]) as f:
                        raw[key] = json.load(f)
            logger.info("Данные успешно загружены из API")
            return raw
        except requests.RequestException as e:
            logger.error(f"Ошибка при загрузке данных из API: {e}")
            return None

    def create_unified_xlsx(self):
        """
        Создает унифицированный DataFrame из сырых данных поставщика
        """
        data = self.get_raw_files()
        if not data:
            return None
        nom_data = data['nom']
        price_data = data['price']

        mapping = {}
        for item in nom_data:
            tovar_id = item.get('tovar_id') or item.get('id')
            if not tovar_id:
                continue
            artikul = item.get('artikul') or item.get('article') or item.get('sku')
            tovar_name = item.get('tovar') or item.get('name') or item.get('title')
            for unit in item.get('units', []):
                unit_id = unit.get('unit_id')
                if not unit_id:
                    continue
                key = (tovar_id, unit_id)
                mapping[key] = {
                    'Название': tovar_name,
                    'Единица измерения': unit.get('unit', 'шт'),
                    'Артикул': artikul
                }

        unified_data = []
        for price_block in price_data:
            for price_item in price_block.get('price_list', []):
                tovar_id = price_item.get('tovar_id')
                unit_id = price_item.get('unit_id')
                price_value = price_item.get('price') or price_item.get('value')
                if not tovar_id or not unit_id or not price_value:
                    continue
                key = (tovar_id, unit_id)
                base_info = mapping.get(key)
                if base_info:
                    unified_data.append({
                        **base_info,
                        'Цена': price_value
                    })

        df = pd.DataFrame(unified_data)
        required_columns = ['Название', 'Единица измерения', 'Артикул', 'Цена']
        for col in required_columns:
            if col not in df.columns:
                df[col] = None
        df = df[required_columns]

        df['Цена'] = pd.to_numeric(df['Цена'], errors='coerce').fillna(0)
        df = df.drop_duplicates(subset=['Артикул'], keep='last')
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
        supplier_name = "altacera"
        
        # Создаем папку с датой и проверяем изменения
        has_changes, current_unified_path, previous_unified_path, current_df, previous_df = \
            self.create_date_folder_with_changes(supplier_name)
        
        # Создаем отчет
        report_path, unified_path = self.create_report_in_date_folder(
            supplier_name, has_changes, current_unified_path, 
            previous_unified_path, current_df, previous_df
        )
        
        return {"unified_path": unified_path, "report_path": report_path}