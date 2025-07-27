import json
import os
import zipfile
from datetime import datetime
import io
import pandas as pd
import requests
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
from datetime import datetime

class AltaceraProcess:
    def __init__(self, base_path):
        self.base_path = base_path
        self.supplier_path = os.path.join(base_path, "altacera")
        self.current_df = None
        self.previous_df = None

    def get_raw_files(self):
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

    def compare_with_old_xlsx(self):
        os.makedirs(self.supplier_path, exist_ok=True)
        today_str = datetime.now().strftime("%Y-%m-%d")
        today_dir = os.path.join(self.supplier_path, today_str)

        latest_dir = None
        for item in os.listdir(self.supplier_path):
            if item == today_str:
                continue
            item_path = os.path.join(self.supplier_path, item)
            if os.path.isdir(item_path):
                try:
                    item_date = datetime.strptime(item, "%d.%m.%Y")
                    if latest_dir is None or item_date > datetime.strptime(latest_dir, "%d.%m.%Y"):
                        latest_dir = item
                except ValueError:
                    continue

        actual_df = self.create_unified_xlsx()
        if actual_df is None:
            logger.error("Не удалось создать текущий DataFrame")
            return False, None

        os.makedirs(today_dir, exist_ok=True)
        unified_path = os.path.join(today_dir, "unified.xlsx")
        actual_df.to_excel(unified_path, index=False)
        logger.info(f"Сохранен unified.xlsx: {unified_path}")

        if latest_dir is None:
            return False, unified_path

        prev_file = os.path.join(self.supplier_path, latest_dir, "unified.xlsx")
        if not os.path.exists(prev_file):
            return False, unified_path

        try:
            prev_df = pd.read_excel(prev_file)
        except Exception as e:
            logger.error(f"Ошибка загрузки предыдущего файла: {str(e)}")
            return False, unified_path

        if actual_df.equals(prev_df):
            logger.info("Изменений нет")
            return False, unified_path
        else:
            self.current_df = actual_df
            self.previous_df = prev_df
            return True, unified_path

    def make_report(self):
        has_changes, unified_path = self.compare_with_old_xlsx()
        if not has_changes:
            return {"unified_path": unified_path, "report_path": None}

        merged = pd.merge(
            self.previous_df,
            self.current_df,
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

        today_str = datetime.now().strftime("%d.%m.%Y")
        report_path = os.path.join(self.supplier_path, today_str, "report.xlsx")

        try:
            with pd.ExcelWriter(report_path) as writer:
                changed_items.to_excel(writer, sheet_name='Измененные', index=False)
                new_items.to_excel(writer, sheet_name='Добавленные', index=False)
                removed_items.to_excel(writer, sheet_name='Удаленные', index=False)
            logger.info(f"Создан отчет: {report_path}")
        except Exception as e:
            logger.error(f"Ошибка создания отчета: {e}")
            return {"unified_path": unified_path, "report_path": None}

        return {"unified_path": unified_path, "report_path": report_path}