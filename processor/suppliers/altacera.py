from datetime import datetime
from pathlib import Path
import os
import io
import json
import time
import zipfile
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from database import get_db_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
project_root = Path(__file__).parent.parent.parent
env_path = project_root / '.env.suppliers'
load_dotenv(env_path)

class AltaceraProcess:
    def __init__(self, base_path):
        self.base_path = base_path
        self.supplier_path = os.path.join(base_path, 'altacera')
        os.makedirs(self.supplier_path, exist_ok=True)

    def _fetch_raw(self, retries=3, delay=5, timeout=10) -> dict:
        """
        Загрузка и распаковка ZIP-файлов с данными 'nom' и 'price'
        """
        base_url = os.getenv('ALTACERA_BASE')
        raw = {}
        for key, fname in [('nom', 'tovar_json.zip'), ('price', 'price_json.zip')]:
            for attempt in range(1, retries + 1):
                try:
                    logger.info(f"[{key}] Запрос {fname}, попытка {attempt}")
                    resp = requests.get(f"{base_url}/{fname}", timeout=timeout)
                    resp.raise_for_status()

                    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                        with z.open(z.namelist()[0]) as f:
                            raw[key] = json.load(f)
                    logger.info(f"[{key}] Успешно загружено")
                    break
                except requests.RequestException as e:
                    logger.warning(f"[{key}] Сетевая ошибка: {e}")
                except (zipfile.BadZipFile, json.JSONDecodeError, IndexError) as e:
                    logger.error(f"[{key}] Ошибка в содержимом ZIP/JSON: {e}")
                    return {}

                if attempt < retries:
                    time.sleep(delay)
            else:
                logger.error(f"[{key}] Не удалось загрузить после {retries} попыток")
                return {}
        return raw

    def _to_dataframe(self, raw: dict) -> pd.DataFrame:
        """
        Формирование unified DataFrame с колонками Название, Артикул, Единица измерения, Цена
        """
        nom = raw.get('nom', [])
        price = raw.get('price', [])
        # Создаем mapping по парам (tovar_id, unit_id)
        mapping = {}
        for item in nom:
            tovar_id = item.get('tovar_id') or item.get('id')
            name = item.get('tovar') or item.get('name') or item.get('title')
            artikul = item.get('artikul') or item.get('article') or item.get('sku')
            for unit in item.get('units', []):
                unit_id = unit.get('unit_id')
                if tovar_id and unit_id:
                    mapping[(tovar_id, unit_id)] = {
                        'Название': name,
                        'Артикул': artikul,
                        'Единица измерения': unit.get('unit', 'шт')
                    }

        unified = []
        for block in price:
            for p in block.get('price_list', []):
                key = (p.get('tovar_id'), p.get('unit_id'))
                info = mapping.get(key)
                price_val = p.get('price') or p.get('value')
                if info and price_val is not None:
                    unified.append({**info, 'Цена': price_val})

        df = pd.DataFrame(unified)
        cols = ['Название', 'Артикул', 'Единица измерения', 'Цена']
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = df[cols]
        df['Цена'] = pd.to_numeric(df['Цена'], errors='coerce').fillna(0)
        # Удаляем дубликаты по названию
        df = df.drop_duplicates(subset=['Название'], keep='last')
        return df

    def _load_previous(self, supplier_name: str) -> pd.DataFrame:
        """
        Загружает последний saved unified-файл из БД
        """
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT current_unified_path FROM file_records WHERE supplier_name=%s ORDER BY date DESC LIMIT 1",
            (supplier_name,)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        path = row[0] if row else None
        if path and os.path.exists(path):
            try:
                return pd.read_excel(path)
            except Exception as e:
                logger.error(f"Ошибка чтения предыдущего файла: {e}")
        return None

    def _compare(self, prev: pd.DataFrame, curr: pd.DataFrame) -> (bool, dict):
        """
        Сравнивает prev и curr по названию и выделяет new, removed, changed
        """
        if prev is None:
            return True, {'new': curr, 'removed': pd.DataFrame(), 'changed': pd.DataFrame()}

        merged = prev.merge(curr, on='Название', how='outer', indicator=True,
                             suffixes=('_prev','_curr'))
        new = merged[merged['_merge']=='right_only'][['Название','Артикул_curr','Единица измерения_curr','Цена_curr']]
        new.columns = ['Название','Артикул','Единица измерения','Цена']

        removed = merged[merged['_merge']=='left_only'][['Название','Артикул_prev','Единица измерения_prev','Цена_prev']]
        removed.columns = ['Название','Артикул','Единица измерения','Цена']

        both = merged[merged['_merge']=='both']
        changed = both[(both['Артикул_prev']!=both['Артикул_curr']) |
                       (both['Единица измерения_prev']!=both['Единица измерения_curr']) |
                       (both['Цена_prev']!=both['Цена_curr'])][
            ['Название','Артикул_curr','Единица измерения_curr','Цена_curr']]
        changed.columns = ['Название','Артикул','Единица измерения','Цена']

        has = not new.empty or not removed.empty or not changed.empty
        return has, {'new': new, 'removed': removed, 'changed': changed}

    def make_report(self, supplier_name: str = 'altacera') -> dict:
        """
        Основная точка входа: получает, сравнивает, сохраняет при изменениях
        """
        raw = self._fetch_raw()
        curr_df = self._to_dataframe(raw) if raw else None
        prev_df = self._load_previous(supplier_name)
        date_str = datetime.now().strftime('%Y-%m-%d')
        dated_folder = os.path.join(self.supplier_path, date_str)
        os.makedirs(dated_folder, exist_ok=True)
        if curr_df is None:
            logger.error("Не удалось получить или обработать данные")
            return {'unified_path': None, 'report_path': None}

        has_changes, parts = self._compare(prev_df, curr_df)
        date_str = datetime.now().strftime('%Y-%m-%d')

        if not has_changes:
            logger.info("Изменений не обнаружено, файлы не создаются.")
            return {'unified_path': None, 'report_path': None}

        # Сохранение unified
        # unified_name = f"unified_{date_str}.xlsx"
        # unified_path = os.path.join(self.supplier_path, unified_name)
        unified_path = os.path.join(dated_folder, 'unified.xlsx')

        curr_df.to_excel(unified_path, index=False)
        logger.info(f"Сохранен unified: {unified_path}")

        # Создание отчета
        # report_name = f"report_{date_str}.xlsx"
        # report_path = os.path.join(self.supplier_path, report_name)
        report_path = os.path.join(dated_folder, 'report.xlsx')

        summary = pd.DataFrame({
            'Метрика': ['Всего (текущих)', 'Всего (предыдущих)', 'Добавленные', 'Удаленные', 'Измененные'],
            'Количество': [len(curr_df), len(prev_df) if prev_df is not None else 0,
                           len(parts['new']), len(parts['removed']), len(parts['changed'])]
        })
        with pd.ExcelWriter(report_path) as writer:
            summary.to_excel(writer, sheet_name='Сводка', index=False)
            parts['new'].to_excel(writer, sheet_name='Добавленные', index=False)
            parts['removed'].to_excel(writer, sheet_name='Удаленные', index=False)
            parts['changed'].to_excel(writer, sheet_name='Измененные', index=False)
        logger.info(f"Сохранен отчет: {report_path}")

        # Запись в базу
        conn = get_db_connection()
        cur = conn.cursor()
        prev_path = self._load_previous_path(supplier_name)
        cur.execute(
            "INSERT INTO file_records(date,current_unified_path,previous_unified_path,report_path,supplier_name)"
            " VALUES(%s,%s,%s,%s,%s)"
            " ON CONFLICT(date,supplier_name) DO UPDATE SET"
            " current_unified_path=EXCLUDED.current_unified_path, report_path=EXCLUDED.report_path",
            (date_str, unified_path, prev_path, report_path, supplier_name)
        )
        conn.commit(); cur.close(); conn.close()

        return {'unified_path': unified_path, 'report_path': report_path}

    def _load_previous_path(self, supplier_name: str) -> str:
        """
        Возвращает путь к предыдущему unified из БД
        """
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT current_unified_path FROM file_records WHERE supplier_name=%s ORDER BY date DESC LIMIT 1",
            (supplier_name,)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        return row[0] if row else None