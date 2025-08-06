from datetime import datetime
from pathlib import Path
import logging
import os
import json
import time
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

class MirKeramiki:
    def __init__(self, base_path):
        self.base_path = base_path
        self.supplier_path = os.path.join(base_path, 'mir_keramiki')
        os.makedirs(self.supplier_path, exist_ok=True)

    def _fetch_raw(self, retries=3, delay=5, timeout=10) -> list:
        """
        Получает сырые данные JSON с API, возвращает список
        """
        url = os.getenv('MIR_KERAMIKI_API')
        headers = {'authorization': os.getenv('MIR_KERAMIKI_KEY')}
        for attempt in range(1, retries + 1):
            try:
                resp = requests.get(url, headers=headers, timeout=timeout)
                if resp.status_code == 200:
                    return resp.json()
                logger.warning(f"Попытка {attempt}: статус {resp.status_code}")
            except requests.RequestException as e:
                logger.warning(f"Попытка {attempt}: ошибка сети {e}")
            if attempt < retries:
                time.sleep(delay)
        logger.error('Не удалось получить данные от API')
        return []

    def _to_dataframe(self, data: list) -> pd.DataFrame:
        """
        Преобразует список словарей в DataFrame
        """
        rows = []
        for idx, item in enumerate(data):
            name = item.get('Name', '').strip()
            if not name:
                continue
            article = item.get('Article')
            if not article or not isinstance(article, str) or not article.strip():
                article = f'NO_ARTICLE_{idx}'
            unit = item.get('Unit', '')
            price = item.get('PriceDiler2', 0)
            rows.append({
                'Название': name,
                'Артикул': article.strip(),
                'Единица измерения': unit,
                'Цена': price
            })
        df = pd.DataFrame(rows)
        # Приводим цену к числовому типу и заполняем NaN нулями
        if not df.empty:
            df['Цена'] = pd.to_numeric(df['Цена'], errors='coerce').fillna(0)
        # Удаляем дубликаты по названию, оставляя последнюю запись
        df = df.drop_duplicates(subset=['Название'], keep='last')
        return df

    def _load_previous(self, supplier_name: str) -> pd.DataFrame:
        """
        Загружает последний unified-файл из БД
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
        Сравнивает prev и curr по названию
        """
        if prev is None:
            return True, {'new': curr, 'removed': pd.DataFrame(), 'changed': pd.DataFrame()}
        merged = prev.merge(curr, on='Название', how='outer', indicator=True,
                             suffixes=('_prev','_curr'))
        new = merged[merged['_merge']=='right_only'][
            ['Название','Артикул_curr','Единица измерения_curr','Цена_curr']]
        new.columns = ['Название','Артикул','Единица измерения','Цена']
        removed = merged[merged['_merge']=='left_only'][
            ['Название','Артикул_prev','Единица измерения_prev','Цена_prev']]
        removed.columns = ['Название','Артикул','Единица измерения','Цена']
        both = merged[merged['_merge']=='both']
        changed = both[(
            (both['Артикул_prev'] != both['Артикул_curr']) |
            (both['Единица измерения_prev'] != both['Единица измерения_curr']) |
            (both['Цена_prev'] != both['Цена_curr'])
        )][['Название','Артикул_curr','Единица измерения_curr','Цена_curr']]
        changed.columns = ['Название','Артикул','Единица измерения','Цена']
        has = not new.empty or not removed.empty or not changed.empty
        return has, {'new': new, 'removed': removed, 'changed': changed}

    def make_report(self, supplier_name: str = 'mir_keramiki') -> dict:
        """
        Основной метод: получает, сравнивает и сохраняет при изменениях
        """
        raw = self._fetch_raw()
        curr_df = self._to_dataframe(raw) if raw else None
        prev_df = self._load_previous(supplier_name)
        date_str = datetime.now().strftime('%Y-%m-%d')
        dated_folder = os.path.join(self.supplier_path, date_str)
        os.makedirs(dated_folder, exist_ok=True)
        if curr_df is None:
            logger.error('Не удалось получить или обработать данные')
            return {'unified_path': None, 'report_path': None}
        has_changes, parts = self._compare(prev_df, curr_df)
        date_str = datetime.now().strftime('%Y-%m-%d')
        if not has_changes:
            logger.info('Изменений не обнаружено, файлы не создаются.')
            return {'unified_path': None, 'report_path': None}
        # Сохранение unified
        # unified_name = f'unified_{date_str}.xlsx'
        # unified_path = os.path.join(self.supplier_path, unified_name)
        unified_path = os.path.join(dated_folder, 'unified.xlsx')

        curr_df.to_excel(unified_path, index=False)
        logger.info(f'Saved unified: {unified_path}')
        # Создание отчета
        # report_name = f'report_{date_str}.xlsx'
        # report_path = os.path.join(self.supplier_path, report_name)
        report_path = os.path.join(dated_folder, 'report.xlsx')

        summary = pd.DataFrame({
            'Метрика': ['Всего (текущих)','Всего (предыдущих)','Добавленные','Удаленные','Измененные'],
            'Количество': [len(curr_df), len(prev_df) if prev_df is not None else 0,
                           len(parts['new']), len(parts['removed']), len(parts['changed'])]
        })
        with pd.ExcelWriter(report_path) as writer:
            summary.to_excel(writer, sheet_name='Сводка', index=False)
            parts['new'].to_excel(writer, sheet_name='Добавленные', index=False)
            parts['removed'].to_excel(writer, sheet_name='Удаленные', index=False)
            parts['changed'].to_excel(writer, sheet_name='Измененные', index=False)
        logger.info(f'Saved report: {report_path}')
        # Запись в БД
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
        Возвращает путь к предыдущему unified-файлу
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
