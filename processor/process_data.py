import time
import logging
import schedule
from datetime import datetime
from suppliers.altacera import AltaceraProcess
from suppliers.mir_keramiki import MirKeramiki

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def process_any_supplier(processor_class, supplier_name, base_path="/app/storage"):
    """
    Универсальная функция для обработки любого поставщика через метод make_report.
    """
    processor = processor_class(base_path)
    result = processor.make_report(supplier_name)

    unified = result.get('unified_path')
    report = result.get('report_path')

    if unified and report:
        logger.info(f"[{supplier_name}] Обработано успешно:")
        logger.info(f"  Unified file: {unified}")
        logger.info(f"  Report file:  {report}")
    else:
        logger.info(f"[{supplier_name}] Изменений не обнаружено, файлы не созданы.")


def process_suppliers():
    """
    Вызывает процесс для всех поставщиков.
    """
    process_any_supplier(AltaceraProcess, "altacera", "/app/storage")
    process_any_supplier(MirKeramiki, "mir_keramiki", "/app/storage")


def main():
    logger.info("=== Initial run before scheduling ===")
    process_suppliers()


if __name__ == "__main__":
    main()
    # Запуск main каждые 12 часов
    schedule.every(12).hours.do(process_suppliers)

    while True:
        schedule.run_pending()
        time.sleep(1)
