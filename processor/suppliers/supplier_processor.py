from typing import Protocol

class SupplierProcess(Protocol):
    name = None

    def get_raw_files(self) -> dict:
        """Получение сырых данных от поставщика."""
        return NotImplementedError

    def create_unified_xlsx(self):
        """Создание унифицированного xlsx файла."""
        return NotImplementedError

    def compare_with_old_xlsx(self):
        """Сравнение с предыдущим файлом."""
        return NotImplementedError

    def make_report(self):
        """Создание отчета при наличии изменений."""
        return NotImplementedError