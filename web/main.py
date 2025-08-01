from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse
import psycopg2
from urllib.parse import urlparse
import os
from jinja2 import Environment, FileSystemLoader

app = FastAPI()

env = Environment(loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates")))

def get_db_connection():
    db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@db:5432/supplier_data2')
    parsed = urlparse(db_url)
    return psycopg2.connect(
        dbname=parsed.path[1:],
        user=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port
    )

@app.get("/", response_class=HTMLResponse)
async def index():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT supplier_name, date, current_unified_path, previous_unified_path, report_path
            FROM file_records
            ORDER BY date DESC
        """)
        results = cur.fetchall()
        cur.close()
        conn.close()

        suppliers = {}

        for row in results:
            supplier_name, date, current_unified_path, previous_unified_path, report_path = row
            # Если имя поставщика не указано, используем "Неизвестный поставщик"
            if supplier_name is None:
                supplier_name = "Неизвестный поставщик"
            
            if supplier_name not in suppliers:
                suppliers[supplier_name] = []
            
            file_info = {
                "date": date.strftime("%d.%m.%Y"),
                "current_unified_path": current_unified_path,
                "previous_unified_path": previous_unified_path,
                "report_path": report_path
            }
            suppliers[supplier_name].append(file_info)

        template = env.get_template('index.html')
        return HTMLResponse(content=template.render(suppliers=suppliers))

    except Exception as e:
        return HTMLResponse(content=f"Ошибка: {str(e)}", status_code=500)


@app.get("/download/{file_path:path}")
async def download_file(file_path: str):
    # # Убираем ведущий слеш, если он есть
    # if file_path.startswith('/'):
    file_path = file_path[4:]

    # Проверяем, что путь не выходит за пределы storage
    # if not file_path.startswith('storage/'):
    #     return {"error": "Invalid file path"}

    # Полный путь к файлу
    #full_path = os.path.join('/app', file_path)

    if os.path.exists(file_path):
        return FileResponse(file_path)
    else:
        return {"error": f"File not found: {file_path}"}