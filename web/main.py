from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse
import psycopg2
from urllib.parse import urlparse
import os
from jinja2 import Environment, FileSystemLoader
import dotenv

app = FastAPI()

env = Environment(loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates")))
load_dotenv('.env')
def get_db_connection():
    db_url = os.getenv('DATABASE_URL')
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
            SELECT DISTINCT ON (supplier_name) supplier_name, date, current_unified_path, previous_unified_path, report_path
            FROM file_records
            ORDER BY supplier_name, date DESC
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

    file_path = file_path[4:]
    if os.path.exists(file_path):
        return FileResponse(file_path, headers={"Content-Disposition": "attachment"})
    else:
        return {"error": f"File not found: {file_path}"}