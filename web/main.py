from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import pandas as pd
import psycopg2
from urllib.parse import urlparse
import os
from jinja2 import Environment, FileSystemLoader

app = FastAPI()

env = Environment(loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates")))

def get_db_connection():
    db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@db:5436/supplier_data2')
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
            SELECT date, unified_path, report_path
            FROM file_records
            ORDER BY date DESC LIMIT 2
        """)
        results = cur.fetchall()
        cur.close()
        conn.close()

        if not results:
            return HTMLResponse(content="Данные не найдены", status_code=404)

        current_record = results[0]
        prev_record = results[1] if len(results) > 1 else None

        current_df = pd.read_excel(current_record[1]) if current_record[1] and os.path.exists(current_record[1]) else None
        prev_df = pd.read_excel(prev_record[1]) if prev_record and prev_record[1] and os.path.exists(prev_record[1]) else None
        report_status = "Отчет доступен" if current_record[2] and os.path.exists(current_record[2]) else "Отчет отсутствует"

        data = {
            "supplier": "Altacera",
            "date": current_record[0].strftime("%d.%m.%Y"),
            "current": current_df.to_dict(orient='records') if current_df is not None else [],
            "prev": prev_df.to_dict(orient='records') if prev_df is not None else [],
            "report": report_status
        }

        template = env.get_template('index.html')
        return HTMLResponse(content=template.render(data=data))

    except Exception as e:
        return HTMLResponse(content=f"Ошибка: {str(e)}", status_code=500)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)