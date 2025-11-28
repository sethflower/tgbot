from pathlib import Path

from openpyxl import Workbook

from app.database import Database


async def export_requests(db: Database) -> Path:
    records = await db.list_requests(limit=500)
    wb = Workbook()
    ws = wb.active
    ws.title = "Заявки"
    headers = [
        "ID",
        "Постачальник",
        "Водій",
        "Телефон",
        "Авто",
        "Вантаж",
        "Тип",
        "Дата",
        "Час",
        "Статус",
    ]
    ws.append(headers)
    for r in records:
        ws.append(
            [
                r["id"],
                r["supplier"],
                r["driver_name"],
                r["driver_phone"],
                r["car_info"],
                r["cargo_type"],
                r["loading_type"],
                r["planned_date"],
                r["planned_time"],
                r["status"],
            ]
        )
    export_path = Path("data/requests.xlsx")
    export_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(export_path)
    return export_path
