import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiosqlite

from .config import settings


STATUS_NEW = "new"
STATUS_CONFIRMED = "confirmed"
STATUS_CANCELLED = "cancelled"
STATUS_RESCHEDULED = "rescheduled"
STATUS_DONE = "done"


class Database:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._lock = asyncio.Lock()

    async def setup(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(
                """
                CREATE TABLE IF NOT EXISTS requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    supplier TEXT NOT NULL,
                    driver_name TEXT NOT NULL,
                    driver_phone TEXT NOT NULL,
                    car_info TEXT NOT NULL,
                    cargo_type TEXT NOT NULL,
                    loading_type TEXT NOT NULL,
                    planned_date TEXT NOT NULL,
                    planned_time TEXT NOT NULL,
                    document_file_id TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    action TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            await db.commit()

    async def add_request(self, data: Dict[str, Any]) -> int:
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                now = datetime.utcnow().isoformat()
                cursor = await db.execute(
                    """
                    INSERT INTO requests (
                        user_id, supplier, driver_name, driver_phone, car_info,
                        cargo_type, loading_type, planned_date, planned_time,
                        document_file_id, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        data["user_id"],
                        data["supplier"],
                        data["driver_name"],
                        data["driver_phone"],
                        data["car_info"],
                        data["cargo_type"],
                        data["loading_type"],
                        data["planned_date"],
                        data["planned_time"],
                        data.get("document_file_id"),
                        STATUS_NEW,
                        now,
                        now,
                    ),
                )
                await db.commit()
                request_id = cursor.lastrowid
                await self.add_log(data["user_id"], f"Created request #{request_id}")
                return int(request_id)

    async def add_log(self, user_id: Optional[int], action: str) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO logs (user_id, action, created_at) VALUES (?, ?, ?)",
                (user_id, action, datetime.utcnow().isoformat()),
            )
            await db.commit()

    async def get_request(self, request_id: int) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            row = await db.execute_fetchone(
                "SELECT * FROM requests WHERE id = ?", (request_id,)
            )
            return dict(row) if row else None

    async def update_status(
        self, request_id: int, status: str, planned_date: Optional[str] = None, planned_time: Optional[str] = None
    ) -> None:
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                row = await db.execute_fetchone(
                    "SELECT * FROM requests WHERE id = ?", (request_id,)
                )
                if not row:
                    return
                planned_date = planned_date or row["planned_date"]
                planned_time = planned_time or row["planned_time"]
                await db.execute(
                    """
                    UPDATE requests
                    SET status = ?, planned_date = ?, planned_time = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        status,
                        planned_date,
                        planned_time,
                        datetime.utcnow().isoformat(),
                        request_id,
                    ),
                )
                await db.commit()
                await self.add_log(row["user_id"], f"Request #{request_id} -> {status}")

    async def list_requests(self, limit: int = 50) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            rows = await db.execute_fetchall(
                "SELECT * FROM requests ORDER BY created_at DESC LIMIT ?", (limit,)
            )
            return [dict(r) for r in rows]

    async def list_pending_notifications(self) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            now = datetime.utcnow()
            target = now + timedelta(hours=1)
            rows = await db.execute_fetchall(
                """
                SELECT * FROM requests
                WHERE status = ? AND datetime(planned_date || ' ' || planned_time) BETWEEN datetime(?) AND datetime(?)
                """,
                (STATUS_CONFIRMED, now.isoformat(), target.isoformat()),
            )
            return [dict(r) for r in rows]

    async def set_done(self, request_id: int) -> None:
        await self.update_status(request_id, STATUS_DONE)
