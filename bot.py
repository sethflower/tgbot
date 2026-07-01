###############################################################
#                      DRIVER QUEUE BOT                       
#       Aiogram 3 • Railway Hosting • PostgreSQL (async)      
#                     FULL PROFESSIONAL EDITION               
###############################################################

import os
import json
import asyncio
import logging
from datetime import datetime, date, timedelta, time as dtime
from tempfile import NamedTemporaryFile
from typing import Any
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardButton,
    BufferedInputFile,
)

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession
)
from sqlalchemy.orm import declarative_base
from sqlalchemy import (
    Column, Integer, BigInteger, String, Boolean,
    Date, Text, TIMESTAMP, select, delete, text, inspect
)

from openpyxl import Workbook

import gspread
from google.oauth2.service_account import Credentials

from dotenv import load_dotenv


###############################################################
#                    LOAD ENVIRONMENT                         
###############################################################

load_dotenv()

KYIV_TZ = ZoneInfo("Europe/Kyiv")

def kyiv_now() -> datetime:
    return datetime.now(KYIV_TZ)

def kyiv_now_naive() -> datetime:
    return kyiv_now().replace(tzinfo=None)

def to_kyiv(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=KYIV_TZ)
    return dt.astimezone(KYIV_TZ)

def min_planned_datetime(base: datetime | None = None) -> datetime:
    return to_kyiv(base or kyiv_now()) + timedelta(hours=1)

def get_min_date_from_state(data: dict[str, Any]) -> date | None:
    raw = data.get("min_plan_dt")
    if isinstance(raw, str):
        try:
            raw_dt = datetime.fromisoformat(raw)
        except ValueError:
            return None
    elif isinstance(raw, datetime):
        raw_dt = raw
    else:
        return None
    return to_kyiv(raw_dt).date()


def get_min_datetime_from_state(data: dict[str, Any]) -> datetime | None:
    raw = data.get("min_plan_dt")
    if isinstance(raw, str):
        try:
            raw_dt = datetime.fromisoformat(raw)
        except ValueError:
            return None
    elif isinstance(raw, datetime):
        raw_dt = raw
    else:
        return None
    return to_kyiv(raw_dt)


def parse_date_input(raw: str) -> date | None:
    try:
        return datetime.strptime(raw.strip(), "%Y-%m-%d").date()
    except Exception:
        return None

def parse_request_ids(raw: str) -> list[int] | None:
    parts = [part.strip() for part in raw.split(",")]
    if not parts or any(not part or not part.isdigit() for part in parts):
        return None

    request_ids = list(dict.fromkeys(int(part) for part in parts))
    if any(request_id <= 0 for request_id in request_ids):
        return None

    return request_ids


ROLE_LABELS = {
    "user": "Користувач",
    "admin": "Адміністратор",
    "superadmin": "Суперадмін",
    "system": "Система",
}

ACTION_LABELS = {
    "request_created": "Створення нової заявки",
    "request_updated": "Зміна заявки користувачем",
    "request_deleted": "Видалення заявки користувачем",
    "request_deleted_by_admin": "Видалення заявки адміністратором",
    "requests_deleted_by_superadmin": "Видалення вибраних заявок суперадміністратором",
    "request_approved": "Підтвердження заявки адміністратором",
    "request_rejected": "Відхилення заявки адміністратором",
    "request_completed": "Завершення заявки",
    "logs_export": "Експорт журналу дій",
    "admin_added": "Додано нового адміністратора",
    "admin_removed": "Видалено адміністратора",
    "database_cleared": "Очищено всі заявки",
    "np_delivery_submitted": "Заявка на доставку поштою",
    "admin_change_time": "Адміністратор запропонував новий час",
    "admin_change_confirmed": "Користувач підтвердив час адміністратора",
    "admin_change_delete": "Користувач скасував заявку після зміни",
    "admin_change_declined": "Користувач відмовився від часу адміністратора",
    "admin_change_proposed": "Користувач запропонував інший час",
    "admin_keep_client_time": "Адміністратор залишив час користувача",
    "admin_keep_admin_time": "Адміністратор залишив свій час",
    "admin_accept_user_proposal": "Адміністратор прийняв пропозицію користувача",
    "admin_reject_user_proposal": "Адміністратор відхилив пропозицію користувача",
}

DETAIL_KEY_LABELS = {
    "request_id": "ID заявки",
    "request_ids": "ID заявок",
    "reason": "Причина",
    "description": "Опис",
    "changes": "Зміни",
    "field": "Поле",
    "old": "Було",
    "new": "Стало",
    "start": "Початок періоду",
    "end": "Кінець періоду",
    "telegram_id": "Telegram ID",
    "last_name": "Прізвище",
    "name": "Ім'я/Прізвище",
    "new_date": "Нова дата",
    "new_time": "Новий час",
    "date": "Дата",
    "time": "Час",
    "planned_date": "Запланована дата",
    "planned_time": "Запланований час",
    "proposed_date": "Запропонована дата",
    "proposed_time": "Запропонований час",
    "supplier": "Постачальник",
    "vehicle_number": "Номер авто",
    "cargo_volume": "Об'єм вантажу",
    "cargo_description": "Товар",
    "loading_type": "Тип завантаження",
    "auto": "Автоматичне закриття",
    "saved_to_sheet": "Запис у Google Sheets",
    "ttn": "ТТН",
}


def _localize_detail_key(key: str) -> str:
    return DETAIL_KEY_LABELS.get(key, key)


def _localize_detail_value(value: Any) -> Any:
    if isinstance(value, bool):
        return "так" if value else "ні"
    if isinstance(value, (datetime, date, dtime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {_localize_detail_key(k): _localize_detail_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_localize_detail_value(v) for v in value]
    return value


def build_action_description(action: str, role_label: str, details: dict[str, Any] | None) -> str:
    d = details or {}
    rid = d.get("request_id")
    reason = d.get("reason")
    date_val = d.get("date") or d.get("new_date") or d.get("planned_date") or d.get("proposed_date")
    time_val = d.get("time") or d.get("new_time") or d.get("planned_time") or d.get("proposed_time")

    if action == "request_created":
        return f"{role_label} створив(ла) нову заявку #{rid} на {d.get('supplier', '')} ({date_val} {time_val}).".strip()
    if action == "request_updated":
        return f"{role_label} оновив(ла) заявку #{rid}. Причина: {reason or 'не вказано'}."
    if action == "request_deleted":
        return f"{role_label} видалив(ла) свою заявку #{rid}. Причина: {reason or 'не вказано'}."
    if action == "request_deleted_by_admin":
        return f"{role_label} видалив(ла) заявку користувача #{rid}."
    if action == "requests_deleted_by_superadmin":
        request_ids = d.get("request_ids", [])
        ids_text = ", ".join(str(req_id) for req_id in request_ids)
        return f"{role_label} видалив(ла) вибрані заявки: {ids_text}."
    if action == "request_approved":
        return f"{role_label} підтвердив(ла) заявку #{rid}."
    if action == "request_rejected":
        return f"{role_label} відхилив(ла) заявку #{rid}. Причина: {reason or 'не вказано'}."
    if action == "request_completed":
        return f"{role_label} завершив(ла) заявку #{rid} ({'авто' if d.get('auto') else 'ручне'} закриття)."
    if action == "logs_export":
        return f"{role_label} експортував(ла) журнал дій за період {d.get('start')} — {d.get('end')}."
    if action == "admin_added":
        return f"{role_label} додав(ла) адміністратора Telegram ID {d.get('telegram_id')} ({d.get('last_name', '')})."
    if action == "admin_removed":
        return f"{role_label} видалив(ла) адміністратора Telegram ID {d.get('telegram_id')}."
    if action == "database_cleared":
        return f"{role_label} повністю очистив(ла) базу заявок."
    if action == "np_delivery_submitted":
        return f"{role_label} подав(ла) поштову заявку: постачальник {d.get('supplier', '')}, ТТН {d.get('ttn', '')}."
    if action == "admin_change_time":
        return f"{role_label} запропонував(ла) новий час для заявки #{rid}: {date_val} {time_val}. Причина: {reason or 'не вказано'}."
    if action == "admin_change_confirmed":
        return f"{role_label} підтвердив(ла) час адміністратора для заявки #{rid}: {date_val} {time_val}."
    if action == "admin_change_delete":
        return f"{role_label} скасував(ла) заявку #{rid} після запропонованих змін. Причина: {reason or 'не вказано'}."
    if action == "admin_change_declined":
        return f"{role_label} відмовив(ла) час адміністратора для заявки #{rid}. Причина: {reason or 'не вказано'}."
    if action == "admin_change_proposed":
        return f"{role_label} запропонував(ла) інший час для заявки #{rid}: {date_val} {time_val}. Причина: {reason or 'не вказано'}."
    if action == "admin_keep_client_time":
        return f"{role_label} залишив(ла) початковий час користувача для заявки #{rid}."
    if action == "admin_keep_admin_time":
        return f"{role_label} залишив(ла) свій час після відмови користувача для заявки #{rid}."
    if action == "admin_accept_user_proposal":
        return f"{role_label} прийняв(ла) пропозицію користувача для заявки #{rid}: {date_val} {time_val}."
    if action == "admin_reject_user_proposal":
        return f"{role_label} відхилив(ла) пропозицію користувача для заявки #{rid}. Причина: {reason or 'не вказано'}."

    return f"{role_label} виконав(ла) дію: {ACTION_LABELS.get(action, action)}."


async def log_action(
    actor_id: int | None,
    actor_role: str,
    action: str,
    details: dict[str, Any] | None = None,
):
    role_label = ROLE_LABELS.get(actor_role, actor_role or "Невідомо")
    action_label = ACTION_LABELS.get(action, action)
    base_details = dict(details or {})
    description = build_action_description(action, role_label, base_details)
    payload_source = {"description": description, **base_details}
    localized_details = _localize_detail_value(payload_source)
    payload = json.dumps(localized_details, ensure_ascii=False) if localized_details else None
    async with SessionLocal() as session:
        session.add(
            ActionLog(
                actor_id=actor_id,
                actor_role=role_label,
                action=action_label,
                details=payload,
                created_at=kyiv_now_naive(),
            )
        )
        await session.commit()


BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPERADMIN_ID = int(os.getenv("SUPERADMIN_ID"))
DATABASE_URL = os.getenv("DATABASE_URL")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID")

if not all([BOT_TOKEN, SUPERADMIN_ID, DATABASE_URL]):
    raise RuntimeError("❌ ENV-переменные BOT_TOKEN / SUPERADMIN_ID / DATABASE_URL не установлены!")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)


###############################################################
#                  LOGGING & BOT INITIALIZATION               
###############################################################

logging.basicConfig(level=logging.INFO)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()


###############################################################
#                        DATABASE                             
###############################################################

Base = declarative_base()

class Admin(Base):
    __tablename__ = "admins"

    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    last_name = Column(Text, nullable=False, default="")
    is_superadmin = Column(Boolean, default=False)


class Request(Base):
    __tablename__ = "requests"

    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False)

    supplier = Column(Text)
    driver_name = Column(Text)
    phone = Column(Text)
    vehicle_number = Column(Text, nullable=True)
    car = Column(Text)
    cargo_description = Column(Text)

    docs_file_id = Column(Text, nullable=True)
    cargo_type = Column(Text)
    loading_type = Column(Text)

    planned_date = Column(Date)
    planned_time = Column(Text)

    date = Column(Date)
    time = Column(Text)

    created_at = Column(TIMESTAMP, default=kyiv_now_naive)
    updated_at = Column(TIMESTAMP, default=kyiv_now_naive, onupdate=kyiv_now_naive)
    status = Column(String, default="new")
    admin_id = Column(BigInteger, nullable=True)
    sheet_row = Column(Integer, nullable=True)
    completed_at = Column(TIMESTAMP, nullable=True)
    pending_date = Column(Date, nullable=True)
    pending_time = Column(Text, nullable=True)
    pending_reason = Column(Text, nullable=True)


class ActionLog(Base):
    __tablename__ = "action_logs"

    id = Column(Integer, primary_key=True)
    actor_id = Column(BigInteger, nullable=True)
    actor_role = Column(String(50), nullable=False)
    action = Column(Text, nullable=False)
    details = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, default=kyiv_now_naive)


engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        def ensure_sheet_row_column(sync_conn):
            inspector = inspect(sync_conn)
            cols = {c["name"] for c in inspector.get_columns("requests")}
            if "sheet_row" not in cols:
                sync_conn.execute(text("ALTER TABLE requests ADD COLUMN sheet_row INTEGER"))
            if "cargo_type" not in cols:
                sync_conn.execute(text("ALTER TABLE requests ADD COLUMN cargo_type TEXT"))
            if "planned_date" not in cols:
                sync_conn.execute(text("ALTER TABLE requests ADD COLUMN planned_date DATE"))
            if "planned_time" not in cols:
                sync_conn.execute(text("ALTER TABLE requests ADD COLUMN planned_time TEXT"))
            if "updated_at" not in cols:
                sync_conn.execute(text("ALTER TABLE requests ADD COLUMN updated_at TIMESTAMP"))
            if "completed_at" not in cols:
                sync_conn.execute(text("ALTER TABLE requests ADD COLUMN completed_at TIMESTAMP"))
            if "cargo_description" not in cols:
                sync_conn.execute(text("ALTER TABLE requests ADD COLUMN cargo_description TEXT"))
            if "vehicle_number" not in cols:
                sync_conn.execute(text("ALTER TABLE requests ADD COLUMN vehicle_number TEXT"))
            if "pending_date" not in cols:
                sync_conn.execute(text("ALTER TABLE requests ADD COLUMN pending_date DATE"))
            if "pending_time" not in cols:
                sync_conn.execute(text("ALTER TABLE requests ADD COLUMN pending_time TEXT"))
            if "pending_reason" not in cols:
                sync_conn.execute(text("ALTER TABLE requests ADD COLUMN pending_reason TEXT"))

            sync_conn.execute(text("UPDATE requests SET planned_date = date WHERE planned_date IS NULL"))
            sync_conn.execute(text("UPDATE requests SET planned_time = time WHERE planned_time IS NULL"))
            sync_conn.execute(text("UPDATE requests SET updated_at = created_at WHERE updated_at IS NULL"))

        await conn.run_sync(ensure_sheet_row_column)

        def ensure_admin_columns(sync_conn):
            inspector = inspect(sync_conn)
            cols = {c["name"] for c in inspector.get_columns("admins")}
            if "last_name" not in cols:
                sync_conn.execute(text("ALTER TABLE admins ADD COLUMN last_name TEXT"))
                sync_conn.execute(
                    text(
                        "UPDATE admins SET last_name = CASE WHEN is_superadmin THEN 'Админ' ELSE '' END"
                    )
                )

        await conn.run_sync(ensure_admin_columns)


###############################################################
#                        GOOGLE SHEETS
###############################################################


def get_sheet_status(status: str) -> str:
    return {
        "new": "Новая",
        "approved": "Принятая",
        "rejected": "Отклонённая",
        "deleted_by_user": "Удалена",
        "pending_user_confirmation": "Ожидает подтверждения пользователя",
        "pending_admin_decision": "Ожидает решения администратора",
        "pending_user_final": "Ожидает окончательного подтверждения пользователя",
    }.get(status, status)


class GoogleSheetClient:
    def __init__(self):
        self._worksheet = None
        self._spreadsheet = None
        self._np_worksheet = None
        self._init_attempted = False

    async def _ensure_client(self) -> bool:
        if self._worksheet:
            return True
        if self._init_attempted:
            return False

        self._init_attempted = True

        if not GOOGLE_SERVICE_ACCOUNT_JSON or not GOOGLE_SPREADSHEET_ID:
            logging.warning("Google Sheets не налаштовано: немає env GOOGLE_SERVICE_ACCOUNT_JSON / GOOGLE_SPREADSHEET_ID")
            return False

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]

        try:
            info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
            creds = Credentials.from_service_account_info(info, scopes=scopes)

            def _init_ws():
                client = gspread.authorize(creds)
                spreadsheet = client.open_by_key(GOOGLE_SPREADSHEET_ID)
                return spreadsheet, spreadsheet.sheet1

            self._spreadsheet, self._worksheet = await asyncio.to_thread(_init_ws)
            logging.info("Google Sheets клієнт ініціалізовано")
        except Exception as exc:
            logging.exception("Не вдалося підключитися до Google Sheets: %s", exc)
            self._worksheet = None

        return self._worksheet is not None

    async def _get_np_worksheet(self):
        if not await self._ensure_client():
            return None

        if self._np_worksheet:
            return self._np_worksheet

        def _fetch_ws():
            try:
                return self._spreadsheet.worksheet("Накопитель НП")
            except gspread.WorksheetNotFound:
                return self._spreadsheet.add_worksheet(
                    title="Накопитель НП", rows=1000, cols=3
                )

        try:
            self._np_worksheet = await asyncio.to_thread(_fetch_ws)
        except Exception as exc:
            logging.exception("Не вдалося отримати аркуш 'Накопитель НП': %s", exc)
            self._np_worksheet = None

        return self._np_worksheet

    def _build_row(self, req: "Request", admin_name: str) -> list[str]:
        admin_decision = req.status in {"approved", "rejected"}

        if req.status == "approved" and req.date and req.time:
            confirmed_date = req.date.strftime("%d.%m.%Y")
            confirmed_time = req.time
        elif req.status == "rejected":
            confirmed_date = confirmed_time = "Отклонена"
        else:
            confirmed_date = confirmed_time = ""

        return [
            req.created_at.strftime("%d.%m.%Y %H:%M") if req.created_at else "",
            req.updated_at.strftime("%d.%m.%Y %H:%M") if admin_decision and req.updated_at else "",
            req.supplier,
            req.phone,
            req.car,
            req.loading_type,
            req.planned_date.strftime("%d.%m.%Y") if req.planned_date else "",
            req.planned_time or "",
            get_sheet_status(req.status),
            confirmed_date,
            confirmed_time,
            "Завершена" if req.completed_at else "Не завершена",
            admin_name if admin_decision and admin_name else "",
            req.completed_at.strftime("%d.%m.%Y %H:%M") if req.completed_at else "",
            str(req.id),
            req.cargo_description or "",
            req.vehicle_number or "",
        ]

    async def _update_row(self, row_number: int, values: list[str]) -> bool:
        try:
            await asyncio.to_thread(
                self._worksheet.update,
                f"A{row_number}:Q{row_number}",
                [values],
                value_input_option="USER_ENTERED",
            )
            return True
        except Exception as exc:
            logging.exception("Не вдалося оновити рядок %s у Sheets: %s", row_number, exc)
            return False

    async def _append_row(self, values: list[str]) -> int | None:
        try:
            result = await asyncio.to_thread(
                self._worksheet.append_row,
                values,
                value_input_option="USER_ENTERED",
                table_range="A2",
            )
            updated_range = None
            if isinstance(result, dict):
                updated_range = result.get("updates", {}).get("updatedRange")

            if updated_range:
                first_cell = updated_range.split("!")[-1].split(":")[0]
                row_digits = "".join(ch for ch in first_cell if ch.isdigit())
                if row_digits.isdigit():
                    return int(row_digits)

            values_count = await asyncio.to_thread(self._worksheet.get_all_values)
            return len(values_count)
        except Exception as exc:
            logging.exception("Не вдалося додати рядок у Sheets: %s", exc)
            return None

    async def append_np_delivery(self, supplier: str, ttn: str) -> bool:
        ws = await self._get_np_worksheet()
        if not ws:
            return False

        values = [
            kyiv_now().strftime("%d.%m.%Y %H:%M"),
            supplier,
            ttn,
        ]

        try:
            await asyncio.to_thread(
                ws.append_row,
                values,
                value_input_option="USER_ENTERED",
                table_range="A2",
            )
            return True
        except Exception as exc:
            logging.exception("Не вдалося додати заявку поштою у Sheets: %s", exc)
            return False

    async def _find_row_by_request_id(self, req_id: int) -> int | None:
        try:
            column_values = await asyncio.to_thread(self._worksheet.col_values, 15)
        except Exception as exc:
            logging.exception(
                "Не вдалося отримати список ID для пошуку заявки %s: %s", req_id, exc
            )
            return None

        for idx, value in enumerate(column_values[1:], start=2):
            if value == str(req_id):
                return idx

        return None

    async def _get_row_number(self, req: "Request") -> int | None:
        if not req.sheet_row:
            return await self._find_row_by_request_id(req.id)

        try:
            cell = await asyncio.to_thread(self._worksheet.cell, req.sheet_row, 15)
            if cell.value == str(req.id):
                return req.sheet_row
        except Exception as exc:
            logging.exception(
                "Не вдалося перевірити рядок %s для заявки %s: %s",
                req.sheet_row,
                req.id,
                exc,
            )

        return await self._find_row_by_request_id(req.id)

    async def _store_row_number(self, req_id: int, row_number: int):
        async with SessionLocal() as session:
            req = await session.get(Request, req_id)
            if not req:
                return
            req.sheet_row = row_number
            await session.commit()

    async def sync_request(self, req: "Request"):
        if not await self._ensure_client():
            return

        admin_name = await get_admin_display_name(req.admin_id)
        values = self._build_row(req, admin_name)

        row_number = await self._get_row_number(req)
        if row_number:
            updated = await self._update_row(row_number, values)
            if updated:
                if req.sheet_row != row_number:
                    await self._store_row_number(req.id, row_number)
            return

        row_number = await self._append_row(values)
        if row_number:
            await self._store_row_number(req.id, row_number)

    async def delete_request(self, req: "Request"):
        if not await self._ensure_client():
            return

        row_number = await self._get_row_number(req)
        if not row_number:
            return

        try:
            await asyncio.to_thread(self._worksheet.delete_rows, row_number)
        except Exception as exc:
            logging.exception("Не вдалося видалити рядок %s у Sheets: %s", row_number, exc)

    async def clear_requests(self):
        if not await self._ensure_client():
            return

        try:
            await asyncio.to_thread(self._worksheet.batch_clear, ["A2:Q"])
        except Exception as exc:
            logging.exception("Не вдалося очистити таблицю Sheets: %s", exc)


sheet_client = GoogleSheetClient()


###############################################################
#                     CONSTANTS & MENUS
###############################################################

BACK_TEXT = "↩️ Назад"
MAIN_MENU_TEXT = "🏠 Головне меню"

# Статусы, требующие действия администратора СЕЙЧАС
ADMIN_ACTION_REQUIRED_STATUSES = {"new", "pending_admin_decision"}
ACTIVE_STATUSES = {
    "new", "pending_user_confirmation", "pending_admin_decision",
    "pending_user_final", "approved",
}


def navigation_keyboard(include_back=True):
    buttons = [[KeyboardButton(text=MAIN_MENU_TEXT)]]
    if include_back:
        buttons.append([KeyboardButton(text=BACK_TEXT)])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def add_inline_navigation(builder: InlineKeyboardBuilder, back_callback: str | None = None):
    buttons = [InlineKeyboardButton(text=MAIN_MENU_TEXT, callback_data="go_main")]
    if back_callback:
        buttons.append(InlineKeyboardButton(text=BACK_TEXT, callback_data=back_callback))
    builder.row(*buttons)
    return builder


async def show_main_menu(message: types.Message, state: FSMContext):
    await state.clear()
    show_admin = await is_admin_user(message.from_user.id)
    await message.answer(
        "<b>🏠 DC Link черга | Головне меню</b>\n"
        "Оберіть, що зробити просто зараз:",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await message.answer(
        "📍 Керування доступними розділами:",
        reply_markup=main_menu(show_admin=show_admin),
    )


@dp.message(F.text == MAIN_MENU_TEXT)
async def handle_main_menu(message: types.Message, state: FSMContext):
    await show_main_menu(message, state)

@dp.callback_query(F.data == "go_main")
async def handle_main_menu_callback(callback: types.CallbackQuery, state: FSMContext):
    await show_main_menu(callback.message, state)
    await callback.answer()

async def is_admin_user(user_id: int) -> bool:
    if user_id == SUPERADMIN_ID:
        return True

    async with SessionLocal() as session:
        res = await session.execute(select(Admin).where(Admin.telegram_id == user_id))
        admin = res.scalar_one_or_none()

    return bool(admin)


def main_menu(show_admin: bool = False):
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Нова заявка", callback_data="menu_new")
    kb.button(text="📂 Мої останні заявки", callback_data="menu_my")
    if show_admin:
        kb.button(text="🛠 Адмін-панель", callback_data="menu_admin")
    kb.adjust(1)
    return kb.as_markup()

def delivery_type_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="🚚 Доставка постачальником", callback_data="delivery_supplier")
    kb.button(text="📦 Доставка НП/TEKS/Інше", callback_data="delivery_np")
    kb.adjust(1)
    return add_inline_navigation(kb).as_markup()

async def prompt_delivery_type(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "📦 Оберіть тип доставки для нової заявки:",
        reply_markup=delivery_type_keyboard(),
    )


async def count_pending_for_admin() -> int:
    async with SessionLocal() as session:
        res = await session.execute(
            select(Request)
            .where(Request.status.in_(ADMIN_ACTION_REQUIRED_STATUSES))
            .where(Request.completed_at.is_(None))
        )
        return len(res.scalars().all())


async def admin_menu(is_superadmin: bool = False):
    pending = await count_pending_for_admin()
    counter = f" ({pending})" if pending else ""
    kb = InlineKeyboardBuilder()
    kb.button(text=f"🔔 Потребують уваги{counter}", callback_data="admin_new")
    kb.button(text="📚 Усі заявки", callback_data="admin_all")
    kb.button(text="🔎 Пошук за ID", callback_data="admin_search")
    kb.button(text="📅 Посмотреть слоты очереди", callback_data="admin_slots_view")
    kb.button(text="📑 Експорт логів", callback_data="admin_logs_export")
    if is_superadmin:
        kb.button(text="➕ Додати адміна", callback_data="admin_add")
        kb.button(text="➖ Видалити адміна", callback_data="admin_remove")
        kb.button(text="🗑 Видалити обрані заявки", callback_data="admin_delete_selected")
        kb.button(text="🗑 Очистити БД", callback_data="admin_clear")
    kb.adjust(1)
    return add_inline_navigation(kb).as_markup()


async def is_super_admin_user(user_id: int) -> bool:
    if user_id == SUPERADMIN_ID:
        return True

    async with SessionLocal() as session:
        res = await session.execute(select(Admin).where(Admin.telegram_id == user_id))
        admin = res.scalar_one_or_none()

    return bool(admin and admin.is_superadmin)


async def get_admin_display_name(admin_id: int | None) -> str:
    if not admin_id:
        return ""
    if admin_id == SUPERADMIN_ID:
        return "Админ"

    async with SessionLocal() as session:
        res = await session.execute(select(Admin).where(Admin.telegram_id == admin_id))
        admin = res.scalar_one_or_none()

    if admin and admin.last_name:
        return admin.last_name

    if admin:
        return "Адміністратор"

    return ""


###############################################################
#                        FSM STATES                           
###############################################################

class QueueForm(StatesGroup):
    supplier = State()
    phone = State()
    vehicle_number = State()
    car = State()
    cargo_description = State()
    loading_type = State()
    calendar = State()
    hour = State()
    minute = State()

class AdminAdd(StatesGroup):
    wait_id = State()
    wait_last_name = State()

class AdminRemove(StatesGroup):
    wait_id = State()

class AdminSearch(StatesGroup):
    wait_id = State()

class AdminDeleteSelected(StatesGroup):
    wait_ids = State()

class AdminChangeForm(StatesGroup):
    calendar = State()
    hour = State()
    minute = State()
    reason = State()

class AdminRejectForm(StatesGroup):
    reason = State()

class AdminPlanView(StatesGroup):
    calendar = State()

class AdminLogsExport(StatesGroup):
    start_date = State()
    end_date = State()

class UserDeleteForm(StatesGroup):
    user_id = State()
    reason = State()

class UserEditForm(StatesGroup):
    user_id = State()
    field_choice = State()
    supplier = State()
    phone = State()
    vehicle_number = State()
    car = State()
    cargo_description = State()
    loading_type = State()
    calendar = State()
    new_date = State()
    hour = State()
    minute = State()
    new_time = State()
    reason = State()

class NPDeliveryForm(StatesGroup):
    supplier = State()
    ttn = State()

class UserChangeResponse(StatesGroup):
    decline_reason = State()
    delete_reason = State()
    propose_reason = State()
    calendar = State()
    hour = State()
    minute = State()

class AdminUserProposalReject(StatesGroup):
    reason = State()


###############################################################
#                 START → BEAUTIFUL RED CARD                  
###############################################################

@dp.message(CommandStart())
async def start(message: types.Message, state: FSMContext):
    await state.clear()
    show_admin = await is_admin_user(message.from_user.id)

    hero = (
        "<b>🚀 DC Link | Електронна черга постачальників</b>\n"
        "Вітаємо у нашому боті.\n\n"
        "• Створіть заявку за лічені кроки\n"
        "• Отримуйте рішення від оператора\n"
        "• Керуйте останніми заявками прямо з бота"
    )

    await message.answer(hero, reply_markup=navigation_keyboard(include_back=False))
    await message.answer(
        "Готові працювати? Оберіть розділ нижче:",
        reply_markup=main_menu(show_admin=show_admin),
    )


###############################################################
#                     MAIN MENU HANDLERS                      
###############################################################

@dp.callback_query(F.data == "menu_new")
async def menu_new(callback: types.CallbackQuery, state: FSMContext):
    await prompt_delivery_type(callback.message, state)
    await callback.answer()


@dp.callback_query(F.data == "delivery_supplier")
async def delivery_supplier(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer(
        "🏢 <b>Крок 1/7</b>\nВкажіть назву постачальника:",
        reply_markup=navigation_keyboard(include_back=False)
    )
    await state.set_state(QueueForm.supplier)
    await callback.answer()


@dp.callback_query(F.data == "delivery_np")
async def delivery_np(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(NPDeliveryForm.supplier)
    await callback.message.answer(
        "✉️ Введіть назву постачальника для доставки НП/TEKS/Інше:",
        reply_markup=navigation_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data == "menu_my")
async def menu_my(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    async with SessionLocal() as session:
        result = await session.execute(
            select(Request)
            .where(Request.user_id == user_id)
            .order_by(Request.id.desc())
            .limit(3)
        )
        rows = result.scalars().all()

    if not rows:
        return await callback.message.answer(
            "📂 Поки немає заявок. Створіть першу, щоб розпочати роботу."
        )

    text = (
        "<b>📂 Останні 3 заявки</b>\n"
        "Швидкий доступ до актуальних звернень:\n\n"
    )
    kb = InlineKeyboardBuilder()
    for req in rows:
        status = get_status_label(req.status)
        text += (
            f"• <b>#{req.id}</b> — "
            f"{req.date.strftime('%d.%m.%Y')} {req.time} — "
            f"{status}\n"
        )
        kb.button(
            text=f"#{req.id} ({req.date.strftime('%d.%m.%Y')} {req.time})",
            callback_data=f"my_view_{req.id}"
        )

    kb.button(text=MAIN_MENU_TEXT, callback_data="go_main")
    kb.adjust(1)

    await callback.message.answer(text, reply_markup=kb.as_markup())


def get_status_label(status: str) -> str:
    return {
        "new": "🟢 На розгляді",
        "approved": "✅ Підтверджена",
        "rejected": "❌ Відхилена",
        "deleted_by_user": "⛔ Скасована користувачем",
        "pending_user_confirmation": "🟡 Чекає підтвердження користувача",
        "pending_admin_decision": "🟠 Чекає рішення адміністратора",
        "pending_user_final": "🟡 Очікує остаточного підтвердження",
    }.get(status, status)


###############################################################
#            STATUS VISUALIZATION & ADMIN CARDS
###############################################################

def get_status_badge(req: "Request") -> str:
    if req.completed_at:
        return "🏁 <b>ЗАВЕРШЕНО</b>"
    status = req.status
    if status == "new":
        return "🟥 <b>НЕ ОБРОБЛЕНО</b>"
    if status == "approved":
        return "🟩 <b>ПІДТВЕРДЖЕНО</b>"
    if status == "rejected":
        return "⬛ <b>ВІДХИЛЕНО</b>"
    if status == "deleted_by_user":
        return "⛔ <b>СКАСОВАНО КОРИСТУВАЧЕМ</b>"
    if status == "pending_user_confirmation":
        return "🟨 <b>ЧЕКАЄ КОРИСТУВАЧА</b>"
    if status == "pending_admin_decision":
        return "🟧 <b>ПОТРІБНЕ РІШЕННЯ АДМІНА</b>"
    if status == "pending_user_final":
        return "🟨 <b>ЧЕКАЄ КОРИСТУВАЧА (фінал)</b>"
    return f"⚪ <b>{status.upper()}</b>"


def get_status_emoji(req: "Request") -> str:
    if req.completed_at:
        return "🏁"
    return {
        "new": "🟥",
        "approved": "🟩",
        "rejected": "⬛",
        "deleted_by_user": "⛔",
        "pending_user_confirmation": "🟨",
        "pending_admin_decision": "🟧",
        "pending_user_final": "🟨",
    }.get(req.status, "⚪")


def format_processed_by(req: "Request", admin_name: str) -> str:
    if req.status == "new" and not req.completed_at:
        return ""
    if not admin_name:
        return ""
    when = ""
    if req.updated_at:
        when = to_kyiv(req.updated_at).strftime("%d.%m.%Y %H:%M")
    return f"👤 <b>Обробив:</b> {admin_name}" + (f" • {when}" if when else "")


def get_confirmed_label(req: "Request") -> str:
    if req.status != "approved":
        return "—"
    return f"{req.date.strftime('%d.%m.%Y')} {req.time}"


def format_request_text(req: "Request") -> str:
    status = get_status_label(req.status)
    final_status = "Завершена" if req.completed_at else "Не завершена"
    planned_date = req.planned_date.strftime('%d.%m.%Y') if req.planned_date else req.date.strftime('%d.%m.%Y')
    planned_time = req.planned_time if req.planned_time else req.time
    confirmed = get_confirmed_label(req)
    return (
        f"<b>📄 Заявка #{req.id}</b>\n"
        f"Статус: {status}\n"
        "━━━━━━━━━━━━━━━━\n"
        f"🏢 <b>Постачальник:</b> {req.supplier}\n"
        f"📞 <b>Телефон:</b> {req.phone}\n"
        f"🚘 <b>Номер авто:</b> {req.vehicle_number or '—'}\n"
        f"🚚 <b>Об'єм:</b> {req.car}\n"
        f"📦 <b>Товар:</b> {req.cargo_description or ''}\n"
        f"🧱 <b>Тип завантаження:</b> {req.loading_type}\n"
        f"📅 <b>План:</b> {planned_date} {planned_time}\n"
        f"✅ <b>Підтверджено:</b> {confirmed}\n"
        f"🏁 <b>Статус завершення:</b> {final_status}"
    )


def build_recent_request_ids(reqs: list["Request"]) -> set[int]:
    return {req.id for req in reqs}


def set_updated_now(req: "Request"):
    req.updated_at = kyiv_now_naive()

def get_user_modify_block_reason(req: "Request") -> str | None:
    if req.status == "deleted_by_user":
        return "Заявка вже видалена"
    if req.completed_at:
        return "Заявка вже завершена, зміни неможливі"
    if req.status == "rejected":
        return "Заявка відхилена адміністратором, редагування неможливе"
    return None

def get_confirmed_datetime(req: "Request") -> datetime | None:
    if not req.date or not req.time:
        return None
    try:
        hour, minute = [int(x) for x in req.time.split(":")[:2]]
        return datetime.combine(req.date, dtime(hour=hour, minute=minute), tzinfo=KYIV_TZ)
    except Exception:
        return None


def build_user_change_keyboard(req_id: int, *, limited: bool = False):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Підтвердити", callback_data=f"user_change_confirm_{req_id}")
    if not limited:
        kb.button(text="🙅‍♂️ Відмовитися", callback_data=f"user_change_decline_{req_id}")
        kb.button(text="📅 Запропонувати нову дату/час", callback_data=f"user_change_propose_{req_id}")
    kb.button(text="🗑 Скасувати заявку", callback_data=f"user_change_delete_{req_id}")
    kb.adjust(1)
    kb.button(text=MAIN_MENU_TEXT, callback_data="go_main")
    return kb.as_markup()


def format_plan_datetime(req: "Request") -> str:
    plan_date = req.planned_date.strftime('%d.%m.%Y') if req.planned_date else ""
    plan_time = req.planned_time or ""
    return f"{plan_date} {plan_time}".strip()


def merge_pending_reason(existing: str | None, prefix: str, reason: str) -> str:
    reason = (reason or "").strip()
    if not reason:
        return existing or ""
    parts = [existing] if existing else []
    parts.append(f"{prefix}: {reason}")
    return "\n".join(filter(None, parts))


###############################################################
#            ADMIN CARD BUILDER (with badges & guards)
###############################################################

async def build_admin_request_view_async(req: "Request", is_superadmin: bool):
    admin_name = await get_admin_display_name(req.admin_id)
    badge = get_status_badge(req)
    processed = format_processed_by(req, admin_name)

    plan_date = req.planned_date.strftime('%d.%m.%Y') if req.planned_date else (req.date.strftime('%d.%m.%Y') if req.date else '—')
    plan_time = req.planned_time if req.planned_time else (req.time or '—')
    confirmed = get_confirmed_label(req)

    text = (
        f"{badge}\n"
        "━━━━━━━━━━━━━━━━\n"
        f"<b>📄 Заявка #{req.id}</b>\n"
        f"🏢 <b>Постачальник:</b> {req.supplier}\n"
        f"📞 <b>Телефон:</b> {req.phone}\n"
        f"🚘 <b>Номер авто:</b> {req.vehicle_number or '—'}\n"
        f"🚚 <b>Об'єм:</b> {req.car}\n"
        f"📦 <b>Товар:</b> {req.cargo_description or ''}\n"
        f"🧱 <b>Тип завантаження:</b> {req.loading_type}\n"
        f"📅 <b>План:</b> {plan_date} {plan_time}\n"
        f"✅ <b>Підтверджено:</b> {confirmed}\n"
    )
    if processed:
        text += f"{processed}\n"

    if req.pending_date and req.pending_time:
        text += (
            f"\n📝 <b>Пропозиція користувача:</b> "
            f"{req.pending_date.strftime('%d.%m.%Y')} {req.pending_time}"
        )
    if req.pending_reason:
        text += f"\nℹ️ <b>Коментар:</b> {req.pending_reason}"

    kb = InlineKeyboardBuilder()

    if req.completed_at:
        pass
    elif req.status == "new":
        kb.button(text="✔ Підтвердити", callback_data=f"adm_ok_{req.id}")
        kb.button(text="🔁 Змінити дату/час", callback_data=f"adm_change_{req.id}")
        kb.button(text="❌ Відхилити", callback_data=f"adm_rej_{req.id}")
    elif req.status == "pending_admin_decision":
        if req.pending_date and req.pending_time:
            kb.button(text="✅ Підтвердити час користувача", callback_data=f"adm_accept_user_proposal_{req.id}")
            kb.button(text="❌ Відхилити час користувача", callback_data=f"adm_reject_user_proposal_{req.id}")
        else:
            kb.button(text="✅ Залишити час користувача", callback_data=f"adm_user_keep_client_{req.id}")
            kb.button(text="🕒 Залишити час адміністратора", callback_data=f"adm_user_keep_admin_{req.id}")
        kb.button(text="🔁 Призначити інший час", callback_data=f"adm_change_{req.id}")
        kb.button(text="🛑 Відхилити заявку", callback_data=f"adm_rej_{req.id}")
    elif req.status in {"pending_user_confirmation", "pending_user_final"}:
        kb.button(text="🔁 Змінити дату/час", callback_data=f"adm_change_{req.id}")
        kb.button(text="❌ Відхилити", callback_data=f"adm_rej_{req.id}")
    elif req.status == "approved":
        kb.button(text="🔁 Змінити дату/час", callback_data=f"adm_change_{req.id}")
        kb.button(text="🏁 Завершити поставку", callback_data=f"adm_finish_{req.id}")
        kb.button(text="❌ Відхилити", callback_data=f"adm_rej_{req.id}")

    if is_superadmin or req.status != "new":
        kb.button(text="🗑 Видалити", callback_data=f"adm_del_{req.id}")

    kb.button(text="⬅️ До списку", callback_data="admin_all")
    kb.adjust(1)
    kb = add_inline_navigation(kb)
    return text, kb.as_markup()


async def refresh_admin_card(callback: types.CallbackQuery, req_id: int):
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        admin = (
            await session.execute(
                select(Admin).where(Admin.telegram_id == callback.from_user.id)
            )
        ).scalar_one_or_none()

    if not req:
        return

    is_superadmin = callback.from_user.id == SUPERADMIN_ID or (admin and admin.is_superadmin)
    text, markup = await build_admin_request_view_async(req, is_superadmin)
    try:
        await callback.message.edit_text(text, reply_markup=markup)
    except Exception:
        try:
            await callback.message.edit_reply_markup(reply_markup=markup)
        except Exception:
            pass


async def guard_already_processed(callback: types.CallbackQuery, req: "Request | None",
                                   allowed_statuses: set[str]) -> bool:
    if not req:
        await callback.answer("Заявку не знайдено.", show_alert=True)
        return True
    if req.completed_at:
        await callback.answer("Заявку вже завершено.", show_alert=True)
        return True
    if req.status not in allowed_statuses:
        admin_name = await get_admin_display_name(req.admin_id)
        who = f" ({admin_name})" if admin_name else ""
        await callback.answer(
            f"⚠️ Заявку вже опрацьовано{who}. Статус: {get_status_label(req.status)}",
            show_alert=True,
        )
        return True
    return False


async def send_request_details(
    req: "Request",
    callback_or_message: "types.CallbackQuery | types.Message",
    *,
    allow_actions: bool,
    recent_ids: set[int] | None = None,
):
    kb = InlineKeyboardBuilder()
    if (
        allow_actions
        and req.id in (recent_ids or set())
        and not get_user_modify_block_reason(req)
    ):
        kb.button(text="✏️ Змінити", callback_data=f"my_edit_{req.id}")
        kb.button(text="🗑 Видалити", callback_data=f"my_delete_{req.id}")
    kb.button(text="⬅️ Мої заявки", callback_data="menu_my")
    kb.button(text=MAIN_MENU_TEXT, callback_data="go_main")
    kb.adjust(1)

    text = format_request_text(req)

    target_message = (
        callback_or_message.message if isinstance(callback_or_message, types.CallbackQuery)
        else callback_or_message
    )

    await target_message.answer(text, reply_markup=kb.as_markup())

    if isinstance(callback_or_message, types.CallbackQuery):
        await callback_or_message.answer()


async def get_user_recent_requests(user_id: int) -> list["Request"]:
    async with SessionLocal() as session:
        result = await session.execute(
            select(Request)
            .where(Request.user_id == user_id)
            .order_by(Request.id.desc())
            .limit(3)
        )
        return result.scalars().all()


@dp.callback_query(F.data.startswith("my_view_"))
async def my_view(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)

    if not req or req.user_id != user_id:
        return await callback.answer("Заявка не знайдена", show_alert=True)

    block_reason = get_user_modify_block_reason(req)
    if block_reason:
        recent = await get_user_recent_requests(user_id)
        await send_request_details(
            req, callback, allow_actions=False,
            recent_ids=build_recent_request_ids(recent),
        )
        return

    recent = await get_user_recent_requests(user_id)
    await send_request_details(req, callback, allow_actions=True, recent_ids=build_recent_request_ids(recent))


def is_request_recent(req_id: int, recent_ids: set[int]) -> bool:
    return req_id in recent_ids


@dp.callback_query(F.data.startswith("my_delete_"))
async def my_delete(callback: types.CallbackQuery, state: FSMContext):
    req_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    recent = await get_user_recent_requests(user_id)
    recent_ids = build_recent_request_ids(recent)

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)

    if not req or req.user_id != user_id:
        return await callback.answer("Заявка не знайдена", show_alert=True)

    if not is_request_recent(req_id, recent_ids):
        return await callback.answer("Можна керувати лише останніми 3 заявками", show_alert=True)

    block_reason = get_user_modify_block_reason(req)
    if block_reason:
        return await callback.answer(block_reason, show_alert=True)

    await state.set_state(UserDeleteForm.reason)
    await state.update_data(req_id=req_id)
    await callback.message.answer(
        "Вкажіть причину видалення заявки:", reply_markup=navigation_keyboard(include_back=False)
    )
    await callback.answer()


async def notify_admins_about_user_deletion(req: "Request | dict[str, Any]", reason: str):
    async with SessionLocal() as session:
        admins = (await session.execute(select(Admin))).scalars().all()

    if isinstance(req, Request):
        data = {
            "id": req.id, "supplier": req.supplier, "phone": req.phone,
            "vehicle_number": req.vehicle_number, "car": req.car,
            "loading_type": req.loading_type, "date": req.date, "time": req.time,
        }
    else:
        data = req

    text = (
        f"❗ Поставщик {data['supplier']} видалив заявку #{data['id']}\n"
        f"Причина: {reason}\n\n"
        f"📄 Дані заявки до видалення:\n"
        f"📞 {data['phone']}\n"
        f"🚘 {data.get('vehicle_number') or '—'}\n"
        f"🚚 {data['car']}\n"
        f"🧱 {data['loading_type']}\n"
        f"📅 {data['date'].strftime('%d.%m.%Y')} ⏰ {data['time']}"
    )

    for admin in admins:
        try:
            await bot.send_message(admin.telegram_id, text)
        except:
            pass


@dp.message(UserDeleteForm.reason)
async def my_delete_reason(message: types.Message, state: FSMContext):
    reason = message.text.strip()
    data = await state.get_data()
    req_id = data.get("req_id")

    if not reason:
        return await message.answer("Причина не може бути порожньою.")

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or req.user_id != message.from_user.id:
            await state.clear()
            return await message.answer("Заявка не знайдена або вам не належить.")

        block_reason = get_user_modify_block_reason(req)
        if block_reason:
            await state.clear()
            return await message.answer(block_reason)

        req_data = {
            "id": req.id, "supplier": req.supplier, "phone": req.phone,
            "vehicle_number": req.vehicle_number, "car": req.car,
            "loading_type": req.loading_type, "date": req.date, "time": req.time,
        }

        await session.delete(req)
        await session.commit()

    await log_action(
        message.from_user.id, "user", "request_deleted",
        {"request_id": req_id, "reason": reason},
    )

    await sheet_client.delete_request(req)
    await notify_admins_about_user_deletion(req_data, reason)
    await message.answer(
        "Заявку видалено з бази. Адміністратори отримали повідомлення.",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await state.clear()


@dp.callback_query(F.data.startswith("my_edit_"))
async def my_edit(callback: types.CallbackQuery, state: FSMContext):
    req_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    recent = await get_user_recent_requests(user_id)
    recent_ids = build_recent_request_ids(recent)

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)

    if not req or req.user_id != user_id:
        return await callback.answer("Заявка не знайдена", show_alert=True)

    if not is_request_recent(req_id, recent_ids):
        return await callback.answer("Можна керувати лише останніми 3 заявками", show_alert=True)

    block_reason = get_user_modify_block_reason(req)
    if block_reason:
        return await callback.answer(block_reason, show_alert=True)

    await state.set_state(UserEditForm.reason)
    await state.update_data(req_id=req_id)
    await callback.message.answer(
        "Вкажіть причину зміни заявки:", reply_markup=navigation_keyboard(include_back=False)
    )
    await callback.answer()


def build_user_edit_choice_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="🏢 Постачальник", callback_data="edit_field_supplier")
    kb.button(text="📞 Телефон", callback_data="edit_field_phone")
    kb.button(text="🚘 Номер авто", callback_data="edit_field_vehicle_number")
    kb.button(text="🚚 Об'єм", callback_data="edit_field_car")
    kb.button(text="📦 Товар", callback_data="edit_field_cargo_description")
    kb.button(text="🧱 Тип завантаження", callback_data="edit_field_loading")
    kb.button(text="📅 Дата та час", callback_data="edit_field_datetime")
    kb.adjust(1)
    return add_inline_navigation(kb, back_callback="edit_cancel").as_markup()

@dp.message(UserEditForm.reason)
async def my_edit_reason(message: types.Message, state: FSMContext):
    reason = message.text.strip()
    data = await state.get_data()
    req_id = data.get("req_id")

    if not reason:
        return await message.answer("Причина не може бути порожньою.")

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or req.user_id != message.from_user.id:
            await state.clear()
            return await message.answer("Заявка не знайдена або вам не належить.")

        block_reason = get_user_modify_block_reason(req)
        if block_reason:
            await state.clear()
            return await message.answer(block_reason)

    await state.update_data(reason=reason)
    await state.set_state(UserEditForm.field_choice)
    await message.answer(
        "Оберіть, що потрібно змінити у заявці:",
        reply_markup=build_user_edit_choice_keyboard(),
    )


async def finalize_user_edit_update(
    message_or_callback: "types.Message | types.CallbackQuery",
    state: FSMContext,
    req: "Request",
    reason: str,
    *,
    text: str,
    changes: list[tuple[str, str, str]],
):
    req.status = "new"
    req.admin_id = None
    set_updated_now(req)

    async with SessionLocal() as session:
        session.add(req)
        await session.commit()

    await log_action(
        req.user_id, "user", "request_updated",
        {
            "request_id": req.id, "reason": reason,
            "changes": [{"field": label, "old": old, "new": new} for label, old, new in changes],
        },
    )

    target = message_or_callback.message if isinstance(message_or_callback, types.CallbackQuery) else message_or_callback
    await target.answer(text, reply_markup=navigation_keyboard(include_back=False))
    await sheet_client.sync_request(req)
    await notify_admins_about_user_edit(req, reason, changes)
    await state.clear()
    if isinstance(message_or_callback, types.CallbackQuery):
        await message_or_callback.answer()


async def _load_request_for_edit(state: FSMContext, user_id: int) -> tuple["Request | None", str | None]:
    data = await state.get_data()
    req_id = data.get("req_id")
    reason = data.get("reason")

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)

    if not req or req.user_id != user_id:
        await state.clear()
        return None, None

    block_reason = get_user_modify_block_reason(req)
    if block_reason:
        await state.clear()
        return None, None

    return req, reason


@dp.message(UserEditForm.supplier)
async def user_edit_supplier(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(UserEditForm.field_choice)
        return await message.answer(
            "Оберіть, що потрібно змінити у заявці:",
            reply_markup=build_user_edit_choice_keyboard(),
        )
    value = message.text.strip()
    if not value:
        return await message.answer("Значення не може бути порожнім.")
    req, reason = await _load_request_for_edit(state, message.from_user.id)
    if not req:
        return await message.answer("Заявка не знайдена або вам не належить.")
    old_value = req.supplier
    req.supplier = value
    await finalize_user_edit_update(
        message, state, req, reason or "",
        text=f"Поле 'Постачальник' оновлено для заявки #{req.id}.",
        changes=[("Постачальник", old_value, req.supplier)],
    )

@dp.message(UserEditForm.phone)
async def user_edit_phone(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(UserEditForm.field_choice)
        return await message.answer(
            "Оберіть, що потрібно змінити у заявці:",
            reply_markup=build_user_edit_choice_keyboard(),
        )
    value = message.text.strip()
    if not value:
        return await message.answer("Значення не може бути порожнім.")
    req, reason = await _load_request_for_edit(state, message.from_user.id)
    if not req:
        return await message.answer("Заявка не знайдена або вам не належить.")
    old_value = req.phone
    req.phone = value
    await finalize_user_edit_update(
        message, state, req, reason or "",
        text=f"Поле 'Телефон' оновлено для заявки #{req.id}.",
        changes=[("Телефон", old_value, req.phone)],
    )


@dp.message(UserEditForm.vehicle_number)
async def user_edit_vehicle_number(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(UserEditForm.field_choice)
        return await message.answer(
            "Оберіть, що потрібно змінити у заявці:",
            reply_markup=build_user_edit_choice_keyboard(),
        )
    value = message.text.strip()
    if not value:
        return await message.answer("Номер авто не може бути порожнім.")
    req, reason = await _load_request_for_edit(state, message.from_user.id)
    if not req:
        return await message.answer("Заявка не знайдена або вам не належить.")
    old_value = req.vehicle_number or ""
    req.vehicle_number = value
    await finalize_user_edit_update(
        message, state, req, reason or "",
        text=f"Поле 'Номер авто' оновлено для заявки #{req.id}.",
        changes=[("Номер авто", old_value, req.vehicle_number)],
    )


@dp.message(UserEditForm.car)
async def user_edit_car(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(UserEditForm.field_choice)
        return await message.answer(
            "Оберіть, що потрібно змінити у заявці:",
            reply_markup=build_user_edit_choice_keyboard(),
        )
    value = message.text.strip()
    if not value:
        return await message.answer("Значення не може бути порожнім.")
    req, reason = await _load_request_for_edit(state, message.from_user.id)
    if not req:
        return await message.answer("Заявка не знайдена або вам не належить.")
    old_value = req.car
    req.car = value
    await finalize_user_edit_update(
        message, state, req, reason or "",
        text=f"Поле 'Об'єм' оновлено для заявки #{req.id}.",
        changes=[("Об'єм", old_value, req.car)],
    )


@dp.message(UserEditForm.cargo_description)
async def user_edit_cargo_description(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(UserEditForm.field_choice)
        return await message.answer(
            "Оберіть, що потрібно змінити у заявці:",
            reply_markup=build_user_edit_choice_keyboard(),
        )
    value = message.text.strip()
    if not value:
        return await message.answer("Значення не може бути порожнім.")
    req, reason = await _load_request_for_edit(state, message.from_user.id)
    if not req:
        return await message.answer("Заявка не знайдена або вам не належить.")
    old_value = req.cargo_description
    req.cargo_description = value
    await finalize_user_edit_update(
        message, state, req, reason or "",
        text=f"Поле 'Товар' оновлено для заявки #{req.id}.",
        changes=[("Товар", old_value, req.cargo_description)],
    )


def build_loading_type_keyboard_for_edit():
    kb = InlineKeyboardBuilder()
    kb.button(text="🚛 Бокове", callback_data="edit_loading_side")
    kb.button(text="🔝 Верхнє", callback_data="edit_loading_top")
    kb.button(text="🔙 Заднє", callback_data="edit_loading_back")
    kb.adjust(1)
    return add_inline_navigation(kb, back_callback="edit_back_to_fields").as_markup()


@dp.callback_query(F.data == "edit_cancel")
async def edit_cancel(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("Редагування скасовано.", reply_markup=navigation_keyboard(include_back=False))
    await callback.answer()


@dp.callback_query(F.data == "edit_back_to_fields")
async def edit_back_to_fields(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(UserEditForm.field_choice)
    await callback.message.answer(
        "Оберіть, що потрібно змінити у заявці:",
        reply_markup=build_user_edit_choice_keyboard(),
    )
    await callback.answer()


@dp.callback_query(UserEditForm.field_choice, F.data.startswith("edit_field_"))
async def user_edit_field_choice(callback: types.CallbackQuery, state: FSMContext):
    field = callback.data.replace("edit_field_", "")
    prompts = {
        "supplier": (UserEditForm.supplier, "🏢 Введіть нову назву постачальника:"),
        "phone": (UserEditForm.phone, "📞 Введіть новий телефон:"),
        "vehicle_number": (UserEditForm.vehicle_number, "🚘 Введіть новий номер авто:"),
        "car": (UserEditForm.car, "🚚 Введіть новий об'єм/тип авто:"),
        "cargo_description": (UserEditForm.cargo_description, "📦 Введіть новий опис товару:"),
    }

    if field == "loading":
        await state.set_state(UserEditForm.loading_type)
        await callback.message.answer(
            "🧱 Оберіть новий тип завантаження:",
            reply_markup=build_loading_type_keyboard_for_edit(),
        )
        return await callback.answer()

    if field == "datetime":
        await state.set_state(UserEditForm.calendar)
        await state.update_data(cal_year=kyiv_now().year, cal_month=kyiv_now().month)
        await callback.message.answer(
            "📅 Оберіть нову дату:",
            reply_markup=build_calendar(kyiv_now().year, kyiv_now().month, prefix="uedit"),
        )
        return await callback.answer()

    state_target, prompt = prompts[field]
    await state.set_state(state_target)
    await callback.message.answer(prompt, reply_markup=navigation_keyboard())
    await callback.answer()


@dp.callback_query(UserEditForm.loading_type, F.data.startswith("edit_loading_"))
async def user_edit_loading(callback: types.CallbackQuery, state: FSMContext):
    mapping = {"side": "Бокове", "top": "Верхнє", "back": "Заднє"}
    key = callback.data.replace("edit_loading_", "")
    value = mapping.get(key)
    if not value:
        return await callback.answer("Невідомий тип", show_alert=True)

    req, reason = await _load_request_for_edit(state, callback.from_user.id)
    if not req:
        await callback.answer()
        return await callback.message.answer("Заявка не знайдена або вам не належить.")

    old_value = req.loading_type
    req.loading_type = value
    await finalize_user_edit_update(
        callback, state, req, reason or "",
        text=f"Поле 'Тип завантаження' оновлено для заявки #{req.id}.",
        changes=[("Тип завантаження", old_value, req.loading_type)],
    )


###############################################################
#                        CALENDAR
###############################################################

MONTHS_UA = [
    "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
    "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень",
]
WEEKDAYS_UA = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]


def build_calendar(year: int, month: int, prefix: str = "cal", min_date: date | None = None):
    import calendar as cal_module
    kb = InlineKeyboardBuilder()
    kb.button(text=f"{MONTHS_UA[month - 1]} {year}", callback_data="cal_ignore")
    for wd in WEEKDAYS_UA:
        kb.button(text=wd, callback_data="cal_ignore")

    month_calendar = cal_module.monthcalendar(year, month)
    today = kyiv_now().date()
    _min = min_date or today

    for week in month_calendar:
        for day in week:
            if day == 0:
                kb.button(text=" ", callback_data="cal_ignore")
            else:
                d = date(year, month, day)
                if d < _min:
                    kb.button(text="·", callback_data="cal_ignore")
                else:
                    kb.button(text=str(day), callback_data=f"{prefix}_day_{year}_{month}_{day}")

    prev_month = month - 1 or 12
    prev_year = year - 1 if month == 1 else year
    next_month = month + 1 if month < 12 else 1
    next_year = year + 1 if month == 12 else year

    kb.button(text="◀️", callback_data=f"{prefix}_nav_{prev_year}_{prev_month}")
    kb.button(text=MAIN_MENU_TEXT, callback_data="go_main")
    kb.button(text="▶️", callback_data=f"{prefix}_nav_{next_year}_{next_month}")

    kb.adjust(1, 7, *[7] * len(month_calendar), 3)
    return kb.as_markup()


def build_hours_keyboard(prefix: str = "hour"):
    kb = InlineKeyboardBuilder()
    for h in range(8, 21):
        kb.button(text=f"{h:02d}:00", callback_data=f"{prefix}_{h}")
    kb.adjust(4)
    kb.button(text=MAIN_MENU_TEXT, callback_data="go_main")
    return kb.as_markup()


def build_minutes_keyboard(prefix: str = "minute"):
    kb = InlineKeyboardBuilder()
    for m in (0, 15, 30, 45):
        kb.button(text=f"{m:02d}", callback_data=f"{prefix}_{m}")
    kb.adjust(4)
    kb.button(text=MAIN_MENU_TEXT, callback_data="go_main")
    return kb.as_markup()


@dp.callback_query(F.data == "cal_ignore")
async def cal_ignore(callback: types.CallbackQuery):
    await callback.answer()


###############################################################
#                   NEW REQUEST FLOW (USER)
###############################################################

@dp.message(QueueForm.supplier)
async def form_supplier(message: types.Message, state: FSMContext):
    value = message.text.strip()
    if not value:
        return await message.answer("Назва не може бути порожньою.")
    await state.update_data(supplier=value)
    await state.set_state(QueueForm.phone)
    await message.answer("📞 <b>Крок 2/7</b>\nВкажіть контактний телефон:", reply_markup=navigation_keyboard())


@dp.message(QueueForm.phone)
async def form_phone(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.supplier)
        return await message.answer("🏢 Вкажіть назву постачальника:", reply_markup=navigation_keyboard(include_back=False))
    value = message.text.strip()
    if not value:
        return await message.answer("Телефон не може бути порожнім.")
    await state.update_data(phone=value)
    await state.set_state(QueueForm.vehicle_number)
    await message.answer("🚘 <b>Крок 3/7</b>\nВкажіть номер авто:", reply_markup=navigation_keyboard())


@dp.message(QueueForm.vehicle_number)
async def form_vehicle_number(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.phone)
        return await message.answer("📞 Вкажіть контактний телефон:", reply_markup=navigation_keyboard())
    value = message.text.strip()
    if not value:
        return await message.answer("Номер авто не може бути порожнім.")
    await state.update_data(vehicle_number=value)
    await state.set_state(QueueForm.car)
    await message.answer("🚚 <b>Крок 4/7</b>\nВкажіть об'єм/тип авто:", reply_markup=navigation_keyboard())


@dp.message(QueueForm.car)
async def form_car(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.vehicle_number)
        return await message.answer("🚘 Вкажіть номер авто:", reply_markup=navigation_keyboard())
    value = message.text.strip()
    if not value:
        return await message.answer("Значення не може бути порожнім.")
    await state.update_data(car=value)
    await state.set_state(QueueForm.cargo_description)
    await message.answer("📦 <b>Крок 5/7</b>\nОпишіть товар:", reply_markup=navigation_keyboard())


@dp.message(QueueForm.cargo_description)
async def form_cargo_description(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.car)
        return await message.answer("🚚 Вкажіть об'єм/тип авто:", reply_markup=navigation_keyboard())
    value = message.text.strip()
    if not value:
        return await message.answer("Опис не може бути порожнім.")
    await state.update_data(cargo_description=value)
    await state.set_state(QueueForm.loading_type)
    kb = InlineKeyboardBuilder()
    kb.button(text="🚛 Бокове", callback_data="load_side")
    kb.button(text="🔝 Верхнє", callback_data="load_top")
    kb.button(text="🔙 Заднє", callback_data="load_back")
    kb.adjust(1)
    await message.answer(
        "🧱 <b>Крок 6/7</b>\nОберіть тип завантаження:",
        reply_markup=add_inline_navigation(kb).as_markup(),
    )


@dp.callback_query(QueueForm.loading_type, F.data.startswith("load_"))
async def form_loading(callback: types.CallbackQuery, state: FSMContext):
    mapping = {"side": "Бокове", "top": "Верхнє", "back": "Заднє"}
    value = mapping.get(callback.data.replace("load_", ""))
    if not value:
        return await callback.answer("Невідомий тип", show_alert=True)
    await state.update_data(loading_type=value)
    min_dt = min_planned_datetime()
    await state.update_data(min_plan_dt=min_dt.isoformat(),
                            cal_year=min_dt.year, cal_month=min_dt.month)
    await state.set_state(QueueForm.calendar)
    await callback.message.answer(
        "📅 <b>Крок 7/7</b>\nОберіть бажану дату:",
        reply_markup=build_calendar(min_dt.year, min_dt.month, min_date=min_dt.date()),
    )
    await callback.answer()


@dp.callback_query(QueueForm.calendar, F.data.startswith("cal_nav_"))
async def form_cal_nav(callback: types.CallbackQuery, state: FSMContext):
    _, _, y, m = callback.data.split("_")
    data = await state.get_data()
    min_date = get_min_date_from_state(data)
    await callback.message.edit_reply_markup(
        reply_markup=build_calendar(int(y), int(m), min_date=min_date)
    )
    await callback.answer()


@dp.callback_query(QueueForm.calendar, F.data.startswith("cal_day_"))
async def form_cal_day(callback: types.CallbackQuery, state: FSMContext):
    _, _, y, m, d = callback.data.split("_")
    chosen = date(int(y), int(m), int(d))
    data = await state.get_data()
    min_date = get_min_date_from_state(data)
    if min_date and chosen < min_date:
        return await callback.answer("Оберіть коректну дату.", show_alert=True)
    await state.update_data(planned_date=chosen.isoformat())
    await state.set_state(QueueForm.hour)
    await callback.message.answer("⏰ Оберіть годину:", reply_markup=build_hours_keyboard())
    await callback.answer()


@dp.callback_query(QueueForm.hour, F.data.startswith("hour_"))
async def form_hour(callback: types.CallbackQuery, state: FSMContext):
    hour = int(callback.data.split("_")[1])
    await state.update_data(hour=hour)
    await state.set_state(QueueForm.minute)
    await callback.message.answer("⏰ Оберіть хвилини:", reply_markup=build_minutes_keyboard())
    await callback.answer()


@dp.callback_query(QueueForm.minute, F.data.startswith("minute_"))
async def form_minute(callback: types.CallbackQuery, state: FSMContext):
    minute = int(callback.data.split("_")[1])
    data = await state.get_data()
    planned_date = date.fromisoformat(data["planned_date"])
    hour = data["hour"]
    planned_time = f"{hour:02d}:{minute:02d}"

    min_dt = get_min_datetime_from_state(data) or min_planned_datetime()
    chosen_dt = datetime.combine(planned_date, dtime(hour=hour, minute=minute), tzinfo=KYIV_TZ)
    if chosen_dt < min_dt:
        return await callback.answer("Оберіть час не раніше ніж за годину від зараз.", show_alert=True)

    async with SessionLocal() as session:
        req = Request(
            user_id=callback.from_user.id,
            supplier=data["supplier"],
            phone=data["phone"],
            vehicle_number=data.get("vehicle_number"),
            car=data["car"],
            cargo_description=data.get("cargo_description"),
            loading_type=data["loading_type"],
            planned_date=planned_date,
            planned_time=planned_time,
            date=planned_date,
            time=planned_time,
            status="new",
            created_at=kyiv_now_naive(),
            updated_at=kyiv_now_naive(),
        )
        session.add(req)
        await session.commit()
        await session.refresh(req)

    await log_action(
        callback.from_user.id, "user", "request_created",
        {"request_id": req.id, "supplier": req.supplier,
         "date": planned_date.isoformat(), "time": planned_time},
    )

    await callback.message.answer(
        f"✅ <b>Заявку #{req.id} створено!</b>\n"
        f"📅 {planned_date.strftime('%d.%m.%Y')} ⏰ {planned_time}\n"
        "Очікуйте на рішення адміністратора.",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await state.clear()
    await callback.answer()

    await sheet_client.sync_request(req)
    await broadcast_new_request(req.id)


async def broadcast_new_request(req_id: int):
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        admins = (await session.execute(select(Admin))).scalars().all()

    for admin in admins:
        is_super = admin.telegram_id == SUPERADMIN_ID or admin.is_superadmin
        text, markup = await build_admin_request_view_async(req, is_super)
        try:
            await bot.send_message(admin.telegram_id, text, reply_markup=markup)
        except Exception:
            pass


async def notify_admins_about_action(req: "Request", action_text: str):
    async with SessionLocal() as session:
        admins = (await session.execute(select(Admin))).scalars().all()
    txt = f"🔔 Заявка #{req.id} ({req.supplier}) {action_text}."
    for admin in admins:
        try:
            await bot.send_message(admin.telegram_id, txt)
        except Exception:
            pass


async def notify_admins_about_user_edit(req: "Request", reason: str, changes: list[tuple[str, str, str]]):
    async with SessionLocal() as session:
        admins = (await session.execute(select(Admin))).scalars().all()
    lines = "\n".join(f"• {label}: {old} → {new}" for label, old, new in changes)
    txt = (
        f"✏️ Користувач змінив заявку #{req.id}\n"
        f"Причина: {reason}\n{lines}"
    )
    for admin in admins:
        try:
            await bot.send_message(admin.telegram_id, txt)
        except Exception:
            pass


###############################################################
#                   NP DELIVERY FLOW
###############################################################

@dp.message(NPDeliveryForm.supplier)
async def np_supplier(message: types.Message, state: FSMContext):
    value = message.text.strip()
    if not value:
        return await message.answer("Назва не може бути порожньою.")
    await state.update_data(supplier=value)
    await state.set_state(NPDeliveryForm.ttn)
    await message.answer("📮 Введіть номер ТТН:", reply_markup=navigation_keyboard())


@dp.message(NPDeliveryForm.ttn)
async def np_ttn(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(NPDeliveryForm.supplier)
        return await message.answer("✉️ Введіть назву постачальника:", reply_markup=navigation_keyboard())
    ttn = message.text.strip()
    if not ttn:
        return await message.answer("ТТН не може бути порожнім.")
    data = await state.get_data()
    supplier = data["supplier"]

    saved = await sheet_client.append_np_delivery(supplier, ttn)
    await log_action(
        message.from_user.id, "user", "np_delivery_submitted",
        {"supplier": supplier, "ttn": ttn, "saved_to_sheet": saved},
    )
    await message.answer(
        f"✅ Заявку на доставку поштою прийнято!\n🏢 {supplier}\n📮 ТТН: {ttn}",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await state.clear()


###############################################################
#                     ADMIN PANEL
###############################################################

@dp.callback_query(F.data == "menu_admin")
async def menu_admin_handler(callback: types.CallbackQuery):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)
    is_superadmin = await is_super_admin_user(callback.from_user.id)
    await callback.message.answer(
        "🛠 <b>Адмін-панель</b>\nКеруйте заявками та доступами:",
        reply_markup=await admin_menu(is_superadmin=is_superadmin),
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_new")
async def admin_new(callback: types.CallbackQuery):
    async with SessionLocal() as session:
        res = await session.execute(
            select(Request)
            .where(Request.status.in_(ADMIN_ACTION_REQUIRED_STATUSES))
            .where(Request.completed_at.is_(None))
            .order_by(Request.id.desc())
        )
        rows = res.scalars().all()

    if not rows:
        return await callback.message.answer(
            "🟢 Немає заявок, що потребують уваги. Усі звернення опрацьовані."
        )

    text = "<b>🔔 Потребують уваги</b>\nЗаявки, що чекають на ваше рішення:\n\n"
    kb = InlineKeyboardBuilder()
    for r in rows:
        emoji = get_status_emoji(r)
        plan_d = r.planned_date.strftime('%d.%m.%Y') if r.planned_date else '—'
        text += f"{emoji} <b>#{r.id}</b> — {r.supplier} — {plan_d} {r.planned_time or ''} — {get_status_label(r.status)}\n"
        kb.button(text=f"{emoji} #{r.id} — {r.supplier}", callback_data=f"admin_view_{r.id}")
    kb.button(text=MAIN_MENU_TEXT, callback_data="go_main")
    kb.adjust(1)
    await callback.message.answer(text, reply_markup=kb.as_markup())


@dp.callback_query(F.data == "admin_all")
async def admin_all(callback: types.CallbackQuery):
    async with SessionLocal() as session:
        res = await session.execute(select(Request).order_by(Request.id.desc()).limit(20))
        rows = res.scalars().all()

    if not rows:
        return await callback.message.answer("⚪ У базі ще немає заявок.")

    text = "<b>📚 Останні 20 заявок</b>\n\n"
    kb = InlineKeyboardBuilder()
    for r in rows:
        emoji = get_status_emoji(r)
        status = get_status_label(r.status)
        d = r.date.strftime('%d.%m.%Y') if r.date else '—'
        text += f"{emoji} <b>#{r.id}</b>  {r.supplier}  —  {d} {r.time or ''}  —  {status}\n"
        kb.button(text=f"{emoji} #{r.id} — {r.supplier} — {d} {r.time or ''}",
                  callback_data=f"admin_view_{r.id}")
    kb.button(text=MAIN_MENU_TEXT, callback_data="go_main")
    kb.adjust(1)
    await callback.message.answer(text, reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("admin_view_"))
async def admin_view(callback: types.CallbackQuery):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)
    req_id = int(callback.data.split("_")[2])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
    if not req:
        return await callback.answer("Заявка не знайдена", show_alert=True)
    is_superadmin = await is_super_admin_user(callback.from_user.id)
    text, markup = await build_admin_request_view_async(req, is_superadmin)
    await callback.message.answer(text, reply_markup=markup)
    await callback.answer()


@dp.callback_query(F.data == "admin_search")
async def admin_search(callback: types.CallbackQuery, state: FSMContext):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)
    await state.set_state(AdminSearch.wait_id)
    await callback.message.answer("🔎 Введіть ID заявки:", reply_markup=navigation_keyboard(include_back=False))
    await callback.answer()


@dp.message(AdminSearch.wait_id)
async def admin_search_id(message: types.Message, state: FSMContext):
    raw = message.text.strip()
    if not raw.isdigit():
        return await message.answer("Введіть числовий ID.")
    req_id = int(raw)
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
    await state.clear()
    if not req:
        return await message.answer("Заявка не знайдена.", reply_markup=navigation_keyboard(include_back=False))
    is_superadmin = await is_super_admin_user(message.from_user.id)
    text, markup = await build_admin_request_view_async(req, is_superadmin)
    await message.answer(text, reply_markup=markup)


###############################################################
#           ADMIN: APPROVE / REJECT / CHANGE / FINISH
###############################################################

async def notify_user(user_id: int, text: str, reply_markup=None):
    try:
        await bot.send_message(user_id, text, reply_markup=reply_markup)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("adm_ok_"))
async def adm_ok(callback: types.CallbackQuery):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)
    req_id = int(callback.data.split("_")[2])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if await guard_already_processed(callback, req, {"new"}):
            return
        req.status = "approved"
        req.admin_id = callback.from_user.id
        req.date = req.planned_date
        req.time = req.planned_time
        set_updated_now(req)
        session.add(req)
        await session.commit()

    await log_action(callback.from_user.id, "admin", "request_approved", {"request_id": req_id})
    await notify_user(
        req.user_id,
        f"✅ Вашу заявку #{req.id} підтверджено!\n"
        f"📅 {req.date.strftime('%d.%m.%Y')} ⏰ {req.time}"
    )
    await sheet_client.sync_request(req)
    await refresh_admin_card(callback, req_id)
    await callback.answer("Підтверджено")


@dp.callback_query(F.data.startswith("adm_rej_"))
async def adm_rej(callback: types.CallbackQuery, state: FSMContext):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)
    req_id = int(callback.data.split("_")[2])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
    if not req:
        return await callback.answer("Заявка не знайдена", show_alert=True)
    if req.completed_at:
        return await callback.answer("Заявку вже завершено.", show_alert=True)
    await state.set_state(AdminRejectForm.reason)
    await state.update_data(req_id=req_id)
    await callback.message.answer("Вкажіть причину відхилення:", reply_markup=navigation_keyboard(include_back=False))
    await callback.answer()


@dp.message(AdminRejectForm.reason)
async def adm_rej_reason(message: types.Message, state: FSMContext):
    reason = message.text.strip()
    if not reason:
        return await message.answer("Причина не може бути порожньою.")
    data = await state.get_data()
    req_id = data["req_id"]
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req:
            await state.clear()
            return await message.answer("Заявка не знайдена.")
        req.status = "rejected"
        req.admin_id = message.from_user.id
        set_updated_now(req)
        session.add(req)
        await session.commit()

    await log_action(message.from_user.id, "admin", "request_rejected", {"request_id": req_id, "reason": reason})
    await notify_user(req.user_id, f"❌ Вашу заявку #{req.id} відхилено.\nПричина: {reason}")
    await sheet_client.sync_request(req)
    await message.answer(f"Заявку #{req_id} відхилено.", reply_markup=navigation_keyboard(include_back=False))
    await state.clear()


@dp.callback_query(F.data.startswith("adm_finish_"))
async def adm_finish(callback: types.CallbackQuery):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)
    req_id = int(callback.data.split("_")[2])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req:
            return await callback.answer("Заявка не знайдена", show_alert=True)
        if req.completed_at:
            return await callback.answer("Вже завершено.", show_alert=True)
        req.completed_at = kyiv_now_naive()
        set_updated_now(req)
        session.add(req)
        await session.commit()

    await log_action(callback.from_user.id, "admin", "request_completed", {"request_id": req_id, "auto": False})
    await notify_user(req.user_id, f"🏁 Вашу поставку #{req.id} завершено. Дякуємо!")
    await sheet_client.sync_request(req)
    await refresh_admin_card(callback, req_id)
    await callback.answer("Завершено")


@dp.callback_query(F.data.startswith("adm_change_"))
async def adm_change(callback: types.CallbackQuery, state: FSMContext):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)
    req_id = int(callback.data.split("_")[2])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
    if not req:
        return await callback.answer("Заявка не знайдена", show_alert=True)
    if req.completed_at:
        return await callback.answer("Заявку вже завершено.", show_alert=True)
    min_dt = min_planned_datetime()
    await state.set_state(AdminChangeForm.calendar)
    await state.update_data(req_id=req_id, min_plan_dt=min_dt.isoformat())
    await callback.message.answer(
        "📅 Оберіть нову дату:",
        reply_markup=build_calendar(min_dt.year, min_dt.month, prefix="admch", min_date=min_dt.date()),
    )
    await callback.answer()


@dp.callback_query(AdminChangeForm.calendar, F.data.startswith("admch_nav_"))
async def admch_nav(callback: types.CallbackQuery, state: FSMContext):
    _, _, y, m = callback.data.split("_")
    data = await state.get_data()
    min_date = get_min_date_from_state(data)
    await callback.message.edit_reply_markup(
        reply_markup=build_calendar(int(y), int(m), prefix="admch", min_date=min_date)
    )
    await callback.answer()


@dp.callback_query(AdminChangeForm.calendar, F.data.startswith("admch_day_"))
async def admch_day(callback: types.CallbackQuery, state: FSMContext):
    _, _, y, m, d = callback.data.split("_")
    chosen = date(int(y), int(m), int(d))
    data = await state.get_data()
    min_date = get_min_date_from_state(data)
    if min_date and chosen < min_date:
        return await callback.answer("Оберіть коректну дату.", show_alert=True)
    await state.update_data(new_date=chosen.isoformat())
    await state.set_state(AdminChangeForm.hour)
    await callback.message.answer("⏰ Оберіть годину:", reply_markup=build_hours_keyboard(prefix="admch_hour"))
    await callback.answer()


@dp.callback_query(AdminChangeForm.hour, F.data.startswith("admch_hour_"))
async def admch_hour(callback: types.CallbackQuery, state: FSMContext):
    hour = int(callback.data.split("_")[2])
    await state.update_data(hour=hour)
    await state.set_state(AdminChangeForm.minute)
    await callback.message.answer("⏰ Оберіть хвилини:", reply_markup=build_minutes_keyboard(prefix="admch_minute"))
    await callback.answer()


@dp.callback_query(AdminChangeForm.minute, F.data.startswith("admch_minute_"))
async def admch_minute(callback: types.CallbackQuery, state: FSMContext):
    minute = int(callback.data.split("_")[2])
    data = await state.get_data()
    new_date = date.fromisoformat(data["new_date"])
    hour = data["hour"]
    new_time = f"{hour:02d}:{minute:02d}"
    min_dt = get_min_datetime_from_state(data) or min_planned_datetime()
    chosen_dt = datetime.combine(new_date, dtime(hour=hour, minute=minute), tzinfo=KYIV_TZ)
    if chosen_dt < min_dt:
        return await callback.answer("Час не раніше ніж за годину від зараз.", show_alert=True)
    await state.update_data(new_time=new_time)
    await state.set_state(AdminChangeForm.reason)
    await callback.message.answer(
        "Вкажіть причину зміни дати/часу:", reply_markup=navigation_keyboard(include_back=False)
    )
    await callback.answer()


@dp.message(AdminChangeForm.reason)
async def admch_reason(message: types.Message, state: FSMContext):
    reason = message.text.strip()
    if not reason:
        return await message.answer("Причина не може бути порожньою.")
    data = await state.get_data()
    req_id = data["req_id"]
    new_date = date.fromisoformat(data["new_date"])
    new_time = data["new_time"]

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req:
            await state.clear()
            return await message.answer("Заявка не знайдена.")
        req.pending_date = new_date
        req.pending_time = new_time
        req.pending_reason = reason
        req.status = "pending_user_confirmation"
        req.admin_id = message.from_user.id
        set_updated_now(req)
        session.add(req)
        await session.commit()

    await log_action(
        message.from_user.id, "admin", "admin_change_time",
        {"request_id": req_id, "new_date": new_date.isoformat(), "new_time": new_time, "reason": reason},
    )

    await notify_user(
        req.user_id,
        f"🔁 Адміністратор пропонує новий час для заявки #{req.id}:\n"
        f"📅 {new_date.strftime('%d.%m.%Y')} ⏰ {new_time}\n"
        f"Причина: {reason}\n\nОберіть дію:",
        reply_markup=build_user_change_keyboard(req_id),
    )
    await sheet_client.sync_request(req)
    await message.answer(
        f"Пропозицію нового часу надіслано користувачу (заявка #{req_id}).",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await state.clear()


###############################################################
#     USER RESPONSE TO ADMIN CHANGE
###############################################################

@dp.callback_query(F.data.startswith("user_change_confirm_"))
async def user_change_confirm(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[3])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or req.user_id != callback.from_user.id:
            return await callback.answer("Заявка не знайдена", show_alert=True)
        if req.status not in {"pending_user_confirmation", "pending_user_final"}:
            return await callback.answer("Дію вже виконано.", show_alert=True)
        if req.pending_date and req.pending_time:
            req.date = req.pending_date
            req.time = req.pending_time
            req.planned_date = req.pending_date
            req.planned_time = req.pending_time
        req.status = "approved"
        req.pending_date = None
        req.pending_time = None
        req.pending_reason = None
        set_updated_now(req)
        session.add(req)
        await session.commit()

    await log_action(
        callback.from_user.id, "user", "admin_change_confirmed",
        {"request_id": req_id, "date": req.date.isoformat(), "time": req.time},
    )
    await notify_admins_about_action(req, "підтверджено користувачем")
    await sheet_client.sync_request(req)
    await callback.message.answer(
        f"✅ Ви підтвердили час для заявки #{req.id}:\n"
        f"📅 {req.date.strftime('%d.%m.%Y')} ⏰ {req.time}",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await callback.answer("Підтверджено")


@dp.callback_query(F.data.startswith("user_change_decline_"))
async def user_change_decline(callback: types.CallbackQuery, state: FSMContext):
    req_id = int(callback.data.split("_")[3])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
    if not req or req.user_id != callback.from_user.id:
        return await callback.answer("Заявка не знайдена", show_alert=True)
    await state.set_state(UserChangeResponse.decline_reason)
    await state.update_data(req_id=req_id)
    await callback.message.answer("Вкажіть причину відмови:", reply_markup=navigation_keyboard(include_back=False))
    await callback.answer()


@dp.message(UserChangeResponse.decline_reason)
async def user_change_decline_reason(message: types.Message, state: FSMContext):
    reason = message.text.strip()
    if not reason:
        return await message.answer("Причина не може бути порожньою.")
    data = await state.get_data()
    req_id = data["req_id"]
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req:
            await state.clear()
            return await message.answer("Заявка не знайдена.")
        req.status = "pending_admin_decision"
        req.pending_reason = merge_pending_reason(req.pending_reason, "Відмова користувача", reason)
        set_updated_now(req)
        session.add(req)
        await session.commit()

    await log_action(message.from_user.id, "user", "admin_change_declined",
                     {"request_id": req_id, "reason": reason})
    await notify_admins_about_action(req, "відхилено користувачем — потрібне рішення")
    await sheet_client.sync_request(req)
    await message.answer("Вашу відмову надіслано адміністратору.", reply_markup=navigation_keyboard(include_back=False))
    await state.clear()


@dp.callback_query(F.data.startswith("user_change_delete_"))
async def user_change_delete(callback: types.CallbackQuery, state: FSMContext):
    req_id = int(callback.data.split("_")[3])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
    if not req or req.user_id != callback.from_user.id:
        return await callback.answer("Заявка не знайдена", show_alert=True)
    await state.set_state(UserChangeResponse.delete_reason)
    await state.update_data(req_id=req_id)
    await callback.message.answer("Вкажіть причину скасування заявки:", reply_markup=navigation_keyboard(include_back=False))
    await callback.answer()


@dp.message(UserChangeResponse.delete_reason)
async def user_change_delete_reason(message: types.Message, state: FSMContext):
    reason = message.text.strip()
    if not reason:
        return await message.answer("Причина не може бути порожньою.")
    data = await state.get_data()
    req_id = data["req_id"]
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req:
            await state.clear()
            return await message.answer("Заявка не знайдена.")
        req_data = {
            "id": req.id, "supplier": req.supplier, "phone": req.phone,
            "vehicle_number": req.vehicle_number, "car": req.car,
            "loading_type": req.loading_type, "date": req.date, "time": req.time,
        }
        await session.delete(req)
        await session.commit()

    await log_action(message.from_user.id, "user", "admin_change_delete",
                     {"request_id": req_id, "reason": reason})
    await sheet_client.delete_request(req)
    await notify_admins_about_user_deletion(req_data, reason)
    await message.answer("Заявку скасовано.", reply_markup=navigation_keyboard(include_back=False))
    await state.clear()


@dp.callback_query(F.data.startswith("user_change_propose_"))
async def user_change_propose(callback: types.CallbackQuery, state: FSMContext):
    req_id = int(callback.data.split("_")[3])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
    if not req or req.user_id != callback.from_user.id:
        return await callback.answer("Заявка не знайдена", show_alert=True)
    min_dt = min_planned_datetime()
    await state.set_state(UserChangeResponse.calendar)
    await state.update_data(req_id=req_id, min_plan_dt=min_dt.isoformat())
    await callback.message.answer(
        "📅 Оберіть бажану дату:",
        reply_markup=build_calendar(min_dt.year, min_dt.month, prefix="uprop", min_date=min_dt.date()),
    )
    await callback.answer()


@dp.callback_query(UserChangeResponse.calendar, F.data.startswith("uprop_nav_"))
async def uprop_nav(callback: types.CallbackQuery, state: FSMContext):
    _, _, y, m = callback.data.split("_")
    data = await state.get_data()
    min_date = get_min_date_from_state(data)
    await callback.message.edit_reply_markup(
        reply_markup=build_calendar(int(y), int(m), prefix="uprop", min_date=min_date)
    )
    await callback.answer()

@dp.callback_query(UserChangeResponse.calendar, F.data.startswith("uprop_day_"))
async def uprop_day(callback: types.CallbackQuery, state: FSMContext):
    _, _, y, m, d = callback.data.split("_")
    chosen = date(int(y), int(m), int(d))
    data = await state.get_data()
    min_date = get_min_date_from_state(data)
    if min_date and chosen < min_date:
        return await callback.answer("Оберіть коректну дату.", show_alert=True)
    await state.update_data(new_date=chosen.isoformat())
    await state.set_state(UserChangeResponse.hour)
    await callback.message.answer("⏰ Оберіть годину:", reply_markup=build_hours_keyboard(prefix="uprop_hour"))
    await callback.answer()


@dp.callback_query(UserChangeResponse.hour, F.data.startswith("uprop_hour_"))
async def uprop_hour(callback: types.CallbackQuery, state: FSMContext):
    hour = int(callback.data.split("_")[2])
    await state.update_data(hour=hour)
    await state.set_state(UserChangeResponse.minute)
    await callback.message.answer("⏰ Оберіть хвилини:", reply_markup=build_minutes_keyboard(prefix="uprop_minute"))
    await callback.answer()


@dp.callback_query(UserChangeResponse.minute, F.data.startswith("uprop_minute_"))
async def uprop_minute(callback: types.CallbackQuery, state: FSMContext):
    minute = int(callback.data.split("_")[2])
    await state.update_data(minute=minute)
    await state.set_state(UserChangeResponse.propose_reason)
    await callback.message.answer(
        "Вкажіть коментар до вашої пропозиції (необов'язково, введіть «-» щоб пропустити):",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await callback.answer()


@dp.message(UserChangeResponse.propose_reason)
async def uprop_reason(message: types.Message, state: FSMContext):
    reason = message.text.strip()
    if reason == "-":
        reason = ""
    data = await state.get_data()
    req_id = data["req_id"]
    new_date = date.fromisoformat(data["new_date"])
    hour = data["hour"]
    minute = data["minute"]
    new_time = f"{hour:02d}:{minute:02d}"

    min_dt = get_min_datetime_from_state(data) or min_planned_datetime()
    chosen_dt = datetime.combine(new_date, dtime(hour=hour, minute=minute), tzinfo=KYIV_TZ)
    if chosen_dt < min_dt:
        return await message.answer("Час не раніше ніж за годину від зараз. Спробуйте ще раз.")

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req:
            await state.clear()
            return await message.answer("Заявка не знайдена.")
        req.pending_date = new_date
        req.pending_time = new_time
        req.pending_reason = merge_pending_reason(None, "Пропозиція користувача", reason) if reason else None
        req.status = "pending_admin_decision"
        set_updated_now(req)
        session.add(req)
        await session.commit()

    await log_action(
        message.from_user.id, "user", "user_propose_time",
        {"request_id": req_id, "date": new_date.isoformat(), "time": new_time, "reason": reason},
    )
    await notify_admins_about_action(req, "надіслав нову пропозицію часу — потрібне рішення")
    await sheet_client.sync_request(req)
    await message.answer(
        f"Вашу пропозицію ({new_date.strftime('%d.%m.%Y')} {new_time}) надіслано адміністратору.",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await state.clear()


###############################################################
#     ADMIN DECISION ON PENDING (user proposal / conflict)
###############################################################

@dp.callback_query(F.data.startswith("adm_accept_user_proposal_"))
async def adm_accept_user_proposal(callback: types.CallbackQuery):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)
    req_id = int(callback.data.split("_")[4])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if await guard_already_processed(callback, req, {"pending_admin_decision"}):
            return
        if req.pending_date and req.pending_time:
            req.date = req.pending_date
            req.time = req.pending_time
            req.planned_date = req.pending_date
            req.planned_time = req.pending_time
        req.status = "approved"
        req.admin_id = callback.from_user.id
        req.pending_date = None
        req.pending_time = None
        req.pending_reason = None
        set_updated_now(req)
        session.add(req)
        await session.commit()

    await log_action(callback.from_user.id, "admin", "accept_user_proposal", {"request_id": req_id})
    await notify_user(
        req.user_id,
        f"✅ Адміністратор прийняв ваш час для заявки #{req.id}:\n"
        f"📅 {req.date.strftime('%d.%m.%Y')} ⏰ {req.time}"
    )
    await sheet_client.sync_request(req)
    await refresh_admin_card(callback, req_id)
    await callback.answer("Час користувача прийнято")


@dp.callback_query(F.data.startswith("adm_reject_user_proposal_"))
async def adm_reject_user_proposal(callback: types.CallbackQuery, state: FSMContext):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)
    req_id = int(callback.data.split("_")[4])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
    if not req:
        return await callback.answer("Заявка не знайдена", show_alert=True)
    await state.set_state(AdminUserProposalReject.reason)
    await state.update_data(req_id=req_id)
    await callback.message.answer(
        "Вкажіть причину відмови від пропозиції користувача:",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await callback.answer()


@dp.message(AdminUserProposalReject.reason)
async def adm_reject_user_proposal_reason(message: types.Message, state: FSMContext):
    reason = message.text.strip()
    if not reason:
        return await message.answer("Причина не може бути порожньою.")
    data = await state.get_data()
    req_id = data["req_id"]
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req:
            await state.clear()
            return await message.answer("Заявка не знайдена.")
        req.status = "rejected"
        req.admin_id = message.from_user.id
        req.pending_date = None
        req.pending_time = None
        req.pending_reason = None
        set_updated_now(req)
        session.add(req)
        await session.commit()

    await log_action(message.from_user.id, "admin", "reject_user_proposal", {"request_id": req_id, "reason": reason})
    await notify_user(req.user_id, f"❌ Адміністратор відхилив вашу пропозицію (заявка #{req.id}).\nПричина: {reason}")
    await sheet_client.sync_request(req)
    await message.answer(f"Пропозицію користувача відхилено (заявка #{req_id}).", reply_markup=navigation_keyboard(include_back=False))
    await state.clear()


@dp.callback_query(F.data.startswith("adm_user_keep_client_"))
async def adm_user_keep_client(callback: types.CallbackQuery):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)
    req_id = int(callback.data.split("_")[4])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if await guard_already_processed(callback, req, {"pending_admin_decision"}):
            return
        req.status = "approved"
        req.admin_id = callback.from_user.id
        req.pending_reason = None
        set_updated_now(req)
        session.add(req)
        await session.commit()

    await log_action(callback.from_user.id, "admin", "keep_client_time", {"request_id": req_id})
    await notify_user(
        req.user_id,
        f"✅ Заявку #{req.id} підтверджено з вашим часом:\n"
        f"📅 {req.date.strftime('%d.%m.%Y')} ⏰ {req.time}"
    )
    await sheet_client.sync_request(req)
    await refresh_admin_card(callback, req_id)
    await callback.answer("Залишено час користувача")


@dp.callback_query(F.data.startswith("adm_user_keep_admin_"))
async def adm_user_keep_admin(callback: types.CallbackQuery):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)
    req_id = int(callback.data.split("_")[4])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if await guard_already_processed(callback, req, {"pending_admin_decision"}):
            return
        req.status = "pending_user_final"
        req.admin_id = callback.from_user.id
        set_updated_now(req)
        session.add(req)
        await session.commit()

    await log_action(callback.from_user.id, "admin", "keep_admin_time", {"request_id": req_id})
    await notify_user(
        req.user_id,
        f"🕒 Адміністратор наполягає на своєму часі для заявки #{req.id}:\n"
        f"📅 {(req.pending_date or req.planned_date).strftime('%d.%m.%Y')} "
        f"⏰ {req.pending_time or req.planned_time}\nПідтвердіть, будь ласка:",
        reply_markup=build_user_change_keyboard(req_id, limited=True),
    )
    await sheet_client.sync_request(req)
    await refresh_admin_card(callback, req_id)
    await callback.answer("Надіслано користувачу")


###############################################################
#            ADMIN: DELETE / CLEAR / MANAGE ADMINS
###############################################################

@dp.callback_query(F.data.startswith("adm_del_"))
async def adm_del(callback: types.CallbackQuery):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)
    req_id = int(callback.data.split("_")[2])
    is_super = await is_super_admin_user(callback.from_user.id)
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req:
            return await callback.answer("Заявка не знайдена", show_alert=True)
        if req.status == "new" and not is_super:
            return await callback.answer("Необроблену заявку може видалити лише суперадмін.", show_alert=True)
        await session.delete(req)
        await session.commit()

    await log_action(callback.from_user.id, "admin", "admin_delete_request", {"request_id": req_id})
    await sheet_client.delete_request(req)
    await callback.message.answer(f"🗑 Заявку #{req_id} видалено.", reply_markup=navigation_keyboard(include_back=False))
    await callback.answer("Видалено")


@dp.callback_query(F.data == "admin_add")
async def admin_add(callback: types.CallbackQuery, state: FSMContext):
    if not await is_super_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Лише для суперадміна.", show_alert=True)
    await state.set_state(AdminAdd.wait_id)
    await callback.message.answer("Введіть Telegram ID нового адміністратора:", reply_markup=navigation_keyboard(include_back=False))
    await callback.answer()


@dp.message(AdminAdd.wait_id)
async def admin_add_id(message: types.Message, state: FSMContext):
    raw = message.text.strip()
    if not raw.isdigit():
        return await message.answer("Введіть числовий ID.")
    await state.update_data(new_admin_id=int(raw))
    await state.set_state(AdminAdd.wait_last_name)
    await message.answer("Введіть прізвище адміністратора:", reply_markup=navigation_keyboard(include_back=False))


@dp.message(AdminAdd.wait_last_name)
async def admin_add_last_name(message: types.Message, state: FSMContext):
    last_name = message.text.strip()
    if not last_name:
        return await message.answer("Прізвище не може бути порожнім.")
    data = await state.get_data()
    new_id = data["new_admin_id"]
    async with SessionLocal() as session:
        exists = (await session.execute(select(Admin).where(Admin.telegram_id == new_id))).scalar_one_or_none()
        if exists:
            exists.last_name = last_name
        else:
            session.add(Admin(telegram_id=new_id, last_name=last_name, is_superadmin=False))
        await session.commit()

    await log_action(message.from_user.id, "admin", "admin_added", {"new_admin_id": new_id, "last_name": last_name})
    await message.answer(f"✅ Адміністратора {last_name} (ID {new_id}) додано.", reply_markup=navigation_keyboard(include_back=False))
    await state.clear()


@dp.callback_query(F.data == "admin_remove")
async def admin_remove(callback: types.CallbackQuery, state: FSMContext):
    if not await is_super_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Лише для суперадміна.", show_alert=True)
    await state.set_state(AdminRemove.wait_id)
    await callback.message.answer("Введіть Telegram ID адміністратора для видалення:", reply_markup=navigation_keyboard(include_back=False))
    await callback.answer()


@dp.message(AdminRemove.wait_id)
async def admin_remove_id(message: types.Message, state: FSMContext):
    raw = message.text.strip()
    if not raw.isdigit():
        return await message.answer("Введіть числовий ID.")
    admin_id = int(raw)
    if admin_id == SUPERADMIN_ID:
        return await message.answer("Неможливо видалити головного суперадміна.")
    async with SessionLocal() as session:
        admin = (await session.execute(select(Admin).where(Admin.telegram_id == admin_id))).scalar_one_or_none()
        if not admin:
            await state.clear()
            return await message.answer("Такого адміністратора не знайдено.")
        await session.delete(admin)
        await session.commit()

    await log_action(message.from_user.id, "admin", "admin_removed", {"removed_admin_id": admin_id})
    await message.answer(f"➖ Адміністратора ID {admin_id} видалено.", reply_markup=navigation_keyboard(include_back=False))
    await state.clear()


@dp.callback_query(F.data == "admin_clear")
async def admin_clear(callback: types.CallbackQuery):
    if not await is_super_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Лише для суперадміна.", show_alert=True)
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Так, очистити", callback_data="admin_clear_confirm")
    kb.button(text="❌ Скасувати", callback_data="go_main")
    kb.adjust(1)
    await callback.message.answer("⚠️ Видалити ВСІ заявки з БД та таблиці? Дію не можна скасувати.", reply_markup=kb.as_markup())
    await callback.answer()


@dp.callback_query(F.data == "admin_clear_confirm")
async def admin_clear_confirm(callback: types.CallbackQuery):
    if not await is_super_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Лише для суперадміна.", show_alert=True)
    async with SessionLocal() as session:
        await session.execute(delete(Request))
        await session.commit()
    await sheet_client.clear_requests()
    await log_action(callback.from_user.id, "admin", "db_cleared", {})
    await callback.message.answer("🗑 Усі заявки видалено.", reply_markup=navigation_keyboard(include_back=False))
    await callback.answer("Готово")


@dp.callback_query(F.data == "admin_delete_selected")
async def admin_delete_selected(callback: types.CallbackQuery, state: FSMContext):
    if not await is_super_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Лише для суперадміна.", show_alert=True)
    await state.set_state(AdminDeleteSelected.wait_ids)
    await callback.message.answer(
        "Введіть ID заявок через кому (напр.: 12, 15, 20):",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await callback.answer()


@dp.message(AdminDeleteSelected.wait_ids)
async def admin_delete_selected_ids(message: types.Message, state: FSMContext):
    ids = [int(x) for x in re.findall(r"\d+", message.text)]
    if not ids:
        return await message.answer("Не знайдено жодного ID.")
    deleted = []
    async with SessionLocal() as session:
        for rid in ids:
            req = await session.get(Request, rid)
            if req:
                await session.delete(req)
                deleted.append(rid)
                await sheet_client.delete_request(req)
        await session.commit()

    await log_action(message.from_user.id, "admin", "admin_delete_selected", {"ids": deleted})
    await message.answer(f"🗑 Видалено заявки: {', '.join(map(str, deleted)) or '—'}", reply_markup=navigation_keyboard(include_back=False))
    await state.clear()


###############################################################
#            ADMIN: SLOTS VIEW & LOGS EXPORT
###############################################################

@dp.callback_query(F.data == "admin_slots_view")
async def admin_slots_view(callback: types.CallbackQuery, state: FSMContext):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)
    now = kyiv_now()
    await state.set_state(AdminPlanView.calendar)
    await callback.message.answer(
        "📅 Оберіть дату для перегляду черги:",
        reply_markup=build_calendar(now.year, now.month, prefix="slotv", min_date=None),
    )
    await callback.answer()


@dp.callback_query(AdminPlanView.calendar, F.data.startswith("slotv_nav_"))
async def slotv_nav(callback: types.CallbackQuery):
    _, _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(
        reply_markup=build_calendar(int(y), int(m), prefix="slotv")
    )
    await callback.answer()


@dp.callback_query(AdminPlanView.calendar, F.data.startswith("slotv_day_"))
async def slotv_day(callback: types.CallbackQuery, state: FSMContext):
    _, _, y, m, d = callback.data.split("_")
    chosen = date(int(y), int(m), int(d))
    async with SessionLocal() as session:
        res = await session.execute(
            select(Request)
            .where(Request.status == "approved")
            .where(Request.date == chosen)
            .where(Request.completed_at.is_(None))
            .order_by(Request.time)
        )
        rows = res.scalars().all()

    if not rows:
        await state.clear()
        return await callback.message.answer(
            f"📅 {chosen.strftime('%d.%m.%Y')}: підтверджених слотів немає.",
            reply_markup=navigation_keyboard(include_back=False),
        )

    text = f"<b>📅 Черга на {chosen.strftime('%d.%m.%Y')}</b>\n\n"
    for r in rows:
        text += f"⏰ {r.time} — #{r.id} {r.supplier} ({r.car}, {r.vehicle_number or '—'})\n"
    await state.clear()
    await callback.message.answer(text, reply_markup=navigation_keyboard(include_back=False))
    await callback.answer()


@dp.callback_query(F.data == "admin_logs_export")
async def admin_logs_export(callback: types.CallbackQuery, state: FSMContext):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)
    await state.set_state(AdminLogsExport.start_date)
    await callback.message.answer(
        "Введіть дату початку (ДД.ММ.РРРР):",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await callback.answer()


def parse_date_input(raw: str) -> date | None:
    try:
        return datetime.strptime(raw.strip(), "%d.%m.%Y").date()
    except ValueError:
        return None


@dp.message(AdminLogsExport.start_date)
async def logs_start_date(message: types.Message, state: FSMContext):
    d = parse_date_input(message.text)
    if not d:
        return await message.answer("Невірний формат. Використайте ДД.ММ.РРРР.")
    await state.update_data(start_date=d.isoformat())
    await state.set_state(AdminLogsExport.end_date)
    await message.answer("Введіть дату завершення (ДД.ММ.РРРР):", reply_markup=navigation_keyboard(include_back=False))


@dp.message(AdminLogsExport.end_date)
async def logs_end_date(message: types.Message, state: FSMContext):
    d = parse_date_input(message.text)
    if not d:
        return await message.answer("Невірний формат. Використайте ДД.ММ.РРРР.")
    data = await state.get_data()
    start_date = date.fromisoformat(data["start_date"])
    end_date = d
    start_dt = datetime.combine(start_date, dtime.min)
    end_dt = datetime.combine(end_date, dtime.max)

    async with SessionLocal() as session:
        res = await session.execute(
            select(ActionLog)
            .where(ActionLog.created_at >= start_dt)
            .where(ActionLog.created_at <= end_dt)
            .order_by(ActionLog.id)
        )
        logs = res.scalars().all()

    if not logs:
        await state.clear()
        return await message.answer("За вказаний період логів немає.", reply_markup=navigation_keyboard(include_back=False))

    buffer = io.StringIO()
    writer = csv.writer(buffer)
        writer.writerow(["ID", "Дата/час", "Actor ID", "Роль", "Дія", "Деталі"])
    for lg in logs:
        writer.writerow([
            lg.id,
            lg.created_at.strftime("%d.%m.%Y %H:%M:%S") if lg.created_at else "",
            lg.actor_id,
            lg.actor_role,
            lg.action,
            lg.details or "",
        ])

    buffer.seek(0)
    file_bytes = buffer.getvalue().encode("utf-8-sig")
    file = BufferedInputFile(
        file_bytes,
        filename=f"logs_{start_date.isoformat()}_{end_date.isoformat()}.csv",
    )
    await message.answer_document(
        file,
        caption=f"📊 Логи за період {start_date.strftime('%d.%m.%Y')} — {end_date.strftime('%d.%m.%Y')}",
    )
    await log_action(
        message.from_user.id, "admin", "logs_exported",
        {"start": start_date.isoformat(), "end": end_date.isoformat(), "count": len(logs)},
    )
    await message.answer("Готово.", reply_markup=navigation_keyboard(include_back=False))
    await state.clear()


@dp.callback_query(F.data == "admin_list")
async def admin_list(callback: types.CallbackQuery):
    if not await is_super_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Лише для суперадміна.", show_alert=True)
    async with SessionLocal() as session:
        admins = (await session.execute(select(Admin).order_by(Admin.id))).scalars().all()
    text = "<b>👥 Список адміністраторів</b>\n\n"
    for a in admins:
        role = "⭐ Суперадмін" if (a.is_superadmin or a.telegram_id == SUPERADMIN_ID) else "Адмін"
        text += f"• {a.last_name or '—'} (ID {a.telegram_id}) — {role}\n"
    await callback.message.answer(text, reply_markup=navigation_keyboard(include_back=False))
    await callback.answer()


###############################################################
#                 AUTO-COMPLETE BACKGROUND TASK
###############################################################

async def auto_complete_task():
    """Автоматично завершує підтверджені поставки, час яких минув."""
    while True:
        try:
            now = kyiv_now_naive()
            async with SessionLocal() as session:
                res = await session.execute(
                    select(Request)
                    .where(Request.status == "approved")
                    .where(Request.completed_at.is_(None))
                )
                rows = res.scalars().all()
                for req in rows:
                    if not req.date or not req.time:
                        continue
                    try:
                        h, m = map(int, req.time.split(":"))
                    except ValueError:
                        continue
                    plan_dt = datetime.combine(req.date, dtime(hour=h, minute=m))
                    if now >= plan_dt + timedelta(hours=AUTO_COMPLETE_AFTER_HOURS):
                        req.completed_at = now
                        set_updated_now(req)
                        session.add(req)
                        await log_action(
                            0, "system", "request_auto_completed",
                            {"request_id": req.id, "auto": True},
                        )
                        await sheet_client.sync_request(req)
                        await notify_user(
                            req.user_id,
                            f"🏁 Поставку #{req.id} автоматично завершено (час минув)."
                        )
                await session.commit()
        except Exception as e:
            logging.exception("auto_complete_task error: %s", e)
        await asyncio.sleep(AUTO_COMPLETE_INTERVAL_SECONDS)


###############################################################
#                       FALLBACK HANDLER
###############################################################

@dp.message()
async def fallback_handler(message: types.Message, state: FSMContext):
    current = await state.get_state()
    if current:
        return  # активна форма опрацьовується власними хендлерами
    await message.answer(
        "Не зовсім зрозумів. Скористайтеся кнопками меню 👇",
        reply_markup=await main_menu(message.from_user.id),
    )


@dp.callback_query()
async def fallback_callback(callback: types.CallbackQuery):
    await callback.answer()


###############################################################
#                          STARTUP
###############################################################

async def on_startup():
    await init_db()
    await ensure_superadmin()
    await sheet_client.init()
    asyncio.create_task(auto_complete_task())
    logging.info("Bot started. Superadmin ID: %s", SUPERADMIN_ID)


async def ensure_superadmin():
    async with SessionLocal() as session:
        admin = (
            await session.execute(select(Admin).where(Admin.telegram_id == SUPERADMIN_ID))
        ).scalar_one_or_none()
        if not admin:
            session.add(Admin(telegram_id=SUPERADMIN_ID, last_name="Суперадмін", is_superadmin=True))
            await session.commit()
        elif not admin.is_superadmin:
            admin.is_superadmin = True
            await session.commit()


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    await on_startup()
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped.")
