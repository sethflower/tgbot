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


BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPERADMIN_ID = int(os.getenv("SUPERADMIN_ID"))
DATABASE_URL = os.getenv("DATABASE_URL")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID")

if not all([BOT_TOKEN, SUPERADMIN_ID, DATABASE_URL]):
    raise RuntimeError("❌ ENV-переменные BOT_TOKEN / SUPERADMIN_ID / DATABASE_URL не установлены!")

# Автоматическое исправление строки подключения
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
            if "pending_date" not in cols:
                sync_conn.execute(text("ALTER TABLE requests ADD COLUMN pending_date DATE"))
            if "pending_time" not in cols:
                sync_conn.execute(text("ALTER TABLE requests ADD COLUMN pending_time TEXT"))
            if "pending_reason" not in cols:
                sync_conn.execute(text("ALTER TABLE requests ADD COLUMN pending_reason TEXT"))

            # backfill plan and timestamps for existing rows
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

    def _build_row(self, req: Request, admin_name: str) -> list[str]:
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
        ]

    async def _update_row(self, row_number: int, values: list[str]) -> bool:
        try:
            await asyncio.to_thread(
                self._worksheet.update,
                f"A{row_number}:P{row_number}",
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

            # fallback: запитати кількість заповнених рядків
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
            logging.exception("Не вдалося додати заявку НП у Sheets: %s", exc)
            return False

    async def _find_row_by_request_id(self, req_id: int) -> int | None:
        """Find the sheet row for a request by its ID (column O)."""
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

    async def _get_row_number(self, req: Request) -> int | None:
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

    async def sync_request(self, req: Request):
        if not await self._ensure_client():
            return

        admin_name = await get_admin_display_name(req.admin_id)
        values = self._build_row(req, admin_name)

        if req.sheet_row:
            updated = await self._update_row(req.sheet_row, values)
            if updated:
                return

        row_number = await self._append_row(values)
        if row_number:
            await self._store_row_number(req.id, row_number)

    async def delete_request(self, req: Request):
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
            await asyncio.to_thread(self._worksheet.batch_clear, ["A2:P"])
        except Exception as exc:
            logging.exception("Не вдалося очистити таблицю Sheets: %s", exc)


sheet_client = GoogleSheetClient()


###############################################################
#                     CONSTANTS & MENUS
###############################################################

BACK_TEXT = "↩️ Назад"
MAIN_MENU_TEXT = "🏠 Головне меню"


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
    kb.button(text="📦 Доставка Новою поштою", callback_data="delivery_np")
    kb.adjust(1)
    return add_inline_navigation(kb).as_markup()

async def prompt_delivery_type(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "📦 Оберіть тип доставки для нової заявки:",
        reply_markup=delivery_type_keyboard(),
    )

def admin_menu(is_superadmin: bool = False):
    kb = InlineKeyboardBuilder()
    kb.button(text="🆕 Нові заявки", callback_data="admin_new")
    kb.button(text="📚 Усі заявки", callback_data="admin_all")
    kb.button(text="🔎 Пошук за ID", callback_data="admin_search")
    kb.button(text="📅 Посмотреть слоты очереди", callback_data="admin_slots_view")
    if is_superadmin:
        kb.button(text="➕ Додати адміна", callback_data="admin_add")
        kb.button(text="➖ Видалити адміна", callback_data="admin_remove")
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

class AdminChangeForm(StatesGroup):
    calendar = State()
    hour = State()
    minute = State()
    reason = State()

class AdminRejectForm(StatesGroup):
    reason = State()

class AdminPlanView(StatesGroup):
    calendar = State()

class UserDeleteForm(StatesGroup):
    user_id = State()
    reason = State()

class UserEditForm(StatesGroup):
    user_id = State()
    field_choice = State()
    supplier = State()
    phone = State()
    car = State()
    cargo_description = State()
    loading_type = State()
    calendar = State()     # выбор даты
    new_date = State()     # подтверждение даты
    hour = State()         # выбор часа
    minute = State()       # <-- ДОБАВИЛИ
    new_time = State()     # подтверждение времени
    reason = State()       # причина изменения

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
        "📦 Введіть назву постачальника:",
        reply_markup=navigation_keyboard(include_back=False)
    )

    await state.set_state(QueueForm.supplier)
    await callback.answer()


@dp.callback_query(F.data == "delivery_np")
async def delivery_np(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(NPDeliveryForm.supplier)

    await callback.message.answer(
        "✉️ Введіть назву постачальника для доставки Новою поштою:",
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


def format_request_text(req: Request) -> str:
    status = get_status_label(req.status)
    final_status = "Завершена" if req.completed_at else "Не завершена"
    planned_date = req.planned_date.strftime('%d.%m.%Y') if req.planned_date else req.date.strftime('%d.%m.%Y')
    planned_time = req.planned_time if req.planned_time else req.time
    return (
        f"<b>📄 Заявка #{req.id}</b>\n"
        f"Статус: {status}\n"
        "━━━━━━━━━━━━━━━━\n"
        f"🏢 <b>Постачальник:</b> {req.supplier}\n"
        f"📞 <b>Контакт:</b> {req.phone}\n"
        f"🚚 <b>Об'єм:</b> {req.car}\n"
        f"📦 <b>Товар:</b> {req.cargo_description or ''}\n"
        f"🧱 <b>Тип завантаження:</b> {req.loading_type}\n"
        f"📅 <b>План:</b> {planned_date} {planned_time}\n"
        f"✅ <b>Підтверджено:</b> {req.date.strftime('%d.%m.%Y')} {req.time}\n"
        f"🏁 <b>Статус завершення:</b> {final_status}"
    )


def build_recent_request_ids(reqs: list[Request]) -> set[int]:
    return {req.id for req in reqs}


def set_updated_now(req: Request):
    req.updated_at = kyiv_now_naive()

def get_user_modify_block_reason(req: Request) -> str | None:
    if req.status == "deleted_by_user":
        return "Заявка вже видалена"
    if req.completed_at:
        return "Заявка вже завершена, зміни неможливі"
    if req.status == "rejected":
        return "Заявка відхилена адміністратором, редагування неможливе"
    return None

def get_confirmed_datetime(req: Request) -> datetime | None:
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


def format_plan_datetime(req: Request) -> str:
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


async def send_request_details(
    req: Request,
    callback_or_message: types.CallbackQuery | types.Message,
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


async def get_user_recent_requests(user_id: int) -> list[Request]:
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
            req,
            callback,
            allow_actions=False,
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


async def notify_admins_about_user_deletion(req: Request | dict[str, Any], reason: str):
    async with SessionLocal() as session:
        admins = (await session.execute(select(Admin))).scalars().all()

    if isinstance(req, Request):
        data = {
            "id": req.id,
            "supplier": req.supplier,
            "phone": req.phone,
            "car": req.car,
            "loading_type": req.loading_type,
            "date": req.date,
            "time": req.time,
        }
    else:
        data = req

    text = (
        f"❗ Поставщик {data['supplier']} видалив заявку #{data['id']}\n"
        f"Причина: {reason}\n\n"
        f"📄 Дані заявки до видалення:\n"
        f"📞 {data['phone']}\n"
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
            "id": req.id,
            "supplier": req.supplier,
            "phone": req.phone,
            "car": req.car,
            "loading_type": req.loading_type,
            "date": req.date,
            "time": req.time,
        }

        await session.delete(req)
        await session.commit()

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
    message_or_callback: types.Message | types.CallbackQuery,
    state: FSMContext,
    req: Request,
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

    target = message_or_callback.message if isinstance(message_or_callback, types.CallbackQuery) else message_or_callback
    await target.answer(text, reply_markup=navigation_keyboard(include_back=False))
    await sheet_client.sync_request(req)
    await notify_admins_about_user_edit(req, reason, changes)
    await state.clear()
    if isinstance(message_or_callback, types.CallbackQuery):
        await message_or_callback.answer()


async def _load_request_for_edit(state: FSMContext, user_id: int) -> tuple[Request | None, str | None]:
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
        message,
        state,
        req,
        reason or "",
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
        message,
        state,
        req,
        reason or "",
        text=f"Поле 'Телефон' оновлено для заявки #{req.id}.",
        changes=[("Телефон", old_value, req.phone)],
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
        message,
        state,
        req,
        reason or "",
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
        message,
        state,
        req,
        reason or "",
        text=f"Поле 'Товар' оновлено для заявки #{req.id}.",
        changes=[("Товар", old_value, req.cargo_description)],
    )


@dp.callback_query(UserEditForm.loading_type, F.data == "edit_back_to_choice")
async def user_edit_loading_back(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(UserEditForm.field_choice)
    await callback.message.answer(
        "Оберіть, що потрібно змінити у заявці:",
        reply_markup=build_user_edit_choice_keyboard(),
    )
    await callback.answer()


@dp.callback_query(UserEditForm.loading_type)
async def user_edit_loading(callback: types.CallbackQuery, state: FSMContext):
    if callback.data not in {"edit_type_pal", "edit_type_loose"}:
        return await callback.answer("Невідомий варіант!", show_alert=True)

    new_value = "Палети" if callback.data == "edit_type_pal" else "Розсип"

    req, reason = await _load_request_for_edit(state, callback.from_user.id)
    if not req:
        await callback.answer("Заявка не знайдена", show_alert=True)
        return

    old_value = req.loading_type
    req.loading_type = new_value
    await finalize_user_edit_update(
        callback,
        state,
        req,
        reason or "",
        text=f"Тип завантаження оновлено для заявки #{req.id}.",
        changes=[("Тип завантаження", old_value, req.loading_type)],
    )


@dp.callback_query(UserEditForm.field_choice, F.data == "edit_cancel")
async def user_edit_cancel(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer(
        "Редагування скасовано.", reply_markup=navigation_keyboard(include_back=False)
    )
    await callback.answer()


@dp.callback_query(UserEditForm.field_choice, F.data.startswith("edit_field_"))
async def user_edit_field_choice(callback: types.CallbackQuery, state: FSMContext):
    choice = callback.data.replace("edit_field_", "")

    prompts = {
        "supplier": (UserEditForm.supplier, "Введіть нову назву постачальника:"),
        "phone": (UserEditForm.phone, "Введіть новий номер телефону:"),
        "car": (UserEditForm.car, "Введіть новий об'єм вантажу:"),
        "cargo_description": (
            UserEditForm.cargo_description,
            "Опишіть товар, який доставляється:",
        ),
    }

    if choice in prompts:
        next_state, text = prompts[choice]
        await state.set_state(next_state)
        await callback.message.answer(text, reply_markup=navigation_keyboard())
    elif choice == "loading":
        kb = InlineKeyboardBuilder()
        kb.button(text="🚚 На палетах", callback_data="edit_type_pal")
        kb.button(text="📦 В розсип", callback_data="edit_type_loose")
        kb.adjust(1)

        await state.set_state(UserEditForm.loading_type)
        await callback.message.answer(
            "Оберіть новий тип завантаження:",
            reply_markup=add_inline_navigation(kb, back_callback="edit_back_to_choice").as_markup(),
        )
    elif choice == "datetime":
        req, _ = await _load_request_for_edit(state, callback.from_user.id)
        if not req:
            return await callback.answer("Заявка не знайдена", show_alert=True)
        min_dt = min_planned_datetime(req.created_at)
        await state.update_data(min_plan_dt=min_dt.isoformat())
        await state.set_state(UserEditForm.calendar)
        await callback.message.answer(
            "Оберіть нову дату:",
            reply_markup=build_date_calendar(
                back_callback="edit_back_to_choice",
                hide_sundays=True,
                min_date=min_dt.date(),
            ),
        )
    else:
        await callback.message.answer("Невідомий вибір.")

    await callback.answer()

@dp.callback_query(UserEditForm.calendar, F.data.startswith("prev_"))
async def user_edit_prev(callback: types.CallbackQuery, state: FSMContext):
    _, y, m = callback.data.split("_")
    data = await state.get_data()
    min_date = get_min_date_from_state(data)
    await callback.message.edit_reply_markup(
        reply_markup=build_date_calendar(
            int(y),
            int(m),
            back_callback="edit_back_to_choice",
            hide_sundays=True,
            min_date=min_date,
        )
    )
    await callback.answer()


@dp.callback_query(UserEditForm.calendar, F.data.startswith("next_"))
async def user_edit_next(callback: types.CallbackQuery, state: FSMContext):
    _, y, m = callback.data.split("_")
    data = await state.get_data()
    min_date = get_min_date_from_state(data)
    await callback.message.edit_reply_markup(
        reply_markup=build_date_calendar(
            int(y),
            int(m),
            back_callback="edit_back_to_choice",
            hide_sundays=True,
            min_date=min_date,
        )
    )
    await callback.answer()


@dp.callback_query(UserEditForm.calendar, F.data == "close_calendar")
async def user_edit_cancel_calendar(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer(
        "Зміну заявки скасовано.", reply_markup=navigation_keyboard(include_back=False)
    )
    await callback.answer()


@dp.callback_query(UserEditForm.calendar, F.data == "edit_back_to_choice")
async def user_edit_back_to_choice(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(UserEditForm.field_choice)
    await callback.message.answer(
        "Оберіть, що потрібно змінити у заявці:",
        reply_markup=build_user_edit_choice_keyboard(),
    )
    await callback.answer()


@dp.callback_query(UserEditForm.calendar, F.data.startswith("day_"))
async def user_edit_day(callback: types.CallbackQuery, state: FSMContext):
    _, y, m, d = callback.data.split("_")
    chosen = date(int(y), int(m), int(d))
    data = await state.get_data()
    min_date = get_min_date_from_state(data)
    min_dt = get_min_datetime_from_state(data)

    if chosen < kyiv_now().date():
        return await callback.answer("Не можна обирати минулі дати", show_alert=True)

    if min_date and chosen < min_date:
        return await callback.answer(
            "Можна обрати час не раніше ніж через 1 годину після створення заявки.",
            show_alert=True,
        )

    if chosen.weekday() == 6:
        return await callback.answer(
            "Запис у неділю недоступний. Оберіть іншу дату.", show_alert=True
        )

    await state.update_data(new_date=chosen)

    kb = InlineKeyboardBuilder()
    hours = available_hours(chosen, earliest_dt=min_dt)
    for hour in hours:
        kb.button(text=f"{hour:02d}", callback_data=f"uhour_{hour:02d}")
    kb.adjust(6)

    if not hours:
        await callback.message.answer(
            "На цю дату немає доступних часових слотів. Оберіть іншу дату.",
            reply_markup=add_inline_navigation(
                InlineKeyboardBuilder(), back_callback="edit_back_to_calendar"
            ).as_markup(),
        )
        return await callback.answer()

    await state.set_state(UserEditForm.hour)
    await callback.message.answer(
        "⏰ Оберіть годину:",
        reply_markup=add_inline_navigation(kb, back_callback="edit_back_to_calendar").as_markup()
    )
    await callback.answer()


@dp.callback_query(UserEditForm.hour, F.data == "edit_back_to_calendar")
async def user_edit_back_to_calendar(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chosen_date: date | None = data.get("new_date")
    min_date = get_min_date_from_state(data)

    if chosen_date:
        markup = build_date_calendar(
            chosen_date.year,
            chosen_date.month,
            back_callback="edit_back_to_choice",
            hide_sundays=True,
            min_date=min_date,
        )
    else:
        markup = build_date_calendar(
            back_callback="edit_back_to_choice", hide_sundays=True, min_date=min_date
        )

    await state.set_state(UserEditForm.calendar)
    await callback.message.answer("Оберіть нову дату:", reply_markup=markup)
    await callback.answer()


@dp.callback_query(UserEditForm.hour, F.data.startswith("uhour_"))
async def user_edit_hour(callback: types.CallbackQuery, state: FSMContext):
    hour = callback.data.replace("uhour_", "")
    data = await state.get_data()
    chosen_date: date | None = data.get("new_date")
    min_dt = get_min_datetime_from_state(data)

    if not chosen_date:
        return await callback.answer("Оберіть дату", show_alert=True)

    valid_hours = {f"{h:02d}" for h in available_hours(chosen_date, earliest_dt=min_dt)}
    if hour not in valid_hours:
        return await callback.answer("Цей час вже недоступний", show_alert=True)

    await state.update_data(new_hour=hour)

    kb = InlineKeyboardBuilder()
    for m in available_minutes(chosen_date, int(hour), earliest_dt=min_dt):
        kb.button(text=f"{m:02d}", callback_data=f"umin_{m:02d}")
    kb.adjust(6)

    await state.set_state(UserEditForm.minute)
    await callback.message.answer(
        "🕒 Оберіть хвилини:",
        reply_markup=add_inline_navigation(kb, back_callback="edit_back_to_hour").as_markup()
    )
    await callback.answer()


@dp.callback_query(UserEditForm.minute, F.data == "edit_back_to_hour")
async def user_edit_back_to_hour(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chosen_date: date | None = data.get("new_date")
    min_dt = get_min_datetime_from_state(data)

    kb = InlineKeyboardBuilder()
    if chosen_date:
        hours = available_hours(chosen_date, earliest_dt=min_dt)
        for hour in hours:
            kb.button(text=f"{hour:02d}", callback_data=f"uhour_{hour:02d}")
    kb.adjust(6)

    await state.set_state(UserEditForm.hour)
    await callback.message.answer(
        "⏰ Оберіть годину:",
        reply_markup=add_inline_navigation(kb, back_callback="edit_back_to_calendar").as_markup()
    )
    await callback.answer()


@dp.callback_query(UserEditForm.minute, F.data.startswith("umin_"))
async def user_edit_minute(callback: types.CallbackQuery, state: FSMContext):
    minute = callback.data.replace("umin_", "")
    data = await state.get_data()

    chosen_date: date | None = data.get("new_date")
    chosen_hour = data.get("new_hour")
    min_dt = get_min_datetime_from_state(data)

    if min_dt:
        min_date = min_dt.date()
    else:
        min_date = None

    if not chosen_date or chosen_date < kyiv_now().date():
        return await callback.answer("Оберіть доступну дату", show_alert=True)

    if min_date and chosen_date < min_date:
        return await callback.answer(
            "Можна обрати час не раніше ніж через 1 годину після створення заявки.",
            show_alert=True,
        )

    if chosen_hour is None:
        return await callback.answer("Спочатку оберіть годину", show_alert=True)

    if int(minute) not in available_minutes(
        chosen_date, int(chosen_hour), earliest_dt=min_dt
    ):
        return await callback.answer("Цей час вже недоступний", show_alert=True)

    req, reason = await _load_request_for_edit(state, callback.from_user.id)
    if not req:
        return await callback.answer("Заявка не знайдена", show_alert=True)

    planned_dt = to_kyiv(
        datetime.combine(chosen_date, dtime(hour=int(chosen_hour), minute=int(minute)))
    )

    min_allowed = min_dt or min_planned_datetime(req.created_at)
    if planned_dt < min_allowed:
        return await callback.answer(
            "Можна обрати час не раніше ніж через 1 годину після створення заявки.",
            show_alert=True,
        )

    old_date = req.date
    old_time = req.time
    req.date = data.get("new_date")
    req.time = f"{data['new_hour']}:{minute}"
    req.planned_date = req.date
    req.planned_time = req.time

    await finalize_user_edit_update(
        callback,
        state,
        req,
        reason or "",
        text=(
            f"Запит на зміну заявки #{req.id} відправлено адміністратору.\n"
            f"📅 {req.date.strftime('%d.%m.%Y')} ⏰ {req.time}"
        ),
        changes=[(
            "Дата та час",
            f"{old_date.strftime('%d.%m.%Y')} {old_time}",
            f"{req.date.strftime('%d.%m.%Y')} {req.time}"
        )],
    )



###############################################################
#                     ADMIN PANEL ACCESS                      
###############################################################

@dp.callback_query(F.data == "menu_admin")
async def menu_admin_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    is_superadmin = await is_super_admin_user(user_id)

    if not is_superadmin:
        async with SessionLocal() as session:
            res = await session.execute(select(Admin).where(Admin.telegram_id == user_id))
            admin = res.scalar_one_or_none()

        if not admin:
            return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)

    await callback.message.answer(
        "🛠 <b>Адмін-панель</b>\nКеруйте заявками та доступами:",
        reply_markup=admin_menu(is_superadmin=is_superadmin),
    )


###############################################################
#                ADMIN — NEW REQUESTS LIST                    
###############################################################

@dp.callback_query(F.data == "admin_new")
async def admin_new(callback: types.CallbackQuery):

    async with SessionLocal() as session:
        res = await session.execute(
            select(Request)
            .where(Request.status == "new")
            .order_by(Request.id.desc())
        )
        rows = res.scalars().all()

    if not rows:
        return await callback.message.answer(
            "🟢 Нових заявок немає. Усі звернення оброблені."
        )

    text = "<b>🆕 Нові заявки</b>\nОстанні звернення, що очікують рішення:\n\n"
    for r in rows:
        text += (
            f"• <b>#{r.id}</b> — "
            f"{r.date.strftime('%d.%m.%Y')} {r.time}\n"
        )

    await callback.message.answer(text)


###############################################################
#            ADMIN — LIST ALL REQUESTS (last 20)              
###############################################################

@dp.callback_query(F.data == "admin_all")
async def admin_all(callback: types.CallbackQuery):

    async with SessionLocal() as session:
        res = await session.execute(
            select(Request)
            .order_by(Request.id.desc())
            .limit(20)
        )
        rows = res.scalars().all()

    if not rows:
        return await callback.message.answer("⚪ У базі ще немає заявок.")

    text = "<b>📚 Останні 20 заявок</b>\nШвидка навігація по архіву:\n\n"
    kb = InlineKeyboardBuilder()
    for r in rows:
        status = get_status_label(r.status)
        text += (
            f"• <b>#{r.id}</b>  "
            f"{r.supplier}  —  {r.date.strftime('%d.%m.%Y')} {r.time}  —  {status}\n"
        )
        kb.button(
            text=(
                f"#{r.id} — {r.supplier} — "
                f"{r.date.strftime('%d.%m.%Y')} {r.time} ({status})"
            ),
            callback_data=f"admin_view_{r.id}"
        )

    kb.button(text=MAIN_MENU_TEXT, callback_data="go_main")
    kb.adjust(1)

    await callback.message.answer(text, reply_markup=kb.as_markup())


async def render_slots_overview(target_date: date) -> str:
    async with SessionLocal() as session:
        res = await session.execute(
            select(Request)
            .where(
                Request.planned_date == target_date,
                ~Request.status.in_(["rejected", "deleted_by_user"]),
            )
        )
        requests_for_day = res.scalars().all()

    slots = all_slots_for_day(target_date)
    busy: dict[str, list[Request]] = {}
    for req in requests_for_day:
        slot_time = req.planned_time or req.time
        if not slot_time:
            continue
        busy.setdefault(slot_time, []).append(req)

    lines = [
        f"<b>📅 Слоти на {target_date.strftime('%d.%m.%Y')}</b>",
        "Слоти 09:00–16:00 з кроком 30 хвилин:",
        "",
    ]

    for slot in slots:
        requests_in_slot = busy.get(slot, [])
        if requests_in_slot:
            details = "; ".join(
                f"{r.supplier} (#{r.id}, {get_status_label(r.status)})"
                for r in requests_in_slot
            )
            lines.append(f"{slot} — 🚧 Зайнято: {details}")
        else:
            lines.append(f"{slot} — ✅ Вільно")

    return "\n".join(lines)


@dp.callback_query(F.data == "admin_slots_view")
async def admin_slots_view(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminPlanView.calendar)
    await callback.message.answer(
        "📅 Оберіть дату, щоб переглянути доступність слотів:",
        reply_markup=build_date_calendar(back_callback="menu_admin"),
    )
    await callback.answer()


@dp.callback_query(AdminPlanView.calendar, F.data.startswith("prev_"))
async def admin_slots_prev(callback: types.CallbackQuery):
    _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(
        reply_markup=build_date_calendar(int(y), int(m), back_callback="menu_admin")
    )


@dp.callback_query(AdminPlanView.calendar, F.data.startswith("next_"))
async def admin_slots_next(callback: types.CallbackQuery):
    _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(
        reply_markup=build_date_calendar(int(y), int(m), back_callback="menu_admin")
    )


@dp.callback_query(AdminPlanView.calendar, F.data == "close_calendar")
async def admin_slots_close(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("❌ Перегляд слотів скасовано.")
    await callback.answer()


@dp.callback_query(AdminPlanView.calendar, F.data.startswith("day_"))
async def admin_slots_for_day(callback: types.CallbackQuery, state: FSMContext):
    _, y, m, d = callback.data.split("_")
    chosen_date = date(int(y), int(m), int(d))

    overview = await render_slots_overview(chosen_date)

    kb = InlineKeyboardBuilder()
    kb.button(text="📅 Обрати іншу дату", callback_data="admin_slots_choose_date")
    kb = add_inline_navigation(kb, back_callback="menu_admin")

    await callback.message.answer(overview, reply_markup=kb.as_markup())
    await callback.answer()


@dp.callback_query(F.data == "admin_slots_choose_date")
async def admin_slots_choose_date(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminPlanView.calendar)
    await callback.message.answer(
        "📅 Оберіть дату, щоб переглянути доступність слотів:",
        reply_markup=build_date_calendar(back_callback="menu_admin"),
    )
    await callback.answer()


def build_admin_request_view(req: Request, is_superadmin: bool):
    status = get_status_label(req.status)
    final_status = "Завершена" if req.completed_at else "Не завершена"
    plan_date = req.planned_date.strftime('%d.%m.%Y') if req.planned_date else req.date.strftime('%d.%m.%Y')
    plan_time = req.planned_time if req.planned_time else req.time
    text = (
        f"<b>📄 Заявка #{req.id}</b>\n"
        f"Статус: {status}\n\n"
        f"🏢 <b>Постачальник:</b> {req.supplier}\n"
        f"📞 <b>Телефон:</b> {req.phone}\n"
        f"🚚 <b>Об'єм:</b> {req.car}\n"
        f"📦 <b>Товар:</b> {req.cargo_description or ''}\n"
        f"🧱 <b>Тип завантаження:</b> {req.loading_type}\n"
        f"📅 <b>План:</b> {plan_date} {plan_time}\n"
        f"✅ <b>Підтверджено:</b> {req.date.strftime('%d.%m.%Y')} {req.time}\n"
        f"🏁 <b>Завершення:</b> {final_status}"
    )
    if req.pending_date and req.pending_time:
        text += (
            f"\n📝 <b>Пропозиція користувача:</b> "
            f"{req.pending_date.strftime('%d.%m.%Y')} {req.pending_time}"
        )
    if req.pending_reason:
        text += f"\nℹ️ <b>Коментар:</b> {req.pending_reason}"
    kb = InlineKeyboardBuilder()
    kb.button(text="✔ Підтвердити", callback_data=f"adm_ok_{req.id}")
    kb.button(text="🔁 Змінити дату/час", callback_data=f"adm_change_{req.id}")
    kb.button(text="❌ Відхилити", callback_data=f"adm_rej_{req.id}")
    if req.status == "approved" and not req.completed_at:
        kb.button(text="🏁 Завершити поставку", callback_data=f"adm_finish_{req.id}")
    if is_superadmin or req.status != "new":
        kb.button(text="🗑 Видалити", callback_data=f"adm_del_{req.id}")
    kb.button(text="⬅️ До списку", callback_data="admin_all")
    kb.adjust(1)
    kb = add_inline_navigation(kb)
    return text, kb.as_markup()


@dp.callback_query(F.data.startswith("admin_view_"))
async def admin_view(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        admin = (
            await session.execute(
                select(Admin).where(Admin.telegram_id == user_id)
            )
        ).scalar_one_or_none()

    if not req:
        return await callback.answer("Заявка не знайдена", show_alert=True)

    is_superadmin = user_id == SUPERADMIN_ID or (admin and admin.is_superadmin)

    if not (is_superadmin or admin):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)

    text, markup = build_admin_request_view(req, is_superadmin)

    await callback.message.answer(text, reply_markup=markup)

    await callback.answer()


@dp.callback_query(F.data == "admin_search")
async def admin_search_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "Введіть ID заявки для пошуку:",
        reply_markup=navigation_keyboard(),
    )
    await state.set_state(AdminSearch.wait_id)
    await callback.answer()


@dp.message(AdminSearch.wait_id)
async def admin_search_wait(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.clear()
        await message.answer("Пошук скасовано.", reply_markup=navigation_keyboard(include_back=False))
        return await show_main_menu(message, state)

    try:
        req_id = int(message.text.strip())
    except ValueError:
        return await message.answer("Будь ласка, введіть числовий ID заявки.")

    user_id = message.from_user.id
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        admin = (
            await session.execute(
                select(Admin).where(Admin.telegram_id == user_id)
            )
        ).scalar_one_or_none()

    is_superadmin = user_id == SUPERADMIN_ID or (admin and admin.is_superadmin)

    if not (is_superadmin or admin):
        await state.clear()
        return await message.answer("⛔ Ви не адміністратор.")

    if not req:
        return await message.answer("Заявка не знайдена.")

    text, markup = build_admin_request_view(req, is_superadmin)

    await message.answer(text, reply_markup=markup)

    await state.clear()
###############################################################
#             ADMIN — ADD ADMIN (FSM Aiogram 3 OK)            
###############################################################

@dp.callback_query(F.data == "admin_add")
async def admin_add(callback: types.CallbackQuery, state: FSMContext):
    if not await is_super_admin_user(callback.from_user.id):
        return await callback.answer(
            "⛔ Тільки суперадмін може керувати адміністраторами.",
            show_alert=True,
        )

    await callback.message.answer(
        "➕ Введіть Telegram ID користувача:",
        reply_markup=navigation_keyboard()
    )
    await state.set_state(AdminAdd.wait_id)


@dp.message(AdminAdd.wait_id)
async def admin_add_wait(message: types.Message, state: FSMContext):
    if not await is_super_admin_user(message.from_user.id):
        await state.clear()
        return await message.answer(
            "⛔ Тільки суперадмін може керувати адміністраторами.",
            reply_markup=navigation_keyboard(include_back=False),
        )

    if message.text == BACK_TEXT:
        await state.clear()
        await message.answer("Скасовано.", reply_markup=navigation_keyboard(include_back=False))
        return await show_main_menu(message, state)


    try:
        tg_id = int(message.text)
    except:
        return await message.answer("❌ ID має бути числовим.")

    async with SessionLocal() as session:
        exists = await session.execute(select(Admin).where(Admin.telegram_id == tg_id))
        if exists.scalar_one_or_none():
            await state.clear()
            return await message.answer("⚠️ Цей користувач вже є адміністратором.")

    await state.update_data(new_admin_id=tg_id)
    await state.set_state(AdminAdd.wait_last_name)
    await message.answer(
        "✏️ Введіть прізвище адміністратора:",
        reply_markup=navigation_keyboard(),
    )


@dp.message(AdminAdd.wait_last_name)
async def admin_add_wait_last_name(message: types.Message, state: FSMContext):
    if not await is_super_admin_user(message.from_user.id):
        await state.clear()
        return await message.answer(
            "⛔ Тільки суперадмін може керувати адміністраторами.",
            reply_markup=navigation_keyboard(include_back=False),
        )

    if message.text == BACK_TEXT:
        await state.set_state(AdminAdd.wait_id)
        return await message.answer(
            "➕ Введіть Telegram ID користувача:",
            reply_markup=navigation_keyboard(),
        )

    last_name = message.text.strip()
    if not last_name:
        return await message.answer("❌ Прізвище не може бути порожнім.")

    data = await state.get_data()
    tg_id = data.get("new_admin_id")

    if not tg_id:
        await state.clear()
        return await message.answer(
            "Сталася помилка. Спробуйте додати адміністратора ще раз.",
            reply_markup=navigation_keyboard(include_back=False),
        )

    async with SessionLocal() as session:
        exists = await session.execute(select(Admin).where(Admin.telegram_id == tg_id))
        if exists.scalar_one_or_none():
            await state.clear()
            return await message.answer("⚠️ Цей користувач вже є адміністратором.")

        admin_last_name = "Админ" if tg_id == SUPERADMIN_ID else last_name
        session.add(
            Admin(
                telegram_id=tg_id,
                last_name=admin_last_name,
                is_superadmin=tg_id == SUPERADMIN_ID,
            )
        )
        await session.commit()

    await state.clear()
    await message.answer(
        f"✔ Адміністратор <b>{admin_last_name}</b> доданий.",
        reply_markup=navigation_keyboard(include_back=False),
    )


###############################################################
#           ADMIN — REMOVE ADMIN (FSM Aiogram 3 OK)           
###############################################################

@dp.callback_query(F.data == "admin_remove")
async def admin_remove(callback: types.CallbackQuery, state: FSMContext):
    if not await is_super_admin_user(callback.from_user.id):
        return await callback.answer(
            "⛔ Тільки суперадмін може керувати адміністраторами.",
            show_alert=True,
        )

    await callback.message.answer(
        "➖ Введіть Telegram ID адміністратора для видалення:",
        reply_markup=navigation_keyboard()
    )
    await state.set_state(AdminRemove.wait_id)


@dp.message(AdminRemove.wait_id)
async def admin_remove_wait(message: types.Message, state: FSMContext):
    if not await is_super_admin_user(message.from_user.id):
        await state.clear()
        return await message.answer(
            "⛔ Тільки суперадмін може керувати адміністраторами.",
            reply_markup=navigation_keyboard(include_back=False),
        )

    if message.text == BACK_TEXT:
        await state.clear()
        await message.answer("Скасовано.", reply_markup=navigation_keyboard(include_back=False))
        return await show_main_menu(message, state)

    try:
        tg_id = int(message.text)
    except:
        return await message.answer("❌ ID має бути числовим.")

    async with SessionLocal() as session:
        admin = (
            await session.execute(
                select(Admin).where(Admin.telegram_id == tg_id)
            )
        ).scalar_one_or_none()

        if not admin:
            await state.clear()
            return await message.answer(
                "Адміністратор з таким ID не знайдений.",
                reply_markup=navigation_keyboard(include_back=False),
            )

        if admin.is_superadmin:
            await state.clear()
            return await message.answer(
                "Суперадміністратора не можна видалити.",
                reply_markup=navigation_keyboard(include_back=False),
            )

        admin_name = "Админ" if admin.telegram_id == SUPERADMIN_ID else (admin.last_name or str(admin.telegram_id))
        await session.delete(admin)
        await session.commit()

    await state.clear()
    await message.answer(
        f"🗑 Адміністратора <b>{admin_name}</b> видалено.",
        reply_markup=navigation_keyboard(include_back=False)
    )


###############################################################
#                ADMIN — CLEAR DATABASE                      
###############################################################

@dp.callback_query(F.data == "admin_clear")
async def admin_clear(callback: types.CallbackQuery):

    if not await is_super_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Тільки суперадмін!", show_alert=True)

    kb = InlineKeyboardBuilder()
    kb.button(text="🗑 Видалити всі заявки", callback_data="admin_clear_yes")
    kb.button(text="❌ Скасувати", callback_data="admin_clear_no")
    kb.adjust(1)
    kb = add_inline_navigation(kb)

    await callback.message.answer(
        "⚠️ Ви впевнені, що хочете видалити всі заявки?",
        reply_markup=kb.as_markup()
    )


@dp.callback_query(F.data == "admin_clear_yes")
async def admin_clear_yes(callback: types.CallbackQuery):
    if not await is_super_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Тільки суперадмін!", show_alert=True)

    async with SessionLocal() as session:
        await session.execute(delete(Request))
        await session.commit()

    await sheet_client.clear_requests()

    await callback.message.answer("🗑 Усі заявки видалено!")


@dp.callback_query(F.data == "admin_clear_no")
async def admin_clear_no(callback: types.CallbackQuery):
    await callback.message.answer("Операцію скасовано.")

###############################################################
#               NOVA POSHTA DELIVERY (SIMPLE FORM)
###############################################################

@dp.message(NPDeliveryForm.supplier)
async def np_supplier_step(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        return await prompt_delivery_type(message, state)

    supplier = message.text.strip()
    if not supplier:
        return await message.answer("⚠️ Вкажіть назву постачальника, щоб продовжити.")

    await state.update_data(supplier=supplier)
    await state.set_state(NPDeliveryForm.ttn)
    await message.answer(
        "✉️ Введіть номер ТТН Нової пошти:",
        reply_markup=navigation_keyboard(),
    )


@dp.message(NPDeliveryForm.ttn)
async def np_ttn_step(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(NPDeliveryForm.supplier)
        return await message.answer(
            "✉️ Введіть назву постачальника для доставки Новою поштою:",
            reply_markup=navigation_keyboard(),
        )

    ttn = message.text.strip()
    if not ttn:
        return await message.answer("⚠️ Введіть номер ТТН, щоб завершити заявку.")

    data = await state.get_data()
    supplier = data.get("supplier", "")

    saved = await sheet_client.append_np_delivery(supplier, ttn)
    await notify_admins_np_delivery(supplier, ttn)

    if saved:
        await message.answer(
            "✅ Заявка на доставку Новою поштою зафіксована. Адміністратори отримали повідомлення.",
            reply_markup=navigation_keyboard(include_back=False),
        )
    else:
        await message.answer(
            "⚠️ Адміністратори сповіщені, але не вдалося записати заявку у Google Sheets.",
            reply_markup=navigation_keyboard(include_back=False),
        )

    await state.clear()


###############################################################
#               DRIVER FORM — INPUT STEPS                     
###############################################################

@dp.message(QueueForm.supplier)
async def step_supplier(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        return await message.answer(
            "ℹ️ Ви на початку анкети. Використовуйте кнопки навігації."
        )

    supplier = message.text.strip()

    if not supplier:
        return await message.answer("⚠️ Вкажіть назву постачальника, щоб продовжити.")

    await state.update_data(supplier=supplier)

    await message.answer(
        "📞 <b>Крок 2/6</b>\nЗалиште контактний номер телефону:",
        reply_markup=navigation_keyboard()
    )
    await state.set_state(QueueForm.phone)


@dp.message(QueueForm.phone)
async def step_phone(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.supplier)
        return await message.answer(
            "🏢 <b>Крок 1/6</b>\nВкажіть назву постачальника:",
            reply_markup=navigation_keyboard(include_back=False)
        )

    phone = message.text.strip()
    if not phone:
        return await message.answer("⚠️ Вкажіть номер телефону для зв'язку.")

    await state.update_data(phone=phone)

    await message.answer(
        "🚚 <b>Крок 3/6</b>\nВкажіть об'єм вантажу:",
        reply_markup=navigation_keyboard()
    )
    await state.set_state(QueueForm.car)


@dp.message(QueueForm.car)
async def step_car(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.phone)
        return await message.answer(
            "📞 <b>Крок 2/6</b>\nЗалиште контактний номер телефону:",
            reply_markup=navigation_keyboard(),
        )

    car = message.text.strip()
    if not car:
        return await message.answer("⚠️ Вкажіть об'єм вантажу.")

    await state.update_data(car=car)

    await message.answer(
        "📦 <b>Крок 4/6</b>\nВкажіть товар, який доставляється:",
        reply_markup=navigation_keyboard(),
    )

    await state.set_state(QueueForm.cargo_description)


@dp.message(QueueForm.cargo_description)
async def step_cargo_description(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.car)
        return await message.answer(
            "🚚 <b>Крок 3/6</b>\nВкажіть об'єм вантажу:",
            reply_markup=navigation_keyboard(),
        )

    cargo_description = message.text.strip()
    if not cargo_description:
        return await message.answer("⚠️ Опишіть товар, який доставляється.")

    await state.update_data(cargo_description=cargo_description)

    kb = InlineKeyboardBuilder()
    kb.button(text="🚚 На палетах", callback_data="type_pal")
    kb.button(text="📦 В розсип", callback_data="type_loose")
    kb.adjust(1)

    await message.answer(
        "⚙️ <b>Крок 5/6</b>\nОберіть тип завантаження:",
        reply_markup=add_inline_navigation(kb, back_callback="back_to_cargo").as_markup(),
    )

    await state.set_state(QueueForm.loading_type)


@dp.callback_query(QueueForm.loading_type, F.data == "back_to_cargo")
async def loading_back(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(QueueForm.cargo_description)
    await callback.message.answer(
        "📦 <b>Крок 4/6</b>\nВкажіть товар, який доставляється:",
        reply_markup=navigation_keyboard(),
    )
    await callback.answer()

###############################################################
#                 LOADING TYPE → DATE
###############################################################

@dp.callback_query(QueueForm.loading_type)
async def step_loading(callback: types.CallbackQuery, state: FSMContext):
    if callback.data == "type_pal":
        t = "Палети"
    elif callback.data == "type_loose":
        t = "Розсип"
    else:
        return await callback.answer("Невідомий варіант!")

    min_dt = min_planned_datetime()
    await state.update_data(loading_type=t, min_plan_dt=min_dt.isoformat())

    await callback.message.answer(
        "📅 <b>Крок 6/6</b>\nОберіть дату та час візиту:",
        reply_markup=build_date_calendar(
            back_callback="back_to_loading", hide_sundays=True, min_date=min_dt.date()
        )
    )

    await state.set_state(QueueForm.calendar)


###############################################################
#                INLINE CALENDAR GENERATOR                    
###############################################################

def build_date_calendar(
    year=None,
    month=None,
    back_callback: str | None = None,
    *,
    hide_sundays: bool = False,
    min_date: date | None = None,
):
    now = kyiv_now()
    today = min_date or now.date()
    year = year or today.year
    month = month or today.month

    current_month_start = date(today.year, today.month, 1)
    requested_month_start = date(year, month, 1)
    if requested_month_start < current_month_start:
        year, month = current_month_start.year, current_month_start.month

    kb = InlineKeyboardBuilder()

    # Заголовок месяца
    month_name = datetime(year, month, 1).strftime("%B %Y")
    kb.row(InlineKeyboardButton(text=f"📅 {month_name}", callback_data="ignore"))

    # День недели, с которого начинается месяц
    first_wday = datetime(year, month, 1, tzinfo=KYIV_TZ).weekday()  # Monday = 0
    if hide_sundays and first_wday == 6:
        first_wday = 0

    row = []
    for _ in range(first_wday):
        row.append(InlineKeyboardButton(text=" ", callback_data="ignore"))
    if row:
        kb.row(*row)

    # Количество дней
    next_month = month + 1 if month < 12 else 1
    next_year = year + 1 if month == 12 else year
    days_in_month = (datetime(next_year, next_month, 1, tzinfo=KYIV_TZ) - timedelta(days=1)).day

    row = []
    for d in range(1, days_in_month + 1):
        day_date = date(year, month, d)
        if day_date < today:
            # Пропускаємо минулі дати, щоб користувач їх не бачив
            continue

        if hide_sundays and day_date.weekday() == 6:
            if row:
                kb.row(*row)
                row = []
            continue

        row.append(
            InlineKeyboardButton(
                text=str(d), callback_data=f"day_{year}_{month}_{d}"
            )
        )
        if len(row) == 7:
            kb.row(*row)
            row = []
    if row:
        kb.row(*row)

    # Навигация
    prev_m = month - 1 or 12
    prev_y = year - 1 if month == 1 else year

    next_m = next_month
    next_y = next_year

    prev_month_last_day = date(prev_y, prev_m, (datetime(year, month, 1, tzinfo=KYIV_TZ) - timedelta(days=1)).day)
    prev_cb = f"prev_{prev_y}_{prev_m}" if prev_month_last_day >= today else "ignore"

    kb.row(
        InlineKeyboardButton(text="⬅", callback_data=prev_cb),
        InlineKeyboardButton(text="Закрити", callback_data="close_calendar"),
        InlineKeyboardButton(text="➡", callback_data=f"next_{next_y}_{next_m}")
    )

    nav_row = [InlineKeyboardButton(text=MAIN_MENU_TEXT, callback_data="go_main")]
    if back_callback:
        nav_row.append(InlineKeyboardButton(text=BACK_TEXT, callback_data=back_callback))
    kb.row(*nav_row)

    return kb.as_markup()


def available_minutes(
    selected_date: date,
    hour: int,
    *,
    now_dt: datetime | None = None,
    earliest_dt: datetime | None = None,
) -> list[int]:
    now_dt = now_dt or kyiv_now()
    earliest_dt = to_kyiv(earliest_dt) if earliest_dt else None

    if hour < 9 or hour > 16:
        return []

    # Години роботи: 09:00–16:00 з кроком 30 хвилин (00 та 30)
    minutes = [0, 30]

    if earliest_dt and selected_date == earliest_dt.date():
        if hour < earliest_dt.hour:
            return []
        if hour == earliest_dt.hour:
            minutes = [m for m in minutes if m >= earliest_dt.minute]

    if selected_date == now_dt.date():
        current_time = now_dt.time()
        if hour < current_time.hour:
            return []
        if hour == current_time.hour:
            minutes = [m for m in minutes if m >= current_time.minute]

    return minutes


def available_hours(
    selected_date: date,
    *,
    now_dt: datetime | None = None,
    earliest_dt: datetime | None = None,
) -> list[int]:
    now_dt = now_dt or kyiv_now()
    earliest_dt = to_kyiv(earliest_dt) if earliest_dt else None
    hours = []

    for hour in range(9, 17):
        minutes = available_minutes(
            selected_date, hour, now_dt=now_dt, earliest_dt=earliest_dt
        )
        if minutes:
            hours.append(hour)

    return hours


def all_slots_for_day(selected_date: date) -> list[str]:
    slots: list[str] = []
    for hour in range(9, 17):
        minutes = [0, 30]
        for minute in minutes:
            slots.append(f"{hour:02d}:{minute:02d}")
    return slots


###############################################################
#        DRIVER — DATE / HOUR / MINUTE SELECTION
###############################################################

@dp.callback_query(QueueForm.calendar, F.data.startswith("prev_"))
async def cal_prev(callback: types.CallbackQuery, state: FSMContext):
    _, y, m = callback.data.split("_")
    data = await state.get_data()
    min_date = get_min_date_from_state(data)
    await callback.message.edit_reply_markup(
        reply_markup=build_date_calendar(
            int(y),
            int(m),
            back_callback="back_to_loading",
            hide_sundays=True,
            min_date=min_date,
        )
    )


@dp.callback_query(QueueForm.calendar, F.data.startswith("next_"))
async def cal_next(callback: types.CallbackQuery, state: FSMContext):
    _, y, m = callback.data.split("_")
    data = await state.get_data()
    min_date = get_min_date_from_state(data)
    await callback.message.edit_reply_markup(
        reply_markup=build_date_calendar(
            int(y),
            int(m),
            back_callback="back_to_loading",
            hide_sundays=True,
            min_date=min_date,
        )
    )

@dp.callback_query(QueueForm.calendar, F.data == "back_to_loading")
async def cal_back_to_loading(callback: types.CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder()
    kb.button(text="🚚 На палетах", callback_data="type_pal")
    kb.button(text="📦 В розсип", callback_data="type_loose")
    kb.adjust(1)

    await state.set_state(QueueForm.loading_type)
    await callback.message.answer(
        "🔹 Оберіть тип завантаження:",
        reply_markup=add_inline_navigation(kb, back_callback="back_to_cargo").as_markup()
    )
    await callback.answer()

@dp.callback_query(QueueForm.calendar, F.data.startswith("day_"))
async def cal_day(callback: types.CallbackQuery, state: FSMContext):
    _, y, m, d = callback.data.split("_")
    chosen = date(int(y), int(m), int(d))
    data = await state.get_data()
    min_date = get_min_date_from_state(data)
    min_dt = get_min_datetime_from_state(data)

    if chosen < kyiv_now().date():
        return await callback.answer("Не можна обирати минулі дати", show_alert=True)

    if min_date and chosen < min_date:
        return await callback.answer(
            "Можна обрати час не раніше ніж через 1 годину після створення заявки.",
            show_alert=True,
        )

    if chosen.weekday() == 6:
        return await callback.answer(
            "Запис у неділю недоступний. Оберіть іншу дату.", show_alert=True
        )

    await state.update_data(date=chosen)

    kb = InlineKeyboardBuilder()
    hours = available_hours(chosen, earliest_dt=min_dt)
    for hour in hours:
        kb.button(text=f"{hour:02d}", callback_data=f"hour_{hour:02d}")
    kb.adjust(6)

    if not hours:
        await callback.message.answer(
            "На цю дату немає доступних часових слотів. Оберіть іншу дату.",
            reply_markup=add_inline_navigation(
                InlineKeyboardBuilder(), back_callback="back_to_calendar"
            ).as_markup(),
        )
        return await callback.answer()

    await callback.message.answer(
        "⏰ Оберіть годину:",
        reply_markup=add_inline_navigation(kb, back_callback="back_to_calendar").as_markup()
    )
    await state.set_state(QueueForm.hour)


@dp.callback_query(QueueForm.calendar, F.data == "close_calendar")
async def close_calendar(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("❌ Вибір дати скасовано.")

@dp.callback_query(QueueForm.hour, F.data == "back_to_calendar")
async def back_to_calendar(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chosen_date: date | None = data.get("date")
    min_date = get_min_date_from_state(data)

    if chosen_date:
        markup = build_date_calendar(
            chosen_date.year,
            chosen_date.month,
            back_callback="back_to_loading",
            hide_sundays=True,
            min_date=min_date,
        )
    else:
        markup = build_date_calendar(
            back_callback="back_to_loading", hide_sundays=True, min_date=min_date
        )

    await state.set_state(QueueForm.calendar)
    await callback.message.answer(
        "📅 <b>Крок 6/6</b>\nОберіть дату та час візиту:", reply_markup=markup
    )
    await callback.answer()


@dp.callback_query(QueueForm.hour, F.data.startswith("hour_"))
async def hour_selected(callback: types.CallbackQuery, state: FSMContext):
    hour = callback.data.replace("hour_", "")
    data = await state.get_data()
    chosen_date: date | None = data.get("date")
    min_dt = get_min_datetime_from_state(data)

    if not chosen_date:
        return await callback.answer("Оберіть дату", show_alert=True)

    valid_hours = {f"{h:02d}" for h in available_hours(chosen_date, earliest_dt=min_dt)}
    if hour not in valid_hours:
        return await callback.answer("Цей час вже недоступний", show_alert=True)

    await state.update_data(hour=hour)

    kb = InlineKeyboardBuilder()
    for m in available_minutes(chosen_date, int(hour), earliest_dt=min_dt):
        kb.button(text=f"{m:02d}", callback_data=f"min_{m:02d}")
    kb.adjust(6)

    await callback.message.answer(
        "🕒 Оберіть хвилини прибуття:",
        reply_markup=add_inline_navigation(kb, back_callback="back_to_hour").as_markup()
    )
    await state.set_state(QueueForm.minute)


@dp.callback_query(QueueForm.minute, F.data.startswith("min_"))
async def minute_selected(callback: types.CallbackQuery, state: FSMContext):

    minute = callback.data.replace("min_", "")
    data = await state.get_data()

    chosen_date: date | None = data.get("date")
    chosen_hour = data.get("hour")
    min_dt = get_min_datetime_from_state(data) or min_planned_datetime()

    if not chosen_date or chosen_date < kyiv_now().date():
        return await callback.answer("Оберіть доступну дату", show_alert=True)

    if chosen_hour is None:
        return await callback.answer("Спочатку оберіть годину", show_alert=True)

    selected_time = dtime(hour=int(chosen_hour), minute=int(minute))
    planned_dt = to_kyiv(datetime.combine(chosen_date, selected_time))

    if planned_dt < min_dt:
        return await callback.answer(
            "Можна обрати час не раніше ніж через 1 годину після створення заявки.",
            show_alert=True,
        )

    if int(minute) not in available_minutes(
        chosen_date, int(chosen_hour), earliest_dt=min_dt
    ):
        return await callback.answer("Цей час вже недоступний", show_alert=True)

    creation_time = kyiv_now()
    async with SessionLocal() as session:
        req = Request(
            user_id=callback.from_user.id,
            supplier=data["supplier"],
            phone=data["phone"],
            car=data["car"],
            cargo_description=data["cargo_description"],
            loading_type=data["loading_type"],
            planned_date=chosen_date,
            planned_time=f"{int(chosen_hour):02d}:{int(minute):02d}",
            date=chosen_date,
            time=f"{int(chosen_hour):02d}:{int(minute):02d}",
            status="new",
            created_at=creation_time.replace(tzinfo=None),
            updated_at=creation_time.replace(tzinfo=None),
        )

        session.add(req)
        await session.commit()
        await session.refresh(req)

    await callback.message.answer(
        f"✅ Заявка #{req.id} відправлена на розгляд.\n"
        f"📅 {req.date.strftime('%d.%m.%Y')} • ⏰ {req.time}",
        reply_markup=navigation_keyboard(include_back=False)
    )

    await sheet_client.sync_request(req)

    # Рассылка всем админам
    await broadcast_new_request(req.id)

    await state.clear()

@dp.callback_query(QueueForm.minute, F.data == "back_to_hour")
async def back_to_hour(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chosen_date: date | None = data.get("date")
    min_dt = get_min_datetime_from_state(data)

    kb = InlineKeyboardBuilder()
    if chosen_date:
        hours = available_hours(chosen_date, earliest_dt=min_dt)
        for hour in hours:
            kb.button(text=f"{hour:02d}", callback_data=f"hour_{hour:02d}")
    kb.adjust(6)

    await state.set_state(QueueForm.hour)
    await callback.message.answer(
        "⏰ Оберіть годину:",
        reply_markup=add_inline_navigation(kb, back_callback="back_to_calendar").as_markup()
    )
    await callback.answer()



###############################################################
#  SEND REQUEST TO ALL ADMINS (AND SEND DOCS IF AVAILABLE)    
###############################################################

async def broadcast_new_request(req_id: int):
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        admins = (await session.execute(select(Admin))).scalars().all()

    text = (
        f"<b>🆕 Нова заявка #{req.id}</b>\n"
        "━━━━━━━━━━━━━━━━\n"
        f"🏢 <b>Постачальник:</b> {req.supplier}\n"
        f"📞 <b>Контакт:</b> {req.phone}\n"
        f"🚚 <b>Об'єм:</b> {req.car}\n"
        f"📦 <b>Товар:</b> {req.cargo_description or ''}\n"
        f"🧱 <b>Тип завантаження:</b> {req.loading_type}\n"
        f"📅 <b>План:</b> {req.planned_date.strftime('%d.%m.%Y')}\n"
        f"⏰ <b>Час:</b> {req.planned_time}\n"
    )

    for admin in admins:
        kb = InlineKeyboardBuilder()
        kb.button(text="✔ Підтвердити", callback_data=f"adm_ok_{req.id}")
        kb.button(text="🔁 Змінити дату/час", callback_data=f"adm_change_{req.id}")
        kb.button(text="❌ Відхилити", callback_data=f"adm_rej_{req.id}")
        kb.adjust(1)

        try:
            await bot.send_message(admin.telegram_id, text, reply_markup=kb.as_markup())
        except:
            pass


async def notify_user_about_admin_change(
    req: Request,
    *,
    admin_reason: str | None = None,
    limited: bool = False,
    rejection_reason: str | None = None,
):
    reason_block = ""
    if admin_reason:
        reason_block += f"\nПричина зміни від адміністратора: {admin_reason}"
    if rejection_reason:
        reason_block += f"\nПричина відмови адміністратора: {rejection_reason}"

    await bot.send_message(
        req.user_id,
        (
            f"🔄 Адміністратор запропонував нові дату та час для вашої заявки #{req.id}.\n"
            f"📅 {req.planned_date.strftime('%d.%m.%Y')}  ⏰ {req.planned_time}\n"
            f"Відреагуйте, будь ласка:\n"
            "• Підтвердіть запропонований час\n"
            "• Вкажіть причину відмови\n"
            "• Запропонуйте інший час або скасуйте заявку"
            f"{reason_block}"
        ),
        reply_markup=build_user_change_keyboard(req.id, limited=limited),
    )


def build_admin_decision_keyboard(req_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Залишити час користувача", callback_data=f"adm_user_keep_client_{req_id}")
    kb.button(text="🕒 Залишити час адміністратора", callback_data=f"adm_user_keep_admin_{req_id}")
    kb.button(text="🔁 Призначити інший час", callback_data=f"adm_change_{req_id}")
    kb.button(text="❌ Відхилити заявку", callback_data=f"adm_rej_{req_id}")
    kb.adjust(1)
    return kb.as_markup()


def build_admin_user_proposal_keyboard(req_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Підтвердити час користувача", callback_data=f"adm_accept_user_proposal_{req_id}")
    kb.button(text="❌ Відхилити час користувача", callback_data=f"adm_reject_user_proposal_{req_id}")
    kb.button(text="🔁 Запропонувати інший час", callback_data=f"adm_change_{req_id}")
    kb.button(text="🛑 Відхилити заявку", callback_data=f"adm_rej_{req_id}")
    kb.adjust(1)
    return kb.as_markup()


async def notify_admins_about_user_decline(req: Request, reason: str):
    async with SessionLocal() as session:
        admins = (await session.execute(select(Admin))).scalars().all()

    plan_text = format_plan_datetime(req)
    text = (
        f"ℹ️ Користувач <b>{req.supplier}</b> відмовився від запропонованих змін для заявки #{req.id}.\n"
        f"Поточний час (адм): {plan_text}\n"
        f"Початковий час користувача: {req.date.strftime('%d.%m.%Y')} {req.time}\n"
        f"Причина користувача: {reason}\n\n"
        "Оберіть подальшу дію:"
    )

    for admin in admins:
        try:
            await bot.send_message(admin.telegram_id, text, reply_markup=build_admin_decision_keyboard(req.id))
        except Exception:
            pass


async def notify_admins_about_user_proposal(req: Request, user_reason: str):
    async with SessionLocal() as session:
        admins = (await session.execute(select(Admin))).scalars().all()

    plan_text = format_plan_datetime(req)
    pending_text = ""
    if req.pending_date and req.pending_time:
        pending_text = f"{req.pending_date.strftime('%d.%m.%Y')} {req.pending_time}"

    text = (
        f"ℹ️ Користувач <b>{req.supplier}</b> запропонував новий час для заявки #{req.id}.\n"
        f"Адмін пропонував: {plan_text}\n"
        f"Нова пропозиція користувача: {pending_text}\n"
        f"Причина користувача: {user_reason}\n\n"
        "Потрібно прийняти рішення."
    )

    for admin in admins:
        try:
            await bot.send_message(admin.telegram_id, text, reply_markup=build_admin_user_proposal_keyboard(req.id))
        except Exception:
            pass
###############################################################
#          ADMIN APPROVE / REJECT / CHANGE DATE-TIME          
###############################################################

@dp.callback_query(F.data.startswith("adm_ok_"))
async def adm_ok(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[2])

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        req.status = "approved"
        req.admin_id = callback.from_user.id
        set_updated_now(req)
        await session.commit()

    await callback.message.answer("✔ Підтверджено!")

    await sheet_client.sync_request(req)

    # Уведомление водителю
    await bot.send_message(
        req.user_id,
        f"🎉 <b>Заявка #{req.id} підтверджена!</b>\n"
        f"📅 {req.date.strftime('%d.%m.%Y')}  ⏰ {req.time}"
    )

    # Уведомление всех админов
    await notify_admins_about_action(req, "підтверджена")


@dp.callback_query(F.data.startswith("adm_rej_"))
async def adm_rej(callback: types.CallbackQuery, state: FSMContext):
    req_id = int(callback.data.split("_")[2])

    await callback.message.answer(
        f"✏️ Вкажіть причину відхилення заявки #{req_id}:",
    )
    await callback.answer()
    await state.set_state(AdminRejectForm.reason)
    await state.update_data(req_id=req_id)


@dp.message(AdminRejectForm.reason)
async def adm_rej_reason(message: types.Message, state: FSMContext):
    reason = (message.text or "").strip()
    if not reason:
        return await message.answer("Будь ласка, вкажіть причину відхилення.")

    data = await state.get_data()
    req_id = data.get("req_id")

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req:
            await state.clear()
            return await message.answer("Заявку не знайдено.")

        req.status = "rejected"
        req.admin_id = message.from_user.id
        set_updated_now(req)
        await session.commit()

    await message.answer("❌ Заявку відхилено.")

    await sheet_client.sync_request(req)

    await bot.send_message(
        req.user_id,
        f"❌ <b>Заявку #{req.id} відхилено адміністратором.</b>\n"
        f"Причина: {reason}"
    )

    await notify_admins_about_action(req, "відхилена", reason=reason)
    await state.clear()


@dp.callback_query(F.data.startswith("adm_finish_"))
async def adm_finish(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    async with SessionLocal() as session:
        admin = (
            await session.execute(
                select(Admin).where(Admin.telegram_id == user_id)
            )
        ).scalar_one_or_none()

    is_superadmin = user_id == SUPERADMIN_ID or (admin and admin.is_superadmin)
    if not (is_superadmin or admin):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)

    req = await complete_request(req_id, auto=False)
    if not req:
        return await callback.answer(
            "Не можна завершити: заявка не підтверджена або вже завершена.",
            show_alert=True,
        )

    await callback.message.answer("🏁 Заявка позначена як завершена.")
    await callback.answer()
@dp.callback_query(F.data.startswith("adm_del_"))
async def adm_delete(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        admin = (
            await session.execute(
                select(Admin).where(Admin.telegram_id == user_id)
            )
        ).scalar_one_or_none()

        is_superadmin = user_id == SUPERADMIN_ID or (admin and admin.is_superadmin)

        if not req:
            return await callback.answer("Заявка не знайдена", show_alert=True)

        if not (is_superadmin or admin):
            return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)

        if not is_superadmin and req.status == "new":
            return await callback.answer(
                "Заявки зі статусом 'Нова' може видаляти лише суперадміністратор.",
                show_alert=True,
            )

        await session.delete(req)
        await session.commit()

    await sheet_client.delete_request(req)

    await callback.message.answer("🗑 Заявку видалено з бази.")
    await callback.answer()

###############################################################
#           ADMIN — CHANGE DATE/TIME (FSM Aiogram 3)
###############################################################

@dp.callback_query(F.data.startswith("adm_change_"))
async def adm_change(callback: types.CallbackQuery, state: FSMContext):
    req_id = int(callback.data.split("_")[2])
    await state.update_data(req_id=req_id)

    await callback.message.answer(
        "🔄 Оберіть нову дату:",
        reply_markup=build_date_calendar(back_callback="admin_change_back")
    )
    await state.set_state(AdminChangeForm.calendar)


@dp.callback_query(AdminChangeForm.calendar, F.data.startswith("prev_"))
async def adm_cal_prev(callback: types.CallbackQuery):
    _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(
        reply_markup=build_date_calendar(int(y), int(m), back_callback="admin_change_back")
    )


@dp.callback_query(AdminChangeForm.calendar, F.data.startswith("next_"))
async def adm_cal_next(callback: types.CallbackQuery):
    _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(
        reply_markup=build_date_calendar(int(y), int(m), back_callback="admin_change_back")
    )


@dp.callback_query(AdminChangeForm.calendar, F.data == "admin_change_back")
async def adm_change_back(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    is_superadmin = await is_super_admin_user(callback.from_user.id)
    await callback.message.answer(
        "Операцію зміни дати/часу скасовано.",
        reply_markup=admin_menu(is_superadmin=is_superadmin)
    )
    await callback.answer()


@dp.callback_query(AdminChangeForm.calendar, F.data.startswith("day_"))
async def adm_cal_day(callback: types.CallbackQuery, state: FSMContext):
    _, y, m, d = callback.data.split("_")
    chosen_date = date(int(y), int(m), int(d))

    if chosen_date < kyiv_now().date():
        return await callback.answer("Не можна обирати минулі дати", show_alert=True)

    await state.update_data(new_date=chosen_date)

    kb = InlineKeyboardBuilder()
    hours = available_hours(chosen_date)
    for h in hours:
        kb.button(text=f"{h:02d}", callback_data=f"ach_hour_{h:02d}")
    kb.adjust(6)

    if not hours:
        await callback.message.answer(
            "На цю дату немає доступних часових слотів. Оберіть іншу дату.",
            reply_markup=add_inline_navigation(
                InlineKeyboardBuilder(), back_callback="admin_back_to_calendar"
            ).as_markup(),
        )
        return await callback.answer()

    await callback.message.answer(
        "⏰ Оберіть годину:",
        reply_markup=add_inline_navigation(kb, back_callback="admin_back_to_calendar").as_markup()
    )
    await state.set_state(AdminChangeForm.hour)


@dp.callback_query(AdminChangeForm.hour, F.data.startswith("ach_hour_"))
async def adm_hour(callback: types.CallbackQuery, state: FSMContext):
    hour = callback.data.replace("ach_hour_", "")
    data = await state.get_data()
    chosen_date: date | None = data.get("new_date")

    if not chosen_date:
        return await callback.answer("Оберіть дату", show_alert=True)

    valid_hours = {f"{h:02d}" for h in available_hours(chosen_date)}
    if hour not in valid_hours:
        return await callback.answer("Цей час вже недоступний", show_alert=True)

    await state.update_data(new_hour=hour)

    kb = InlineKeyboardBuilder()
    for m in available_minutes(chosen_date, int(hour)):
        kb.button(text=f"{m:02d}", callback_data=f"ach_min_{m:02d}")
    kb.adjust(6)

    await callback.message.answer(
        "🕒 Оберіть хвилини:",
        reply_markup=add_inline_navigation(kb, back_callback="admin_back_to_hour").as_markup()
    )
    await state.set_state(AdminChangeForm.minute)

@dp.callback_query(AdminChangeForm.hour, F.data == "admin_back_to_calendar")
async def admin_back_to_calendar(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chosen_date: date | None = data.get("new_date")

    if chosen_date:
        markup = build_date_calendar(
            chosen_date.year,
            chosen_date.month,
            back_callback="admin_change_back"
        )
    else:
        markup = build_date_calendar(back_callback="admin_change_back")

    await state.set_state(AdminChangeForm.calendar)
    await callback.message.answer("🔄 Оберіть нову дату:", reply_markup=markup)
    await callback.answer()


@dp.callback_query(AdminChangeForm.minute, F.data == "admin_back_to_hour")
async def admin_back_to_hour(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chosen_date: date | None = data.get("new_date")

    kb = InlineKeyboardBuilder()
    if chosen_date:
        hours = available_hours(chosen_date)
        for h in hours:
            kb.button(text=f"{h:02d}", callback_data=f"ach_hour_{h:02d}")
    kb.adjust(6)

    await state.set_state(AdminChangeForm.hour)
    await callback.message.answer(
        "⏰ Оберіть годину:",
        reply_markup=add_inline_navigation(kb, back_callback="admin_back_to_calendar").as_markup()
    )
    await callback.answer()



@dp.callback_query(AdminChangeForm.minute, F.data.startswith("ach_min_"))
async def adm_min(callback: types.CallbackQuery, state: FSMContext):

    minute = callback.data.replace("ach_min_", "")
    data = await state.get_data()
    req_id = data["req_id"]

    new_date = data["new_date"]
    new_hour = data.get("new_hour")

    if new_date < kyiv_now().date():
        return await callback.answer("Дата не може бути в минулому", show_alert=True)

    if new_hour is None:
        return await callback.answer("Спочатку оберіть годину", show_alert=True)

    if int(minute) not in available_minutes(new_date, int(new_hour)):
        return await callback.answer("Цей час вже недоступний", show_alert=True)

    new_time = f"{int(new_hour):02d}:{int(minute):02d}"
    await state.update_data(new_time=new_time)
    await state.set_state(AdminChangeForm.reason)

    await callback.message.answer(
        f"✏️ Вкажіть причину зміни дати/часу для заявки #{req_id}:",
    )
    await callback.answer()


@dp.message(AdminChangeForm.reason)
async def adm_change_reason(message: types.Message, state: FSMContext):
    reason = (message.text or "").strip()
    if not reason:
        return await message.answer("Будь ласка, вкажіть причину зміни дати або часу.")

    data = await state.get_data()
    req_id = data.get("req_id")
    new_date: date | None = data.get("new_date")
    new_time: str | None = data.get("new_time")

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or not new_date or not new_time:
            await state.clear()
            return await message.answer("Не вдалося змінити дату/час.")

        req.planned_date = new_date
        req.planned_time = new_time
        req.pending_date = None
        req.pending_time = None
        req.pending_reason = merge_pending_reason(None, "Admin", reason)
        req.status = "pending_user_confirmation"
        req.admin_id = message.from_user.id
        set_updated_now(req)
        await session.commit()
        await session.refresh(req)

    await message.answer("🔁 Запит на зміну дати/часу надіслано користувачу для підтвердження.")

    await sheet_client.sync_request(req)

    await notify_user_about_admin_change(req, admin_reason=reason)

    await notify_admins_about_action(req, "змінена (очікує підтвердження користувача)", reason=reason)

    await state.clear()


###############################################################
#      USER REACTION TO ADMIN DATE/TIME CHANGE                
###############################################################


async def _load_request_for_user_decision(
    req_id: int,
    user_id: int,
    allowed_statuses: set[str],
) -> Request | None:
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or req.user_id != user_id or req.status not in allowed_statuses:
            return None
    return req


@dp.callback_query(F.data.startswith("user_change_confirm_"))
async def user_change_confirm(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[-1])

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or req.user_id != callback.from_user.id or req.status not in {"pending_user_confirmation", "pending_user_final"}:
            return await callback.answer("Ця дія недоступна.", show_alert=True)
        if not req.planned_date or not req.planned_time:
            return await callback.answer("Немає запропонованої дати/часу.", show_alert=True)

        req.date = req.planned_date
        req.time = req.planned_time
        req.status = "approved"
        req.pending_date = None
        req.pending_time = None
        req.pending_reason = None
        set_updated_now(req)
        await session.commit()
        await session.refresh(req)

    await sheet_client.sync_request(req)

    await callback.message.answer(
        f"✅ Ви підтвердили запропоновані зміни. Заявка #{req.id} оновлена.\n"
        f"📅 {req.date.strftime('%d.%m.%Y')}  ⏰ {req.time}",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await callback.answer()
    await notify_admins_about_action(req, "підтверджена користувачем після зміни")


@dp.callback_query(F.data.startswith("user_change_delete_"))
async def user_change_delete(callback: types.CallbackQuery, state: FSMContext):
    req_id = int(callback.data.split("_")[-1])
    req = await _load_request_for_user_decision(
        req_id, callback.from_user.id, {"pending_user_confirmation", "pending_user_final"}
    )
    if not req:
        return await callback.answer("Дія недоступна.", show_alert=True)

    await state.set_state(UserChangeResponse.delete_reason)
    await state.update_data(req_id=req_id)
    await callback.message.answer(
        "Вкажіть причину скасування заявки:", reply_markup=navigation_keyboard(include_back=False)
    )
    await callback.answer()


@dp.message(UserChangeResponse.delete_reason)
async def user_change_delete_reason(message: types.Message, state: FSMContext):
    reason = (message.text or "").strip()
    if not reason:
        return await message.answer("Причина не може бути порожньою.")

    data = await state.get_data()
    req_id = data.get("req_id")

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or req.user_id != message.from_user.id or req.status not in {"pending_user_confirmation", "pending_user_final"}:
            await state.clear()
            return await message.answer("Заявку не знайдено або дія недоступна.")

        req.status = "rejected"
        req.pending_date = None
        req.pending_time = None
        req.pending_reason = merge_pending_reason(req.pending_reason, "User cancel", reason)
        set_updated_now(req)
        await session.commit()
        await session.refresh(req)

    await sheet_client.sync_request(req)

    await message.answer(
        f"Заявка #{req.id} відхилена за вашою ініціативою.",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await notify_admins_about_action(req, "відхилена користувачем після зміни", reason=reason)
    await state.clear()


@dp.callback_query(F.data.startswith("user_change_decline_"))
async def user_change_decline(callback: types.CallbackQuery, state: FSMContext):
    req_id = int(callback.data.split("_")[-1])
    req = await _load_request_for_user_decision(
        req_id, callback.from_user.id, {"pending_user_confirmation"}
    )
    if not req:
        return await callback.answer("Ця дія недоступна.", show_alert=True)

    await state.set_state(UserChangeResponse.decline_reason)
    await state.update_data(req_id=req_id)
    await callback.message.answer(
        "Вкажіть причину, чому ви не згодні з новим часом:", reply_markup=navigation_keyboard(include_back=False)
    )
    await callback.answer()


@dp.message(UserChangeResponse.decline_reason)
async def user_change_decline_reason(message: types.Message, state: FSMContext):
    reason = (message.text or "").strip()
    if not reason:
        return await message.answer("Причина не може бути порожньою.")

    data = await state.get_data()
    req_id = data.get("req_id")

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or req.user_id != message.from_user.id or req.status != "pending_user_confirmation":
            await state.clear()
            return await message.answer("Дія недоступна або заявка не знайдена.")

        req.status = "pending_admin_decision"
        req.pending_reason = merge_pending_reason(req.pending_reason, "User", reason)
        set_updated_now(req)
        await session.commit()
        await session.refresh(req)

    await sheet_client.sync_request(req)

    await message.answer(
        "Вашу відмову зафіксовано. Адміністратор розгляне причину та відповість.",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await notify_admins_about_user_decline(req, reason)
    await state.clear()


@dp.callback_query(F.data.startswith("user_change_propose_"))
async def user_change_propose(callback: types.CallbackQuery, state: FSMContext):
    req_id = int(callback.data.split("_")[-1])
    req = await _load_request_for_user_decision(
        req_id, callback.from_user.id, {"pending_user_confirmation"}
    )
    if not req:
        return await callback.answer("Ця дія недоступна.", show_alert=True)

    min_dt = min_planned_datetime(req.created_at)
    await state.update_data(req_id=req_id, min_plan_dt=min_dt.isoformat())
    await state.set_state(UserChangeResponse.propose_reason)
    await callback.message.answer(
        "Опишіть, чому вам не підходить запропонований час:",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await callback.answer()


@dp.message(UserChangeResponse.propose_reason)
async def user_change_propose_reason(message: types.Message, state: FSMContext):
    reason = (message.text or "").strip()
    if not reason:
        return await message.answer("Причина не може бути порожньою.")

    data = await state.get_data()
    req_id = data.get("req_id")
    await state.update_data(user_reason=reason)
    await state.set_state(UserChangeResponse.calendar)

    min_date = get_min_date_from_state(data)

    await message.answer(
        "Оберіть нову дату:",
        reply_markup=build_date_calendar(
            back_callback="user_change_cancel",
            hide_sundays=True,
            min_date=min_date,
        ),
    )


@dp.callback_query(UserChangeResponse.calendar, F.data.startswith("prev_"))
async def user_change_prev(callback: types.CallbackQuery, state: FSMContext):
    _, y, m = callback.data.split("_")
    data = await state.get_data()
    min_date = get_min_date_from_state(data)
    await callback.message.edit_reply_markup(
        reply_markup=build_date_calendar(
            int(y),
            int(m),
            back_callback="user_change_cancel",
            hide_sundays=True,
            min_date=min_date,
        )
    )
    await callback.answer()


@dp.callback_query(UserChangeResponse.calendar, F.data.startswith("next_"))
async def user_change_next(callback: types.CallbackQuery, state: FSMContext):
    _, y, m = callback.data.split("_")
    data = await state.get_data()
    min_date = get_min_date_from_state(data)
    await callback.message.edit_reply_markup(
        reply_markup=build_date_calendar(
            int(y),
            int(m),
            back_callback="user_change_cancel",
            hide_sundays=True,
            min_date=min_date,
        )
    )
    await callback.answer()


@dp.callback_query(UserChangeResponse.calendar, F.data == "close_calendar")
async def user_change_close_calendar(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer(
        "Скасовано вибір нового часу.",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await callback.answer()


@dp.callback_query(UserChangeResponse.calendar, F.data == "user_change_cancel")
async def user_change_cancel(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer(
        "Скасовано вибір нового часу.",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await callback.answer()


@dp.callback_query(UserChangeResponse.calendar, F.data.startswith("day_"))
async def user_change_day(callback: types.CallbackQuery, state: FSMContext):
    _, y, m, d = callback.data.split("_")
    chosen = date(int(y), int(m), int(d))
    data = await state.get_data()
    min_date = get_min_date_from_state(data)
    min_dt = get_min_datetime_from_state(data)

    if chosen < kyiv_now().date():
        return await callback.answer("Не можна обирати минулі дати", show_alert=True)

    if min_date and chosen < min_date:
        return await callback.answer(
            "Можна обрати час не раніше ніж через 1 годину після створення заявки.",
            show_alert=True,
        )

    if chosen.weekday() == 6:
        return await callback.answer(
            "Запис у неділю недоступний. Оберіть іншу дату.", show_alert=True
        )

    await state.update_data(new_date=chosen)

    kb = InlineKeyboardBuilder()
    hours = available_hours(chosen, earliest_dt=min_dt)
    for hour in hours:
        kb.button(text=f"{hour:02d}", callback_data=f"uchour_{hour:02d}")
    kb.adjust(6)

    if not hours:
        await callback.message.answer(
            "На цю дату немає доступних часових слотів. Оберіть іншу дату.",
            reply_markup=add_inline_navigation(
                InlineKeyboardBuilder(), back_callback="user_change_cancel"
            ).as_markup(),
        )
        return await callback.answer()

    await callback.message.answer(
        "⏰ Оберіть годину:",
        reply_markup=add_inline_navigation(kb, back_callback="user_change_cancel").as_markup()
    )
    await state.set_state(UserChangeResponse.hour)
    await callback.answer()


@dp.callback_query(UserChangeResponse.hour, F.data == "user_change_cancel")
async def user_change_back_to_calendar(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chosen_date: date | None = data.get("new_date")
    min_date = get_min_date_from_state(data)

    if chosen_date:
        markup = build_date_calendar(
            chosen_date.year,
            chosen_date.month,
            back_callback="user_change_cancel",
            hide_sundays=True,
            min_date=min_date,
        )
    else:
        markup = build_date_calendar(
            back_callback="user_change_cancel", hide_sundays=True, min_date=min_date
        )

    await state.set_state(UserChangeResponse.calendar)
    await callback.message.answer(
        "Оберіть нову дату:",
        reply_markup=markup,
    )
    await callback.answer()


@dp.callback_query(UserChangeResponse.hour, F.data.startswith("uchour_"))
async def user_change_hour(callback: types.CallbackQuery, state: FSMContext):
    hour = callback.data.replace("uchour_", "")
    data = await state.get_data()
    chosen_date: date | None = data.get("new_date")
    min_dt = get_min_datetime_from_state(data)

    if not chosen_date:
        return await callback.answer("Оберіть дату", show_alert=True)

    valid_hours = {f"{h:02d}" for h in available_hours(chosen_date, earliest_dt=min_dt)}
    if hour not in valid_hours:
        return await callback.answer("Цей час вже недоступний", show_alert=True)

    await state.update_data(new_hour=hour)

    kb = InlineKeyboardBuilder()
    for m in available_minutes(chosen_date, int(hour), earliest_dt=min_dt):
        kb.button(text=f"{m:02d}", callback_data=f"ucmin_{m:02d}")
    kb.adjust(6)

    await callback.message.answer(
        "🕒 Оберіть хвилини:",
        reply_markup=add_inline_navigation(kb, back_callback="user_change_cancel").as_markup()
    )
    await state.set_state(UserChangeResponse.minute)
    await callback.answer()


@dp.callback_query(UserChangeResponse.minute, F.data == "user_change_cancel")
async def user_change_back_to_hour(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chosen_date: date | None = data.get("new_date")
    min_dt = get_min_datetime_from_state(data)

    kb = InlineKeyboardBuilder()
    if chosen_date:
        hours = available_hours(chosen_date, earliest_dt=min_dt)
        for h in hours:
            kb.button(text=f"{h:02d}", callback_data=f"uchour_{h:02d}")
    kb.adjust(6)

    await state.set_state(UserChangeResponse.hour)
    await callback.message.answer(
        "⏰ Оберіть годину:",
        reply_markup=add_inline_navigation(kb, back_callback="user_change_cancel").as_markup()
    )
    await callback.answer()


@dp.callback_query(UserChangeResponse.minute, F.data.startswith("ucmin_"))
async def user_change_minute(callback: types.CallbackQuery, state: FSMContext):
    minute = callback.data.replace("ucmin_", "")
    data = await state.get_data()
    req_id = data.get("req_id")
    chosen_date: date | None = data.get("new_date")
    chosen_hour = data.get("new_hour")
    min_dt = get_min_datetime_from_state(data)
    user_reason = data.get("user_reason", "")

    if not chosen_date or chosen_date < kyiv_now().date():
        return await callback.answer("Оберіть доступну дату", show_alert=True)

    if chosen_hour is None:
        return await callback.answer("Спочатку оберіть годину", show_alert=True)

    if min_dt:
        min_date = min_dt.date()
        if chosen_date < min_date:
            return await callback.answer(
                "Можна обрати час не раніше ніж через 1 годину після створення заявки.",
                show_alert=True,
            )

    if int(minute) not in available_minutes(
        chosen_date, int(chosen_hour), earliest_dt=min_dt
    ):
        return await callback.answer("Цей час вже недоступний", show_alert=True)

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or req.user_id != callback.from_user.id or req.status != "pending_user_confirmation":
            await state.clear()
            return await callback.answer("Дія недоступна.", show_alert=True)

        req.pending_date = chosen_date
        req.pending_time = f"{int(chosen_hour):02d}:{int(minute):02d}"
        req.status = "pending_admin_decision"
        req.pending_reason = merge_pending_reason(req.pending_reason, "User", user_reason)
        set_updated_now(req)
        await session.commit()
        await session.refresh(req)

    await sheet_client.sync_request(req)

    await callback.message.answer(
        "Пропозицію щодо нового часу відправлено адміністратору. Очікуйте відповіді.",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await notify_admins_about_user_proposal(req, user_reason)
    await state.clear()
    await callback.answer()


###############################################################
#        ADMIN DECISIONS AFTER USER RESPONSE                  
###############################################################


@dp.callback_query(F.data.startswith("adm_user_keep_client_"))
async def adm_keep_client_time(callback: types.CallbackQuery):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)

    req_id = int(callback.data.split("_")[-1])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or req.status not in {"pending_admin_decision", "pending_user_confirmation", "pending_user_final"}:
            return await callback.answer("Заявка недоступна для цієї дії.", show_alert=True)

        req.planned_date = req.date
        req.planned_time = req.time
        req.pending_date = None
        req.pending_time = None
        req.pending_reason = None
        req.status = "approved"
        req.admin_id = callback.from_user.id
        set_updated_now(req)
        await session.commit()
        await session.refresh(req)

    await sheet_client.sync_request(req)

    await callback.message.answer("✅ Залишили час користувача та підтвердили заявку.")
    await bot.send_message(
        req.user_id,
        f"✅ Адміністратор залишив ваш початковий час для заявки #{req.id}.\n"
        f"📅 {req.date.strftime('%d.%m.%Y')}  ⏰ {req.time}",
    )
    await notify_admins_about_action(req, "підтверджена (залишено час користувача)")
    await callback.answer()


@dp.callback_query(F.data.startswith("adm_user_keep_admin_"))
async def adm_keep_admin_time(callback: types.CallbackQuery):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)

    req_id = int(callback.data.split("_")[-1])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or req.status not in {"pending_admin_decision", "pending_user_confirmation"}:
            return await callback.answer("Заявка недоступна для цієї дії.", show_alert=True)

        req.status = "pending_user_final"
        req.pending_reason = merge_pending_reason(
            req.pending_reason,
            "Admin",
            "Адміністратор залишив запропонований час після відмови користувача.",
        )
        set_updated_now(req)
        await session.commit()
        await session.refresh(req)

    await sheet_client.sync_request(req)

    await callback.message.answer(
        "⏳ Очікуємо остаточне рішення користувача щодо часу адміністратора."
    )
    await notify_user_about_admin_change(
        req,
        admin_reason="Адміністратор залишив запропонований раніше час.",
        limited=True,
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("adm_accept_user_proposal_"))
async def adm_accept_user_proposal(callback: types.CallbackQuery):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)

    req_id = int(callback.data.split("_")[-1])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or req.status != "pending_admin_decision" or not req.pending_date or not req.pending_time:
            return await callback.answer("Немає пропозиції користувача для підтвердження.", show_alert=True)

        req.planned_date = req.pending_date
        req.planned_time = req.pending_time
        req.date = req.pending_date
        req.time = req.pending_time
        req.pending_date = None
        req.pending_time = None
        req.pending_reason = None
        req.status = "approved"
        req.admin_id = callback.from_user.id
        set_updated_now(req)
        await session.commit()
        await session.refresh(req)

    await sheet_client.sync_request(req)

    await callback.message.answer("✅ Пропозиція користувача підтверджена.")
    await bot.send_message(
        req.user_id,
        f"✅ Адміністратор підтвердив запропонований вами час для заявки #{req.id}.\n"
        f"📅 {req.date.strftime('%d.%m.%Y')}  ⏰ {req.time}",
    )
    await notify_admins_about_action(req, "підтверджена (час користувача)")
    await callback.answer()


@dp.callback_query(F.data.startswith("adm_reject_user_proposal_"))
async def adm_reject_user_proposal(callback: types.CallbackQuery, state: FSMContext):
    if not await is_admin_user(callback.from_user.id):
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)

    req_id = int(callback.data.split("_")[-1])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or req.status != "pending_admin_decision":
            return await callback.answer("Дія недоступна.", show_alert=True)

    await state.set_state(AdminUserProposalReject.reason)
    await state.update_data(req_id=req_id)
    await callback.message.answer(
        "Вкажіть причину відхилення пропозиції користувача:",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await callback.answer()


@dp.message(AdminUserProposalReject.reason)
async def adm_reject_user_proposal_reason(message: types.Message, state: FSMContext):
    reason = (message.text or "").strip()
    if not reason:
        return await message.answer("Причина не може бути порожньою.")

    data = await state.get_data()
    req_id = data.get("req_id")

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or req.status != "pending_admin_decision":
            await state.clear()
            return await message.answer("Заявка не знайдена або дія недоступна.")

        req.pending_date = None
        req.pending_time = None
        req.status = "pending_user_final"
        req.pending_reason = merge_pending_reason(req.pending_reason, "Admin", reason)
        req.admin_id = message.from_user.id
        set_updated_now(req)
        await session.commit()
        await session.refresh(req)

    await sheet_client.sync_request(req)

    await message.answer("Відповідь користувачу надіслано.")
    await notify_user_about_admin_change(req, rejection_reason=reason, limited=True)
    await state.clear()


###############################################################
#        BROADCAST ACTION TO ALL ADMINS (Uniﬁed Function)     
###############################################################

async def notify_admins_about_action(req: Request, action: str, *, reason: str | None = None):
    async with SessionLocal() as session:
        admins = (await session.execute(select(Admin))).scalars().all()

    final_status = "Завершена" if req.completed_at else "Не завершена"
    text = (
        f"ℹ️ <b>Заявка #{req.id} {action}</b>\n\n"
        f"📅 {req.date.strftime('%d.%m.%Y')}  ⏰ {req.time}\n"
        f"🏢 {req.supplier}\n"
        f"🚚 {req.car}\n"
        f"🧱 {req.loading_type}\n"
        f"🏁 {final_status}"
    )
    if reason:
        text += f"\n\nПричина: {reason}"

    for a in admins:
        try:
            await bot.send_message(a.telegram_id, text)
        except:
            pass

async def notify_admins_np_delivery(supplier: str, ttn: str):
    async with SessionLocal() as session:
        admins = (await session.execute(select(Admin))).scalars().all()

    text = (
        "📦 <b>НП-відправка</b>\n"
        f"Постачальник <b>{supplier}</b> відправив посилку № {ttn}.\n"
        "Підтвердження не потрібне, це повідомлення лише для інформації."
    )

    for admin in admins:
        try:
            await bot.send_message(admin.telegram_id, text)
        except:
            pass

async def notify_admins_about_user_edit(
    req: Request, reason: str, changes: list[tuple[str, str, str]]
):
    async with SessionLocal() as session:
        admins = (await session.execute(select(Admin))).scalars().all()

    changes_text = "\n".join(
        f"• <b>{label}:</b> {old} → {new}" for label, old, new in changes
    ) or "• Зміни не зафіксовані"

    text = (
        f"ℹ️ Поставщик {req.supplier} змінив заявку #{req.id}\n"
        f"Причина: {reason}\n\n"
        f"Потрібно повторно підтвердити/відхилити або скоригувати дату чи час.\n"
        f"📅 {req.date.strftime('%d.%m.%Y')} ⏰ {req.time}\n"
        f"📞 {req.phone}\n"
        f"🚚 {req.car}\n\n"
        f"Що змінено:\n{changes_text}"
    )

    for admin in admins:
        try:
            await bot.send_message(admin.telegram_id, text)
        except:
            pass


###############################################################
#                 COMPLETE & AUTO-CLOSE REQUESTS
###############################################################

COMPLETION_MESSAGE = (
    "Заявка #{} завершена. Гарної Вам дороги та дякую за співпрацю."
)


async def complete_request(req_id: int, *, auto: bool = False) -> Request | None:
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or req.completed_at or req.status != "approved":
            return None

        req.completed_at = kyiv_now_naive()
        set_updated_now(req)
        await session.commit()
        await session.refresh(req)

    await sheet_client.sync_request(req)

    try:
        await bot.send_message(req.user_id, COMPLETION_MESSAGE.format(req.id))
    except Exception:
        pass

    await notify_admins_about_action(
        req, "завершена автоматично" if auto else "завершена"
    )
    return req


async def auto_close_overdue_requests():
    while True:
        try:
            await _auto_close_tick()
        except Exception as exc:
            logging.exception("Помилка автозакриття заявок: %s", exc)
        await asyncio.sleep(300)


async def _auto_close_tick():
    now = kyiv_now()
    async with SessionLocal() as session:
        res = await session.execute(
            select(Request).where(
                Request.status == "approved",
                Request.completed_at.is_(None),
            )
        )
        requests = res.scalars().all()

    for req in requests:
        confirmed_dt = get_confirmed_datetime(req)
        if confirmed_dt:
            close_after = confirmed_dt + timedelta(hours=20)
        else:
            approved_at = req.updated_at or req.created_at
            if not approved_at:
                continue

            if approved_at.tzinfo is None:
                approved_at = approved_at.replace(tzinfo=KYIV_TZ)

            close_after = approved_at + timedelta(hours=20)

        if now >= close_after:
            await complete_request(req.id, auto=True)
            
###############################################################
#                         BOT STARTUP                         
###############################################################

async def main():
    await init_db()

    # Создать суперадмина, если он не добавлен
    async with SessionLocal() as session:
        res = await session.execute(
            select(Admin).where(Admin.telegram_id == SUPERADMIN_ID)
        )
        if not res.scalar_one_or_none():
            session.add(
                Admin(
                    telegram_id=SUPERADMIN_ID,
                    is_superadmin=True,
                    last_name="Админ",
                )
            )
            await session.commit()

    asyncio.create_task(auto_close_overdue_requests())
    print("Bot started!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
