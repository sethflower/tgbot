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
    is_superadmin = Column(Boolean, default=False)


class Request(Base):
    __tablename__ = "requests"

    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False)

    supplier = Column(Text)
    driver_name = Column(Text)
    phone = Column(Text)
    car = Column(Text)

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

            # backfill plan and timestamps for existing rows
            sync_conn.execute(text("UPDATE requests SET planned_date = date WHERE planned_date IS NULL"))
            sync_conn.execute(text("UPDATE requests SET planned_time = time WHERE planned_time IS NULL"))
            sync_conn.execute(text("UPDATE requests SET updated_at = created_at WHERE updated_at IS NULL"))

        await conn.run_sync(ensure_sheet_row_column)


###############################################################
#                        GOOGLE SHEETS
###############################################################


def get_sheet_status(status: str) -> str:
    return {
        "new": "Новая",
        "approved": "Принятая",
        "rejected": "Отклонённая",
        "deleted_by_user": "Удалена",
    }.get(status, status)


class GoogleSheetClient:
    def __init__(self):
        self._worksheet = None
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
                return client.open_by_key(GOOGLE_SPREADSHEET_ID).sheet1

            self._worksheet = await asyncio.to_thread(_init_ws)
            logging.info("Google Sheets клієнт ініціалізовано")
        except Exception as exc:
            logging.exception("Не вдалося підключитися до Google Sheets: %s", exc)
            self._worksheet = None

        return self._worksheet is not None

    def _build_row(self, req: Request) -> list[str]:
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
            str(req.admin_id) if admin_decision and req.admin_id else "",
            req.completed_at.strftime("%d.%m.%Y %H:%M") if req.completed_at else "",
            str(req.id),
        ]

    async def _update_row(self, row_number: int, values: list[str]) -> bool:
        try:
            await asyncio.to_thread(
                self._worksheet.update,
                f"A{row_number}:O{row_number}",
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

        values = self._build_row(req)

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

        if not req.sheet_row:
            return

        try:
            await asyncio.to_thread(self._worksheet.delete_rows, req.sheet_row)
        except Exception as exc:
            logging.exception("Не вдалося видалити рядок %s у Sheets: %s", req.sheet_row, exc)

    async def clear_requests(self):
        if not await self._ensure_client():
            return

        try:
            await asyncio.to_thread(self._worksheet.batch_clear, ["A2:O"])
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
    await message.answer(
        "<b>🏠 DC Link черга | Головне меню</b>\n"
        "Оберіть, що зробити просто зараз:",
        reply_markup=navigation_keyboard(include_back=False),
    )
    await message.answer(
        "📍 Керування доступними розділами:",
        reply_markup=main_menu(),
    )


@dp.message(F.text == MAIN_MENU_TEXT)
async def handle_main_menu(message: types.Message, state: FSMContext):
    await show_main_menu(message, state)

@dp.callback_query(F.data == "go_main")
async def handle_main_menu_callback(callback: types.CallbackQuery, state: FSMContext):
    await show_main_menu(callback.message, state)
    await callback.answer()

def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Нова заявка", callback_data="menu_new")
    kb.button(text="📂 Мої останні заявки", callback_data="menu_my")
    kb.button(text="🛠 Адмін-панель", callback_data="menu_admin")
    kb.adjust(1)
    return kb.as_markup()

def admin_menu(is_superadmin: bool = False):
    kb = InlineKeyboardBuilder()
    kb.button(text="🆕 Нові заявки", callback_data="admin_new")
    kb.button(text="📚 Усі заявки", callback_data="admin_all")
    kb.button(text="🔎 Пошук за ID", callback_data="admin_search")
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


###############################################################
#                        FSM STATES                           
###############################################################

class QueueForm(StatesGroup):
    supplier = State()
    phone = State()
    car = State()
    loading_type = State()
    calendar = State()
    hour = State()
    minute = State()

class AdminAdd(StatesGroup):
    wait_id = State()

class AdminRemove(StatesGroup):
    wait_id = State()

class AdminSearch(StatesGroup):
    wait_id = State()

class AdminChangeForm(StatesGroup):
    calendar = State()
    hour = State()
    minute = State()

class UserDeleteForm(StatesGroup):
    user_id = State()
    reason = State()

class UserEditForm(StatesGroup):
    user_id = State()
    field_choice = State()
    supplier = State()
    phone = State()
    car = State()
    loading_type = State()
    calendar = State()     # выбор даты
    new_date = State()     # подтверждение даты
    hour = State()         # выбор часа
    minute = State()       # <-- ДОБАВИЛИ
    new_time = State()     # подтверждение времени
    reason = State()       # причина изменения





###############################################################
#                 START → BEAUTIFUL RED CARD                  
###############################################################

@dp.message(CommandStart())
async def start(message: types.Message, state: FSMContext):
    await state.clear()

    hero = (
        "<b>🚀 DC Link | Електронна черга постачальників</b>\n"
        "Працюємо у корпоративному стилі: швидко, чітко, без зайвого шуму.\n\n"
        "• Створіть заявку за лічені кроки\n"
        "• Отримуйте статуси та рішення\n"
        "• Керуйте останніми заявками прямо з бота"
    )

    await message.answer(hero, reply_markup=navigation_keyboard(include_back=False))
    await message.answer(
        "Готові працювати? Оберіть розділ нижче:", reply_markup=main_menu()
    )


###############################################################
#                     MAIN MENU HANDLERS                      
###############################################################

@dp.callback_query(F.data == "menu_new")
async def menu_new(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()

    await callback.message.answer(
        "📦 Введіть назву постачальника:",
        reply_markup=navigation_keyboard(include_back=False)
    )

    await state.set_state(QueueForm.supplier)


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
        f"🚚 <b>Авто:</b> {req.car}\n"
        f"🧱 <b>Тип завантаження:</b> {req.loading_type}\n"
        f"📅 <b>План:</b> {planned_date} {planned_time}\n"
        f"✅ <b>Підтверджено:</b> {req.date.strftime('%d.%m.%Y')} {req.time}\n"
        f"🏁 <b>Статус завершення:</b> {final_status}"
    )


def build_recent_request_ids(reqs: list[Request]) -> set[int]:
    return {req.id for req in reqs}


def set_updated_now(req: Request):
    req.updated_at = kyiv_now_naive()


def get_confirmed_datetime(req: Request) -> datetime | None:
    if not req.date or not req.time:
        return None
    try:
        hour, minute = [int(x) for x in req.time.split(":")[:2]]
        return datetime.combine(req.date, dtime(hour=hour, minute=minute), tzinfo=KYIV_TZ)
    except Exception:
        return None


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
        and req.status != "deleted_by_user"
        and not req.completed_at
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

    if req.status == "deleted_by_user":
        return await callback.answer("Заявка вже видалена", show_alert=True)

    if req.completed_at:
        return await callback.answer("Заявка вже завершена, зміни неможливі", show_alert=True)

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

        if req.completed_at:
            await state.clear()
            return await message.answer("Заявка вже завершена, зміни неможливі.")

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

    if req.status == "deleted_by_user":
        return await callback.answer("Заявка вже видалена", show_alert=True)

    if req.completed_at:
        return await callback.answer("Заявка вже завершена, редагування неможливе", show_alert=True)

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
    kb.button(text="🚚 Авто", callback_data="edit_field_car")
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

        if req.completed_at:
            await state.clear()
            return await message.answer("Заявка вже завершена, редагування неможливе.")

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

    if not req or req.user_id != user_id or req.completed_at:
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
        text=f"Поле 'Авто' оновлено для заявки #{req.id}.",
        changes=[("Авто", old_value, req.car)],
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
        "car": (UserEditForm.car, "Введіть нову марку і номер авто:"),
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
        await state.set_state(UserEditForm.calendar)
        await callback.message.answer(
            "Оберіть нову дату:",
            reply_markup=build_date_calendar(back_callback="edit_back_to_choice"),
        )
    else:
        await callback.message.answer("Невідомий вибір.")

    await callback.answer()

@dp.callback_query(UserEditForm.calendar, F.data.startswith("prev_"))
async def user_edit_prev(callback: types.CallbackQuery, state: FSMContext):
    _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(
        reply_markup=build_date_calendar(int(y), int(m), back_callback="edit_back_to_choice")
    )
    await callback.answer()


@dp.callback_query(UserEditForm.calendar, F.data.startswith("next_"))
async def user_edit_next(callback: types.CallbackQuery, state: FSMContext):
    _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(
        reply_markup=build_date_calendar(int(y), int(m), back_callback="edit_back_to_choice")
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

    await state.update_data(new_date=chosen)

    kb = InlineKeyboardBuilder()
    for hour in range(24):
        kb.button(text=f"{hour:02d}", callback_data=f"uhour_{hour:02d}")
    kb.adjust(6)

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

    if chosen_date:
        markup = build_date_calendar(
            chosen_date.year, chosen_date.month, back_callback="edit_back_to_choice"
        )
    else:
        markup = build_date_calendar(back_callback="edit_back_to_choice")

    await state.set_state(UserEditForm.calendar)
    await callback.message.answer("Оберіть нову дату:", reply_markup=markup)
    await callback.answer()


@dp.callback_query(UserEditForm.hour, F.data.startswith("uhour_"))
async def user_edit_hour(callback: types.CallbackQuery, state: FSMContext):
    hour = callback.data.replace("uhour_", "")
    await state.update_data(new_hour=hour)

    kb = InlineKeyboardBuilder()
    for m in range(0, 60, 5):
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
    kb = InlineKeyboardBuilder()
    for hour in range(24):
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

    req, reason = await _load_request_for_edit(state, callback.from_user.id)
    if not req:
        return await callback.answer("Заявка не знайдена", show_alert=True)

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
        status = "🟢 NEW" if r.status == "new" else f"⚪ {get_status_label(r.status)}"
        text += (
            f"• <b>#{r.id}</b>  "
            f"{r.date.strftime('%d.%m.%Y')} {r.time}  —  {status}\n"
        )
        kb.button(
            text=f"#{r.id} — {r.date.strftime('%d.%m.%Y')} {r.time} ({r.status})",
            callback_data=f"admin_view_{r.id}"
        )

    kb.button(text=MAIN_MENU_TEXT, callback_data="go_main")
    kb.adjust(1)

    await callback.message.answer(text, reply_markup=kb.as_markup())


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
        f"🚚 <b>Авто:</b> {req.car}\n"
        f"🧱 <b>Тип завантаження:</b> {req.loading_type}\n"
        f"📅 <b>План:</b> {plan_date} {plan_time}\n"
        f"✅ <b>Підтверджено:</b> {req.date.strftime('%d.%m.%Y')} {req.time}\n"
        f"🏁 <b>Завершення:</b> {final_status}"
    )
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

        session.add(Admin(telegram_id=tg_id, is_superadmin=False))
        await session.commit()

    await state.clear()
    await message.answer(
        f"✔ Користувач <code>{tg_id}</code> доданий як адміністратор.",
        reply_markup=navigation_keyboard(include_back=False)
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
        await session.execute(delete(Admin).where(Admin.telegram_id == tg_id))
        await session.commit()

    await state.clear()
    await message.answer(
        f"🗑 Адміністратора <code>{tg_id}</code> видалено.",
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
        "📞 <b>Крок 2/5</b>\nЗалиште контактний номер телефону:",
        reply_markup=navigation_keyboard()
    )
    await state.set_state(QueueForm.phone)


@dp.message(QueueForm.phone)
async def step_phone(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.supplier)
        return await message.answer(
            "🏢 <b>Крок 1/5</b>\nВкажіть назву постачальника:",
            reply_markup=navigation_keyboard(include_back=False)
        )

    phone = message.text.strip()
    if not phone:
        return await message.answer("⚠️ Вкажіть номер телефону для зв'язку.")

    await state.update_data(phone=phone)

    await message.answer(
        "🚚 <b>Крок 3/5</b>\nВведіть марку та номер авто:",
        reply_markup=navigation_keyboard()
    )
    await state.set_state(QueueForm.car)


@dp.message(QueueForm.car)
async def step_car(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.phone)
        return await message.answer(
            "📞 <b>Крок 2/5</b>\nЗалиште контактний номер телефону:",
            reply_markup=navigation_keyboard(),
        )

    car = message.text.strip()
    if not car:
        return await message.answer("⚠️ Вкажіть марку та номер авто.")

    await state.update_data(car=car)

    kb = InlineKeyboardBuilder()
    kb.button(text="🚚 На палетах", callback_data="type_pal")
    kb.button(text="📦 В розсип", callback_data="type_loose")
    kb.adjust(1)

    await message.answer(
        "⚙️ <b>Крок 4/5</b>\nОберіть тип завантаження:",
        reply_markup=add_inline_navigation(kb, back_callback="back_to_car").as_markup(),
    )

    await state.set_state(QueueForm.loading_type)


@dp.callback_query(QueueForm.loading_type, F.data == "back_to_car")
async def loading_back(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(QueueForm.car)
    await callback.message.answer(
        "🚚 <b>Крок 3/5</b>\nВведіть марку та номер авто:",
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

    await state.update_data(loading_type=t)

    await callback.message.answer(
        "📅 <b>Крок 5/5</b>\nОберіть дату та час візиту:",
        reply_markup=build_date_calendar(back_callback="back_to_loading")
    )

    await state.set_state(QueueForm.calendar)


###############################################################
#                INLINE CALENDAR GENERATOR                    
###############################################################

def build_date_calendar(year=None, month=None, back_callback: str | None = None):
    now = kyiv_now()
    today = now.date()
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

    # Дни недели
    kb.row(*[
        InlineKeyboardButton(text=d, callback_data="ignore")
        for d in ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]
    ])

    # День недели, с которого начинается месяц
    first_wday = datetime(year, month, 1, tzinfo=KYIV_TZ).weekday()  # Monday = 0

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
            row.append(InlineKeyboardButton(text=str(d), callback_data="ignore"))
        else:
            row.append(
                InlineKeyboardButton(text=str(d), callback_data=f"day_{year}_{month}_{d}")
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


###############################################################
#        DRIVER — DATE / HOUR / MINUTE SELECTION              
###############################################################

@dp.callback_query(QueueForm.calendar, F.data.startswith("prev_"))
async def cal_prev(callback: types.CallbackQuery):
    _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(
        reply_markup=build_date_calendar(int(y), int(m), back_callback="back_to_loading")
    )


@dp.callback_query(QueueForm.calendar, F.data.startswith("next_"))
async def cal_next(callback: types.CallbackQuery):
    _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(
        reply_markup=build_date_calendar(int(y), int(m), back_callback="back_to_loading")
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

    if chosen < kyiv_now().date():
        return await callback.answer("Не можна обирати минулі дати", show_alert=True)

    await state.update_data(date=chosen)

    kb = InlineKeyboardBuilder()
    for hour in range(9, 17):
        kb.button(text=f"{hour:02d}", callback_data=f"hour_{hour:02d}")
    kb.adjust(6)

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

    if chosen_date:
        markup = build_date_calendar(
            chosen_date.year,
            chosen_date.month,
            back_callback="back_to_loading"
        )
    else:
        markup = build_date_calendar(back_callback="back_to_loading")

    await state.set_state(QueueForm.calendar)
    await callback.message.answer(
        "📅 <b>Крок 5/5</b>\nОберіть дату та час візиту:", reply_markup=markup
    )
    await callback.answer()


@dp.callback_query(QueueForm.hour, F.data.startswith("hour_"))
async def hour_selected(callback: types.CallbackQuery, state: FSMContext):
    hour = callback.data.replace("hour_", "")
    await state.update_data(hour=hour)

    kb = InlineKeyboardBuilder()
    minutes = [0] if hour == "16" else list(range(0, 60, 5))
    for m in minutes:
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

    if not chosen_date or chosen_date < kyiv_now().date():
        return await callback.answer("Оберіть доступну дату", show_alert=True)

    if chosen_hour is None:
        return await callback.answer("Спочатку оберіть годину", show_alert=True)

    selected_time = dtime(hour=int(chosen_hour), minute=int(minute))
    if not (dtime(hour=9) <= selected_time <= dtime(hour=16)):
        return await callback.answer("Доступний час з 09:00 до 16:00", show_alert=True)

    async with SessionLocal() as session:
        req = Request(
            user_id=callback.from_user.id,
            supplier=data["supplier"],
            phone=data["phone"],
            car=data["car"],
            loading_type=data["loading_type"],
            planned_date=chosen_date,
            planned_time=f"{int(chosen_hour):02d}:{int(minute):02d}",
            date=chosen_date,
            time=f"{int(chosen_hour):02d}:{int(minute):02d}",
            status="new",
            created_at=kyiv_now_naive(),
            updated_at=kyiv_now_naive(),
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
    kb = InlineKeyboardBuilder()
    for hour in range(24):
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
        f"🚚 <b>Авто:</b> {req.car}\n"
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
async def adm_rej(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[2])

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        req.status = "rejected"
        req.admin_id = callback.from_user.id
        set_updated_now(req)
        await session.commit()

    await callback.message.answer("❌ Відхилено!")

    await sheet_client.sync_request(req)

    await bot.send_message(
        req.user_id,
        f"❌ <b>Заявку #{req.id} відхилено адміністратором.</b>"
    )

    await notify_admins_about_action(req, "відхилена")


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

    await state.update_data(new_date=chosen_date)

    kb = InlineKeyboardBuilder()
    for h in range(9, 17):
        kb.button(text=f"{h:02d}", callback_data=f"ach_hour_{h:02d}")
    kb.adjust(6)

    await callback.message.answer(
        "⏰ Оберіть годину:",
        reply_markup=add_inline_navigation(kb, back_callback="admin_back_to_calendar").as_markup()
    )
    await state.set_state(AdminChangeForm.hour)


@dp.callback_query(AdminChangeForm.hour, F.data.startswith("ach_hour_"))
async def adm_hour(callback: types.CallbackQuery, state: FSMContext):
    hour = callback.data.replace("ach_hour_", "")
    await state.update_data(new_hour=hour)

    kb = InlineKeyboardBuilder()
    minutes = [0] if hour == "16" else list(range(0, 60, 5))
    for m in minutes:
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
    kb = InlineKeyboardBuilder()
    for h in range(9, 17):
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
    new_time = f"{int(data['new_hour']):02d}:{int(minute):02d}"

    if new_date < kyiv_now().date():
        return await callback.answer("Дата не може бути в минулому", show_alert=True)

    chosen_time = dtime(hour=int(data["new_hour"]), minute=int(minute))
    if not (dtime(hour=9) <= chosen_time <= dtime(hour=16)):
        return await callback.answer("Доступний час з 09:00 до 16:00", show_alert=True)

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        req.date = new_date
        req.time = new_time
        req.status = "approved"
        req.admin_id = callback.from_user.id
        set_updated_now(req)
        await session.commit()

    await callback.message.answer("🔁 Дата/час успішно змінені!")

    await sheet_client.sync_request(req)

    # Уведомление водителю
    await bot.send_message(
        req.user_id,
        f"🔄 <b>Час вашої заявки #{req.id} змінено:</b>\n"
        f"📅 {req.date.strftime('%d.%m.%Y')}  ⏰ {req.time}"
    )

    # Уведомить всех админов
    await notify_admins_about_action(req, "змінена (дата/час)")

    await state.clear()


###############################################################
#        BROADCAST ACTION TO ALL ADMINS (Uniﬁed Function)     
###############################################################

async def notify_admins_about_action(req: Request, action: str):
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

    for a in admins:
        try:
            await bot.send_message(a.telegram_id, text)
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
        approved_at = req.updated_at or req.created_at
        if not approved_at:
            continue

        if approved_at.tzinfo is None:
            approved_at = approved_at.replace(tzinfo=KYIV_TZ)
            
        if now >= approved_at + timedelta(hours=20):
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
            session.add(Admin(telegram_id=SUPERADMIN_ID, is_superadmin=True))
            await session.commit()

    asyncio.create_task(auto_close_overdue_requests())
    print("Bot started!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
