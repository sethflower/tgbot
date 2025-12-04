###############################################################
#                      DRIVER QUEUE BOT                       
#       Aiogram 3 • Railway Hosting • PostgreSQL (async)      
#                     FULL PROFESSIONAL EDITION               
###############################################################

import os
import asyncio
import logging
from datetime import datetime, date, timedelta

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
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
    Date, Text, TIMESTAMP, select, delete
)

from dotenv import load_dotenv


###############################################################
#                    LOAD ENVIRONMENT                         
###############################################################

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPERADMIN_ID = int(os.getenv("SUPERADMIN_ID"))
DATABASE_URL = os.getenv("DATABASE_URL")

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
    loading_type = Column(Text)

    date = Column(Date)
    time = Column(Text)

    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    status = Column(String, default="new")
    admin_id = Column(BigInteger, nullable=True)


engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


###############################################################
#                     CONSTANTS & MENUS                       
###############################################################

BACK_TEXT = "⬅️ Назад"
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
    await message.answer("🏠 Ви у головному меню.", reply_markup=ReplyKeyboardRemove())
    await message.answer("Оберіть дію:", reply_markup=main_menu())


@dp.message(F.text == MAIN_MENU_TEXT)
async def handle_main_menu(message: types.Message, state: FSMContext):
    await show_main_menu(message, state)

@dp.callback_query(F.data == "go_main")
async def handle_main_menu_callback(callback: types.CallbackQuery, state: FSMContext):
    await show_main_menu(callback.message, state)
    await callback.answer()

def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="▶️ Створити заявку", callback_data="menu_new")
    kb.button(text="📋 Мій список заявок", callback_data="menu_my")
    kb.button(text="⚙️ Адмін-панель", callback_data="menu_admin")
    kb.adjust(1)
    return kb.as_markup()

def admin_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="🆕 Нові заявки", callback_data="admin_new")
    kb.button(text="📚 Усі заявки", callback_data="admin_all")
    kb.button(text="➕ Додати адміна", callback_data="admin_add")
    kb.button(text="➖ Видалити адміна", callback_data="admin_remove")
    kb.button(text="🗑 Очистити БД", callback_data="admin_clear")
    kb.adjust(1)
    return kb.as_markup()


###############################################################
#                        FSM STATES                           
###############################################################

class QueueForm(StatesGroup):
    supplier = State()
    driver_name = State()
    phone = State()
    car = State()
    docs = State()
    loading_type = State()
    calendar = State()
    hour = State()
    minute = State()

class AdminAdd(StatesGroup):
    wait_id = State()

class AdminRemove(StatesGroup):
    wait_id = State()

class AdminChangeForm(StatesGroup):
    calendar = State()
    hour = State()
    minute = State()
###############################################################
#                 START → BEAUTIFUL RED CARD                  
###############################################################

@dp.message(CommandStart())
async def start(message: types.Message, state: FSMContext):
    await state.clear()

    text = (
        "🟥 <b>DC Link — Електронна черга постачальників</b>\n\n"
        "👋 Вітаємо у електронній черзі постачальників\n"
        "Цей бот допоможе створити заявку на вивантаження.\n\n"
        "Натисніть кнопку нижче, щоб почати."
    )

    await message.answer(text, reply_markup=main_menu())


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
        return await callback.message.answer("У вас немає заявок.")

    text = "<b>📋 Ваші останні 3 заявки:</b>\n\n"
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
        "new": "🟢 Нова",
        "approved": "✔ Підтверджена",
        "rejected": "❌ Відхилена",
        "deleted_by_user": "⛔ Видалена користувачем",
    }.get(status, status)


def format_request_text(req: Request) -> str:
    status = get_status_label(req.status)
    return (
        f"<b>📄 Заявка #{req.id}</b>\n"
        f"Статус: {status}\n\n"
        f"🏢 <b>Постачальник:</b> {req.supplier}\n"
        f"👤 <b>Водій:</b> {req.driver_name}\n"
        f"📞 <b>Телефон:</b> {req.phone}\n"
        f"🚚 <b>Авто:</b> {req.car}\n"
        f"🧱 <b>Тип завантаження:</b> {req.loading_type}\n"
        f"📅 <b>Дата:</b> {req.date.strftime('%d.%m.%Y')}\n"
        f"⏰ <b>Час:</b> {req.time}"
    )


def build_recent_request_ids(reqs: list[Request]) -> set[int]:
    return {req.id for req in reqs}


async def send_request_details(
    req: Request,
    callback_or_message: types.CallbackQuery | types.Message,
    *,
    allow_actions: bool,
    recent_ids: set[int] | None = None,
):
    kb = InlineKeyboardBuilder()
    if allow_actions and req.id in (recent_ids or set()) and req.status != "deleted_by_user":
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

    if req.docs_file_id:
        await target_message.answer_photo(req.docs_file_id, caption=text, reply_markup=kb.as_markup())
    else:
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

    await state.set_state(UserDeleteForm.reason)
    await state.update_data(req_id=req_id)
    await callback.message.answer(
        "Вкажіть причину видалення заявки:", reply_markup=navigation_keyboard(include_back=False)
    )
    await callback.answer()


async def notify_admins_about_user_deletion(req: Request, reason: str):
    async with SessionLocal() as session:
        admins = (await session.execute(select(Admin))).scalars().all()

    text = (
        f"❗ Поставщик {req.supplier} видалив заявку #{req.id}\n"
        f"Причина: {reason}\n\n"
        f"📄 Дані заявки до видалення:\n"
        f"👤 {req.driver_name}\n"
        f"📞 {req.phone}\n"
        f"🚚 {req.car}\n"
        f"🧱 {req.loading_type}\n"
        f"📅 {req.date.strftime('%d.%m.%Y')} ⏰ {req.time}"
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

        req.status = "deleted_by_user"
        await session.commit()

    await notify_admins_about_user_deletion(req, reason)
    await message.answer("Заявку видалено. Адміністратори отримали повідомлення.")
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

    await state.set_state(UserEditForm.reason)
    await state.update_data(req_id=req_id)
    await callback.message.answer(
        "Вкажіть причину зміни заявки:", reply_markup=navigation_keyboard(include_back=False)
    )
    await callback.answer()


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

    await state.update_data(reason=reason)
    await state.set_state(UserEditForm.calendar)
    await message.answer(
        "Оберіть нову дату:",
        reply_markup=build_date_calendar()
    )


@dp.callback_query(UserEditForm.calendar, F.data.startswith("prev_"))
async def user_edit_prev(callback: types.CallbackQuery, state: FSMContext):
    _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(reply_markup=build_date_calendar(int(y), int(m)))
    await callback.answer()


@dp.callback_query(UserEditForm.calendar, F.data.startswith("next_"))
async def user_edit_next(callback: types.CallbackQuery, state: FSMContext):
    _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(reply_markup=build_date_calendar(int(y), int(m)))
    await callback.answer()


@dp.callback_query(UserEditForm.calendar, F.data == "close_calendar")
async def user_edit_cancel_calendar(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("Зміну заявки скасовано.")
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
        markup = build_date_calendar(chosen_date.year, chosen_date.month)
    else:
        markup = build_date_calendar()

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

    req_id = data.get("req_id")
    reason = data.get("reason")
    new_date: date = data.get("new_date")
    new_time = f"{data['new_hour']}:{minute}"

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req or req.user_id != callback.from_user.id:
            await state.clear()
            return await callback.answer("Заявка не знайдена", show_alert=True)

        req.date = new_date
        req.time = new_time
        req.status = "new"
        req.admin_id = None
        await session.commit()

    await callback.message.answer(
        f"Запит на зміну заявки #{req.id} відправлено адміністратору.\n"
        f"📅 {req.date.strftime('%d.%m.%Y')} ⏰ {req.time}"
    )

    await notify_admins_about_user_edit(req, reason)
    await state.clear()
    await callback.answer()


###############################################################
#                     ADMIN PANEL ACCESS                      
###############################################################

@dp.callback_query(F.data == "menu_admin")
async def menu_admin_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    if user_id == SUPERADMIN_ID:
        return await callback.message.answer("⚙️ <b>Адмін-панель:</b>", reply_markup=admin_menu())

    async with SessionLocal() as session:
        res = await session.execute(select(Admin).where(Admin.telegram_id == user_id))
        admin = res.scalar_one_or_none()

    if not admin:
        return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)

    await callback.message.answer("⚙️ <b>Адмін-панель:</b>", reply_markup=admin_menu())


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
        return await callback.message.answer("🟢 Немає нових заявок.")

    text = "<b>🆕 Нові заявки:</b>\n\n"
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
        return await callback.message.answer("⚪ Немає заявок в базі.")

    text = "<b>📚 Останні 20 заявок:</b>\n\n"
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


@dp.callback_query(F.data.startswith("admin_view_"))
async def admin_view(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[2])

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)

    if not req:
        return await callback.answer("Заявка не знайдена", show_alert=True)

    status = get_status_label(req.status)

    text = (
        f"<b>📄 Заявка #{req.id}</b>\n"
        f"Статус: {status}\n\n"
        f"🏢 <b>Постачальник:</b> {req.supplier}\n"
        f"👤 <b>Водій:</b> {req.driver_name}\n"
        f"📞 <b>Телефон:</b> {req.phone}\n"
        f"🚚 <b>Авто:</b> {req.car}\n"
        f"🧱 <b>Тип завантаження:</b> {req.loading_type}\n"
        f"📅 <b>Дата:</b> {req.date.strftime('%d.%m.%Y')}\n"
        f"⏰ <b>Час:</b> {req.time}"
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="✔ Підтвердити", callback_data=f"adm_ok_{req.id}")
    kb.button(text="🔁 Змінити дату/час", callback_data=f"adm_change_{req.id}")
    kb.button(text="❌ Відхилити", callback_data=f"adm_rej_{req.id}")
    kb.button(text="⬅️ До списку", callback_data="admin_all")
    kb.adjust(1)

    if req.docs_file_id:
        await callback.message.answer_photo(
            req.docs_file_id,
            caption=text,
            reply_markup=kb.as_markup(),
        )
    else:
        await callback.message.answer(text, reply_markup=kb.as_markup())

    await callback.answer()
###############################################################
#             ADMIN — ADD ADMIN (FSM Aiogram 3 OK)            
###############################################################

@dp.callback_query(F.data == "admin_add")
async def admin_add(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "➕ Введіть Telegram ID користувача:",
        reply_markup=navigation_keyboard()
    )
    await state.set_state(AdminAdd.wait_id)


@dp.message(AdminAdd.wait_id)
async def admin_add_wait(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.clear()
        await message.answer("Скасовано.", reply_markup=ReplyKeyboardRemove())
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
        reply_markup=ReplyKeyboardRemove()
    )


###############################################################
#           ADMIN — REMOVE ADMIN (FSM Aiogram 3 OK)           
###############################################################

@dp.callback_query(F.data == "admin_remove")
async def admin_remove(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "➖ Введіть Telegram ID адміністратора для видалення:",
        reply_markup=navigation_keyboard()
    )
    await state.set_state(AdminRemove.wait_id)


@dp.message(AdminRemove.wait_id)
async def admin_remove_wait(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.clear()
        await message.answer("Скасовано.", reply_markup=ReplyKeyboardRemove())
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
        reply_markup=ReplyKeyboardRemove()
    )


###############################################################
#                ADMIN — CLEAR DATABASE                      
###############################################################

@dp.callback_query(F.data == "admin_clear")
async def admin_clear(callback: types.CallbackQuery):

    if callback.from_user.id != SUPERADMIN_ID:
        return await callback.answer("⛔ Тільки суперадмін!", show_alert=True)

    kb = InlineKeyboardBuilder()
    kb.button(text="🗑 Видалити всі заявки", callback_data="admin_clear_yes")
    kb.button(text="❌ Скасувати", callback_data="admin_clear_no")
    kb.adjust(1)

    await callback.message.answer(
        "⚠️ Ви впевнені, що хочете видалити всі заявки?",
        reply_markup=kb.as_markup()
    )


@dp.callback_query(F.data == "admin_clear_yes")
async def admin_clear_yes(callback: types.CallbackQuery):
    async with SessionLocal() as session:
        await session.execute(delete(Request))
        await session.commit()

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
        return await message.answer("Ви на початку анкети. Користуйтеся кнопками нижче.")
        
    supplier = message.text.strip()

    if not supplier:
        return await message.answer("⚠ Введіть постачальника.")

    await state.update_data(supplier=supplier)

    await message.answer(
        "🚛 Введіть ПІБ водія:",
        reply_markup=navigation_keyboard()
    )
    await state.set_state(QueueForm.driver_name)


@dp.message(QueueForm.driver_name)
async def step_driver_name(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.supplier)
        return await message.answer(
            "📦 Введіть постачальника:",
            reply_markup=navigation_keyboard(include_back=False)
        )

    name = message.text.strip()
    if not name:
        return await message.answer("⚠ Введіть ПІБ водія.")

    await state.update_data(driver_name=name)

    await message.answer("📞 Введіть номер телефону:", reply_markup=navigation_keyboard())
    await state.set_state(QueueForm.phone)


@dp.message(QueueForm.phone)
async def step_phone(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.driver_name)
        return await message.answer(
            "🚛 Введіть ПІБ водія:",
            reply_markup=navigation_keyboard()
        )

    phone = message.text.strip()
    if not phone:
        return await message.answer("⚠ Введіть номер телефону.")

    await state.update_data(phone=phone)

    await message.answer("🚚 Введіть марку і номер авто:", reply_markup=navigation_keyboard())
    await state.set_state(QueueForm.car)


@dp.message(QueueForm.car)
async def step_car(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.phone)
        return await message.answer(
            "📞 Введіть номер телефону:",
            reply_markup=navigation_keyboard()
        )

    car = message.text.strip()
    if not car:
        return await message.answer("⚠ Введіть марку та номер авто.")

    await state.update_data(car=car)

    kb = InlineKeyboardBuilder()
    kb.button(text="📸 Завантажити фото документів", callback_data="photo_upload")
    kb.button(text="⏭ Пропустити", callback_data="photo_skip")
    kb.adjust(1)

    await message.answer(
        "📋 Завантажте фото документів або пропустіть:",
        reply_markup=add_inline_navigation(kb, back_callback="back_to_car").as_markup()
    )

    await state.set_state(QueueForm.docs)
###############################################################
#                DOCUMENT UPLOAD (Aiogram 3 OK)               
###############################################################

@dp.callback_query(QueueForm.docs, F.data == "photo_upload")
async def photo_upload(callback: types.CallbackQuery):
    await callback.message.answer("📸 Надішліть фото документів.")

@dp.callback_query(QueueForm.docs, F.data == "back_to_car")
async def back_to_car(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(QueueForm.car)
    await callback.message.answer(
        "🚚 Введіть марку і номер авто:",
        reply_markup=navigation_keyboard()
    )
    await callback.answer()


@dp.message(QueueForm.docs, F.text == BACK_TEXT)
async def docs_back(message: types.Message, state: FSMContext):
    await state.set_state(QueueForm.car)
    await message.answer(
        "🚚 Введіть марку і номер авто:",
        reply_markup=navigation_keyboard()
    )



@dp.message(QueueForm.docs, F.photo)
async def photo_received(message: types.Message, state: FSMContext):
    file_id = message.photo[-1].file_id
    await state.update_data(docs_file_id=file_id)

    kb = InlineKeyboardBuilder()
    kb.button(text="⏭ Далі", callback_data="photo_done")
    kb.adjust(1)

    await message.answer(
        "Фото збережено.",
        reply_markup=add_inline_navigation(kb, back_callback="back_to_car").as_markup()
    )


@dp.callback_query(QueueForm.docs, F.data == "photo_skip")
@dp.callback_query(QueueForm.docs, F.data == "photo_done")
async def photo_done(callback: types.CallbackQuery, state: FSMContext):

    kb = InlineKeyboardBuilder()
    kb.button(text="🚚 На палетах", callback_data="type_pal")
    kb.button(text="📦 В розсип", callback_data="type_loose")
    kb.adjust(1)

    await callback.message.answer(
        "😉 Оберіть тип завантаження:",
        reply_markup=add_inline_navigation(kb, back_callback="back_to_docs").as_markup()
    )

    await state.set_state(QueueForm.loading_type)

@dp.callback_query(QueueForm.loading_type, F.data == "back_to_docs")
async def loading_back(callback: types.CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder()
    kb.button(text="📸 Завантажити документи", callback_data="photo_upload")
    kb.button(text="⏭ Пропустити", callback_data="photo_skip")
    kb.adjust(1)

    await state.set_state(QueueForm.docs)
    await callback.message.answer(
        "📋 Завантажте документи або пропустіть:",
        reply_markup=add_inline_navigation(kb, back_callback="back_to_car").as_markup()
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
        "🔹 Оберіть дату:",
        reply_markup=build_date_calendar(back_callback="back_to_loading")
    )

    await state.set_state(QueueForm.calendar)


###############################################################
#                INLINE CALENDAR GENERATOR                    
###############################################################

def build_date_calendar(year=None, month=None, back_callback: str | None = None):
    now = datetime.now()
    year = year or now.year
    month = month or now.month

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
    first_wday = datetime(year, month, 1).weekday()  # Monday = 0

    row = []
    for _ in range(first_wday):
        row.append(InlineKeyboardButton(text=" ", callback_data="ignore"))
    if row:
        kb.row(*row)

    # Количество дней
    next_month = month + 1 if month < 12 else 1
    next_year = year + 1 if month == 12 else year
    days_in_month = (datetime(next_year, next_month, 1) - timedelta(days=1)).day

    row = []
    for d in range(1, days_in_month + 1):
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

    kb.row(
        InlineKeyboardButton(text="⬅", callback_data=f"prev_{prev_y}_{prev_m}"),
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
        reply_markup=add_inline_navigation(kb, back_callback="back_to_docs").as_markup()
    )
    await callback.answer()

@dp.callback_query(QueueForm.calendar, F.data.startswith("day_"))
async def cal_day(callback: types.CallbackQuery, state: FSMContext):
    _, y, m, d = callback.data.split("_")
    chosen = date(int(y), int(m), int(d))

    await state.update_data(date=chosen)

    kb = InlineKeyboardBuilder()
    for hour in range(24):
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
    await callback.message.answer("🔹 Оберіть дату:", reply_markup=markup)
    await callback.answer()


@dp.callback_query(QueueForm.hour, F.data.startswith("hour_"))
async def hour_selected(callback: types.CallbackQuery, state: FSMContext):
    hour = callback.data.replace("hour_", "")
    await state.update_data(hour=hour)

    kb = InlineKeyboardBuilder()
    for m in range(0, 60, 5):
        kb.button(text=f"{m:02d}", callback_data=f"min_{m:02d}")
    kb.adjust(6)

    await callback.message.answer(
        "🕒 Оберіть хвилини:",
        reply_markup=add_inline_navigation(kb, back_callback="back_to_hour").as_markup()
    )
    await state.set_state(QueueForm.minute)


@dp.callback_query(QueueForm.minute, F.data.startswith("min_"))
async def minute_selected(callback: types.CallbackQuery, state: FSMContext):

    minute = callback.data.replace("min_", "")
    data = await state.get_data()

    async with SessionLocal() as session:
        req = Request(
            user_id=callback.from_user.id,
            supplier=data["supplier"],
            driver_name=data["driver_name"],
            phone=data["phone"],
            car=data["car"],
            docs_file_id=data.get("docs_file_id"),
            loading_type=data["loading_type"],
            date=data["date"],
            time=f"{data['hour']}:{minute}",
            status="new",
            created_at=datetime.utcnow()
        )

        session.add(req)
        await session.commit()
        await session.refresh(req)

    await callback.message.answer(
        f"✅ Заявку #{req.id} відправлено адміністратору!\n"
        f"📅 {req.date.strftime('%d.%m.%Y')} ⏰ {req.time}"
    )

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
        f"<b>📦 Нова заявка #{req.id}</b>\n\n"
        f"🏢 <b>Постачальник:</b> {req.supplier}\n"
        f"👤 <b>Водій:</b> {req.driver_name}\n"
        f"📞 <b>Телефон:</b> {req.phone}\n"
        f"🚚 <b>Авто:</b> {req.car}\n"
        f"🧱 <b>Тип:</b> {req.loading_type}\n"
        f"📅 <b>Дата:</b> {req.date.strftime('%d.%m.%Y')}\n"
        f"⏰ <b>Час:</b> {req.time}\n"
    )

    for admin in admins:
        kb = InlineKeyboardBuilder()
        kb.button(text="✔ Підтвердити", callback_data=f"adm_ok_{req.id}")
        kb.button(text="🔁 Змінити дату/час", callback_data=f"adm_change_{req.id}")
        kb.button(text="❌ Відхилити", callback_data=f"adm_rej_{req.id}")
        kb.adjust(1)

        try:
            await bot.send_message(admin.telegram_id, text, reply_markup=kb.as_markup())
            if req.docs_file_id:
                await bot.send_photo(admin.telegram_id, req.docs_file_id)
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
        await session.commit()

    await callback.message.answer("✔ Підтверджено!")

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
        await session.commit()

    await callback.message.answer("❌ Відхилено!")

    await bot.send_message(
        req.user_id,
        f"❌ <b>Заявку #{req.id} відхилено адміністратором.</b>"
    )

    await notify_admins_about_action(req, "відхилена")


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
    await callback.message.answer(
        "Операцію зміни дати/часу скасовано.",
        reply_markup=admin_menu()
    )
    await callback.answer()


@dp.callback_query(AdminChangeForm.calendar, F.data.startswith("day_"))
async def adm_cal_day(callback: types.CallbackQuery, state: FSMContext):
    _, y, m, d = callback.data.split("_")
    chosen_date = date(int(y), int(m), int(d))

    await state.update_data(new_date=chosen_date)

    kb = InlineKeyboardBuilder()
    for h in range(24):
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
    for m in range(0, 60, 5):
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
    for h in range(24):
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
    new_time = f"{data['new_hour']}:{minute}"

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        req.date = new_date
        req.time = new_time
        req.admin_id = callback.from_user.id
        await session.commit()

    await callback.message.answer("🔁 Дата/час успішно змінені!")

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

    text = (
        f"ℹ️ <b>Заявка #{req.id} {action}</b>\n\n"
        f"📅 {req.date.strftime('%d.%m.%Y')}  ⏰ {req.time}\n"
        f"👤 {req.driver_name}\n"
        f"🏢 {req.supplier}"
    )

    for a in admins:
        try:
            await bot.send_message(a.telegram_id, text)
        except:
            pass

async def notify_admins_about_user_edit(req: Request, reason: str):
    async with SessionLocal() as session:
        admins = (await session.execute(select(Admin))).scalars().all()

    text = (
        f"ℹ️ Поставщик {req.supplier} змінив заявку #{req.id}\n"
        f"Причина: {reason}\n\n"
        f"Потрібно повторно підтвердити/відхилити або скоригувати дату чи час.\n"
        f"📅 {req.date.strftime('%d.%m.%Y')} ⏰ {req.time}\n"
        f"👤 {req.driver_name} — {req.phone}"
    )

    for admin in admins:
        try:
            await bot.send_message(admin.telegram_id, text)
        except:
            pass
            
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

    print("Bot started!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
