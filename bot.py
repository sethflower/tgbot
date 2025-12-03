###############################################################
#                      DRIVER QUEUE BOT                       #
#       Aiogram 3 • Railway Hosting • PostgreSQL (async)      #
#                     FULL PROFESSIONAL EDITION               #
#            Красивый интерфейс • Меню • Админ-панель         #
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
    InlineKeyboardMarkup,
    InlineKeyboardButton
)

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
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

# Чиним неправильные префиксы postgres://
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

BACK_TEXT = "⬅ Назад"

def back_keyboard(enabled=True):
    if not enabled:
        return ReplyKeyboardRemove()
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BACK_TEXT)]],
        resize_keyboard=True
    )

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
        "🟥 <b>DC Link — Електронна черга водіїв</b>\n\n"
        "👋 Вітаємо у електронній черзі водіїв DC Link!\n"
        "Цей бот допоможе створити заявку на вивантаження.\n\n"
        "Натисніть кнопку нижче щоб почати."
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="▶️ Створити заявку", callback_data="menu_new")
    kb.button(text="📋 Мій список заявок", callback_data="menu_my")
    kb.button(text="⚙️ Адмін-панель", callback_data="menu_admin")
    kb.adjust(1)

    await message.answer(text, reply_markup=kb.as_markup())


###############################################################
#                     MAIN MENU HANDLERS
###############################################################

@dp.callback_query(F.data == "menu_new")
async def menu_new(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer(
        "🔹 Введіть постачальника:",
        reply_markup=back_keyboard(False)
    )
    await state.set_state(QueueForm.supplier)


@dp.callback_query(F.data == "menu_my")
async def menu_my(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    async with SessionLocal() as session:
        result = await session.execute(
            select(Request).where(Request.user_id == user_id).order_by(Request.id.desc()).limit(10)
        )
        rows = result.scalars().all()

    if not rows:
        return await callback.message.answer("У вас немає заявок.")

    text = "<b>📋 Ваші останні 10 заявок:</b>\n\n"
    for req in rows:
        text += f"• <b>#{req.id}</b> — {req.date.strftime('%d.%m.%Y')} {req.time} — {req.status}\n"

    await callback.message.answer(text)


@dp.callback_query(F.data == "menu_admin")
async def menu_admin(callback: types.CallbackQuery):
    if callback.from_user.id != SUPERADMIN_ID:
        async with SessionLocal() as session:
            result = await session.execute(
                select(Admin).where(Admin.telegram_id == callback.from_user.id)
            )
            admin = result.scalar_one_or_none()
            if not admin:
                return await callback.answer("⛔ Ви не адміністратор.", show_alert=True)

    await callback.message.answer("⚙️ <b>Адмін-панель:</b>", reply_markup=admin_menu())


###############################################################
#                ADMIN — NEW REQUESTS LIST
###############################################################

@dp.callback_query(F.data == "admin_new")
async def admin_new(callback: types.CallbackQuery):
    async with SessionLocal() as session:
        result = await session.execute(
            select(Request).where(Request.status == "new").order_by(Request.id.desc())
        )
        rows = result.scalars().all()

    if not rows:
        return await callback.message.answer("🟢 Немає нових заявок.")

    text = "<b>🆕 Нові заявки:</b>\n\n"
    for r in rows:
        text += f"• <b>#{r.id}</b> — {r.date.strftime('%d.%m.%Y')} {r.time}\n"

        await callback.message.answer(text)


###############################################################
#            ADMIN — LIST ALL REQUESTS (last 10)
###############################################################

@dp.callback_query(F.data == "admin_all")
async def admin_all(callback: types.CallbackQuery):
    async with SessionLocal() as session:
        result = await session.execute(
            select(Request).order_by(Request.id.desc()).limit(10)
        )
        rows = result.scalars().all()

    if not rows:
        return await callback.message.answer("⚪ Немає заявок в історії.")

    text = "<b>📚 Останні 10 заявок (нові → старі):</b>\n\n"
    for r in rows:
        text += (
            f"• <b>#{r.id}</b> — "
            f"{r.date.strftime('%d.%m.%Y')} {r.time} — "
            f"{'🟢 NEW' if r.status=='new' else '⚪ ' + r.status}\n"
        )

    await callback.message.answer(text)


###############################################################
#             ADMIN — ADD ADMIN (interactive menu)
###############################################################

@dp.callback_query(F.data == "admin_add")
async def admin_add(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "➕ Введіть Telegram ID користувача, якого потрібно зробити адміном:",
        reply_markup=back_keyboard()
    )
    await state.set_state("add_admin_wait_id")


@dp.message(F.text, state="add_admin_wait_id")
async def add_admin_wait_id(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.clear()
        return await message.answer("Повертаємось.", reply_markup=ReplyKeyboardRemove())

    try:
        tg_id = int(message.text.strip())
    except:
        return await message.answer("❌ ID має бути числом. Спробуйте ще.")

    async with SessionLocal() as session:
        existing = await session.execute(
            select(Admin).where(Admin.telegram_id == tg_id)
        )
        if existing.scalar_one_or_none():
            await state.clear()
            return await message.answer("⚠️ Цей користувач вже є адміністратором.")

        session.add(Admin(telegram_id=tg_id, is_superadmin=False))
        await session.commit()

    await message.answer(f"✔ Користувач <code>{tg_id}</code> став адміністратором.", reply_markup=ReplyKeyboardRemove())
    await state.clear()


###############################################################
#           ADMIN — REMOVE ADMIN (interactive menu)
###############################################################

@dp.callback_query(F.data == "admin_remove")
async def admin_remove(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "➖ Введіть Telegram ID користувача, якого потрібно видалити з адмінів:",
        reply_markup=back_keyboard()
    )
    await state.set_state("remove_admin_wait_id")


@dp.message(F.text, state="remove_admin_wait_id")
async def remove_admin_wait_id(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.clear()
        return await message.answer("Повертаємось.", reply_markup=ReplyKeyboardRemove())

    try:
        tg_id = int(message.text.strip())
    except:
        return await message.answer("❌ ID має бути числом.")

    async with SessionLocal() as session:
        await session.execute(
            delete(Admin).where(Admin.telegram_id == tg_id)
        )
        await session.commit()

    await message.answer(f"🗑 Адміністратора <code>{tg_id}</code> видалено.", reply_markup=ReplyKeyboardRemove())
    await state.clear()


###############################################################
#                ADMIN — CLEAR DATABASE
###############################################################

@dp.callback_query(F.data == "admin_clear")
async def admin_clear(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != SUPERADMIN_ID:
        return await callback.answer("⛔ Тільки суперадмін!", show_alert=True)

    kb = InlineKeyboardBuilder()
    kb.button(text="🗑 Очистити ВСІ заявки", callback_data="admin_clear_yes")
    kb.button(text="❌ Скасувати", callback_data="admin_clear_no")
    kb.adjust(1)

    await callback.message.answer(
        "⚠️ Ви точно хочете очистити ВСІ заявки у базі даних?",
        reply_markup=kb.as_markup()
    )


@dp.callback_query(F.data == "admin_clear_yes")
async def admin_clear_yes(callback: types.CallbackQuery):
    async with SessionLocal() as session:
        await session.execute(delete(Request))
        await session.commit()

    await callback.message.answer("🗑 Усі заявки успішно видалено!")


@dp.callback_query(F.data == "admin_clear_no")
async def admin_clear_no(callback: types.CallbackQuery):
    await callback.message.answer("Очищення скасовано.")


###############################################################
#                 DRIVER FORM — INPUT STEPS
###############################################################

@dp.message(QueueForm.supplier)
async def step_supplier(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if not text:
        return await message.answer("⚠ Введіть назву постачальника.")

    await state.update_data(supplier=text)

    await message.answer(
        "🔹 Введіть ПІБ водія:",
        reply_markup=back_keyboard()
    )
    await state.set_state(QueueForm.driver_name)


@dp.message(QueueForm.driver_name)
async def step_driver(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.supplier)
        return await message.answer("🔹 Введіть постачальника:", reply_markup=back_keyboard(False))

    text = message.text.strip()
    if not text:
        return await message.answer("⚠ Введіть ПІБ водія.")

    await state.update_data(driver_name=text)

    await message.answer(
        "🔹 Введіть номер телефону:",
        reply_markup=back_keyboard()
    )
    await state.set_state(QueueForm.phone)


@dp.message(QueueForm.phone)
async def step_phone(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.driver_name)
        return await message.answer("🔹 Введіть ПІБ водія:", reply_markup=back_keyboard())

    text = message.text.strip()
    if not text:
        return await message.answer("⚠ Введіть номер телефону.")

    await state.update_data(phone=text)

    await message.answer(
        "🔹 Введіть марку і номер авто:",
        reply_markup=back_keyboard()
    )
    await state.set_state(QueueForm.car)


@dp.message(QueueForm.car)
async def step_car(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.phone)
        return await message.answer("🔹 Введіть номер телефону:", reply_markup=back_keyboard())

    text = message.text.strip()
    if not text:
        return await message.answer("⚠ Введіть марку і номер авто.")

    await state.update_data(car=text)

    kb = InlineKeyboardBuilder()
    kb.button(text="📸 Завантажити документи", callback_data="photo_upload")
    kb.button(text="⏭ Пропустити", callback_data="photo_skip")
    kb.adjust(1)

    await message.answer(
        "🔹 Завантажте фото документів або пропустіть:",
        reply_markup=kb.as_markup()
    )
    await state.set_state(QueueForm.docs)


###############################################################
#                DOCUMENT UPLOAD
###############################################################

@dp.callback_query(QueueForm.docs, F.data == "photo_upload")
async def photo_upload(callback: types.CallbackQuery):
    await callback.message.answer("📸 Надішліть фото документів.")


@dp.message(QueueForm.docs, F.photo)
async def photo_received(message: types.Message, state: FSMContext):
    file_id = message.photo[-1].file_id
    await state.update_data(docs_file_id=file_id)

    kb = InlineKeyboardBuilder()
    kb.button(text="⏭ Далі", callback_data="photo_done")
    kb.adjust(1)

    await message.answer("Фото збережено.", reply_markup=kb.as_markup())


@dp.callback_query(QueueForm.docs, F.data == "photo_skip")
@dp.callback_query(QueueForm.docs, F.data == "photo_done")
async def photo_done(callback: types.CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder()
    kb.button(text="📦 На палетах", callback_data="type_pal")
    kb.button(text="🧱 В розсип", callback_data="type_loose")
    kb.adjust(1)

    await callback.message.answer(
        "🔹 Оберіть тип завантаження:",
        reply_markup=kb.as_markup()
    )
    await state.set_state(QueueForm.loading_type)


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
        reply_markup=build_date_calendar()
    )
    await state.set_state(QueueForm.calendar)


###############################################################
#                INLINE CALENDAR GENERATOR
###############################################################

def build_date_calendar(year=None, month=None):
    now = datetime.now()
    year = year or now.year
    month = month or now.month

    kb = InlineKeyboardBuilder()

    month_name = datetime(year, month, 1).strftime("%B %Y")
    kb.row(InlineKeyboardButton(text=f"📅 {month_name}", callback_data="ignore"))

    kb.row(*[
        InlineKeyboardButton(text=d, callback_data="ignore")
        for d in ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]
    ])

    first = datetime(year, month, 1).weekday()
    if first != 0:
        kb.row(*[
            InlineKeyboardButton(text=" ", callback_data="ignore")
            for _ in range(first)
        ])

    days = (datetime(year + (month == 12), (month % 12) + 1, 1) - timedelta(days=1)).day

    row = []
    for d in range(1, days + 1):
        row.append(InlineKeyboardButton(text=str(d), callback_data=f"day_{year}_{month}_{d}"))
        if len(row) == 7:
            kb.row(*row)
            row = []
    if row:
        kb.row(*row)

    prev_m = month - 1 or 12
    prev_y = year - 1 if month == 1 else year

    next_m = month + 1 if month < 12 else 1
    next_y = year + 1 if month == 12 else year

    kb.row(
        InlineKeyboardButton(text="⬅", callback_data=f"prev_{prev_y}_{prev_m}"),
        InlineKeyboardButton(text="Закрити", callback_data="close_calendar"),
        InlineKeyboardButton(text="➡", callback_data=f"next_{next_y}_{next_m}")
    )

    return kb.as_markup()


###############################################################
#       DRIVER — DATE / HOUR / MINUTE SELECTION
###############################################################

@dp.callback_query(QueueForm.calendar, F.data.startswith("prev_"))
async def cal_prev(callback: types.CallbackQuery):
    _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(reply_markup=build_date_calendar(int(y), int(m)))


@dp.callback_query(QueueForm.calendar, F.data.startswith("next_"))
async def cal_next(callback: types.CallbackQuery):
    _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(reply_markup=build_date_calendar(int(y), int(m)))


@dp.callback_query(QueueForm.calendar, F.data.startswith("day_"))
async def cal_day(callback: types.CallbackQuery, state: FSMContext):
    _, y, m, d = callback.data.split("_")
    chosen = date(int(y), int(m), int(d))

    await state.update_data(date=chosen)

    kb = InlineKeyboardBuilder()
    for h in range(24):
        kb.button(text=f"{h:02d}", callback_data=f"hour_{h:02d}")
    kb.adjust(6)

    await callback.message.answer(
        "⏰ Оберіть годину:",
        reply_markup=kb.as_markup()
    )
    await state.set_state(QueueForm.hour)


@dp.callback_query(QueueForm.calendar, F.data == "close_calendar")
async def close_calendar(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("❌ Вибір дати скасовано.")


@dp.callback_query(QueueForm.hour, F.data.startswith("hour_"))
async def hour_selected(callback: types.CallbackQuery, state: FSMContext):
    h = callback.data.replace("hour_", "")
    await state.update_data(hour=h)

    kb = InlineKeyboardBuilder()
    for m in range(0, 60, 5):
        kb.button(text=f"{m:02d}", callback_data=f"min_{m:02d}")
    kb.adjust(6)

    await callback.message.answer("🕒 Оберіть хвилини:", reply_markup=kb.as_markup())
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
            created_at=datetime.utcnow(),
            status="new"
        )
        session.add(req)
        await session.commit()
        await session.refresh(req)

    await broadcast_new_request(req.id)

    await callback.message.answer(
        f"✅ Заявку #{req.id} відправлено адміністратору!\n"
        f"👤 {req.driver_name}\n"
        f"📅 {req.date.strftime('%d.%m.%Y')}  ⏰ {req.time}"
    )

    await state.clear()


###############################################################
#       SEND REQUEST TO ALL ADMINS (AND NOTIFICATIONS)
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

        for a in admins:
            kb = InlineKeyboardBuilder()
            kb.button(text="✔ Підтвердити", callback_data=f"adm_ok_{req.id}")
            kb.button(text="🔁 Змінити дату/час", callback_data=f"adm_change_{req.id}")
            kb.button(text="❌ Відхилити", callback_data=f"adm_rej_{req.id}")
            kb.adjust(1)

            try:
                await bot.send_message(a.telegram_id, text, reply_markup=kb.as_markup())
                if req.docs_file_id:
                    await bot.send_photo(a.telegram_id, req.docs_file_id)
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

    # уведомить водителя
    await bot.send_message(
        req.user_id,
        f"🎉 <b>Заявка #{req.id} підтверджена!</b>\n📅 {req.date.strftime('%d.%m.%Y')}  ⏰ {req.time}"
    )

    # уведомить всех админов
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

    await bot.send_message(req.user_id, f"❌ <b>Заявку #{req.id} відхилено адміністратором.</b>")

    await notify_admins_about_action(req, "відхилена")


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


###############################################################
#                         BOT STARTUP
###############################################################

async def main():
    await init_db()

    async with SessionLocal() as session:
        res = await session.execute(select(Admin).where(Admin.telegram_id == SUPERADMIN_ID))
        if not res.scalar_one_or_none():
            session.add(Admin(telegram_id=SUPERADMIN_ID, is_superadmin=True))
            await session.commit()

    print("Bot started!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
