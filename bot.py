###############################################################
#                      DRIVER QUEUE BOT                       #
#                   Aiogram3 + Railway + PostgreSQL           #
#                      Final Premium Version                  #
###############################################################

import os
import asyncio
import logging
from datetime import datetime, date, timedelta

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

from sqlalchemy.ext.asyncio import (
    create_async_engine, async_sessionmaker, AsyncSession
)
from sqlalchemy import (
    Column, Integer, BigInteger, String, Boolean, Date, Text, TIMESTAMP,
    select, delete
)
from sqlalchemy.orm import declarative_base

from dotenv import load_dotenv

###############################################################
#                     CONFIG & INITIALIZATION
###############################################################

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPERADMIN_ID = int(os.getenv("SUPERADMIN_ID"))
DATABASE_URL = os.getenv("DATABASE_URL")

if not all([BOT_TOKEN, SUPERADMIN_ID, DATABASE_URL]):
    raise RuntimeError("Не встановлені BOT_TOKEN / SUPERADMIN_ID / DATABASE_URL")

# исправляем обычный postgres://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

logging.basicConfig(level=logging.INFO)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

###############################################################
#                       DATABASE
###############################################################

Base = declarative_base()

class Admin(Base):
    __tablename__ = "admins"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True)
    is_superadmin = Column(Boolean, default=False)


class Request(Base):
    __tablename__ = "requests"
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger)

    supplier = Column(Text)
    driver_name = Column(Text)
    phone = Column(Text)
    car = Column(Text)

    docs_file_id = Column(Text)

    loading_type = Column(Text)
    date = Column(Date)
    time = Column(Text)

    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    status = Column(String, default="new")
    admin_id = Column(BigInteger, nullable=True)
    admin_comment = Column(Text)

engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


###############################################################
#                       FSM STATES
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
    preview = State()


class AdminChangeForm(StatesGroup):
    calendar = State()
    hour = State()
    minute = State()


###############################################################
#                       HELPERS
###############################################################

async def get_admins():
    async with SessionLocal() as session:
        result = await session.execute(select(Admin))
        return result.scalars().all()


async def notify_all_admins(text, exclude=None):
    admins = await get_admins()
    for adm in admins:
        if exclude and adm.telegram_id == exclude:
            continue
        try:
            await bot.send_message(adm.telegram_id, text)
        except:
            pass


###############################################################
#                 INLINE CALENDARS & TIME PICKERS
###############################################################

def build_calendar(year=None, month=None):
    now = datetime.now()
    year = year or now.year
    month = month or now.month

    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text=f"📅 {datetime(year, month, 1).strftime('%B %Y')}", callback_data="ignore"))

    kb.row(*[
        types.InlineKeyboardButton(text=x, callback_data="ignore")
        for x in ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]
    ])

    start_wd = datetime(year, month, 1).weekday()
    if start_wd != 0:
        kb.row(*[types.InlineKeyboardButton(text=" ", callback_data="ignore")] * start_wd)

    days = (datetime(year + (month == 12), (month % 12) + 1, 1) - timedelta(days=1)).day

    row = []
    for d in range(1, days + 1):
        row.append(types.InlineKeyboardButton(text=str(d), callback_data=f"cal_day_{year}_{month}_{d}"))
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
        types.InlineKeyboardButton(text="⬅", callback_data=f"cal_prev_{prev_y}_{prev_m}"),
        types.InlineKeyboardButton(text="Закрити", callback_data="cal_close"),
        types.InlineKeyboardButton(text="➡", callback_data=f"cal_next_{next_y}_{next_m}")
    )

    return kb.as_markup()


def build_hour_keyboard():
    kb = InlineKeyboardBuilder()
    for h in range(24):
        kb.button(text=f"{h:02d}", callback_data=f"hour_{h:02d}")
    kb.adjust(6)
    return kb.as_markup()


def build_minute_keyboard():
    kb = InlineKeyboardBuilder()
    for m in range(0, 60, 5):
        kb.button(text=f"{m:02d}", callback_data=f"min_{m:02d}")
    kb.adjust(6)
    return kb.as_markup()


###############################################################
#                       MAIN MENU
###############################################################

def main_menu(is_admin=False, is_super=False):
    kb = InlineKeyboardBuilder()
    kb.button(text="📄 Створити заявку", callback_data="new_request")

    if is_admin:
        kb.button(text="🆕 Нові заявки", callback_data="admin_new")
        kb.button(text="📋 Всі заявки", callback_data="admin_all")

    if is_super:
        kb.button(text="➕ Додати адміна", callback_data="adm_add")
        kb.button(text="➖ Видалити адміна", callback_data="adm_delete")
        kb.button(text="🗑 Очистити БД", callback_data="adm_clear")

    kb.adjust(1)
    return kb.as_markup()


###############################################################
#                       START
###############################################################

@dp.message(CommandStart())
async def start(message: types.Message, state: FSMContext):
    await state.clear()

    admins = await get_admins()
    is_admin = any(a.telegram_id == message.from_user.id for a in admins)
    is_super = any(a.telegram_id == message.from_user.id and a.is_superadmin for a in admins)

    await message.answer(
        "Вітаю! Оберіть дію:",
        reply_markup=main_menu(is_admin, is_super)
    )


###############################################################
#                   CREATE REQUEST — STEP 1
###############################################################

@dp.callback_query(F.data == "new_request")
async def new_req(callback, state):
    await state.clear()
    await callback.message.answer("🔹 Введіть постачальника:")
    await state.set_state(QueueForm.supplier)


@dp.message(QueueForm.supplier)
async def step_supplier(msg, state):
    await state.update_data(supplier=msg.text)
    await msg.answer("🔹 Введіть ПІБ водія:")
    await state.set_state(QueueForm.driver_name)


@dp.message(QueueForm.driver_name)
async def step_driver(msg, state):
    await state.update_data(driver_name=msg.text)
    await msg.answer("🔹 Введіть телефон:")
    await state.set_state(QueueForm.phone)


@dp.message(QueueForm.phone)
async def step_phone(msg, state):
    await state.update_data(phone=msg.text)
    await msg.answer("🔹 Введіть марку та номер авто:")
    await state.set_state(QueueForm.car)


@dp.message(QueueForm.car)
async def step_car(msg, state):
    await state.update_data(car=msg.text)

    kb = InlineKeyboardBuilder()
    kb.button(text="📸 Завантажити фото", callback_data="photo_upload")
    kb.button(text="⏭ Пропустити", callback_data="photo_skip")
    kb.adjust(1)

    await msg.answer("🔹 Завантажте фото документів або пропустіть:", reply_markup=kb.as_markup())
    await state.set_state(QueueForm.docs)


@dp.callback_query(QueueForm.docs, F.data == "photo_upload")
async def ask_photo(callback):
    await callback.message.answer("Надішліть фото документів.")


@dp.message(QueueForm.docs, F.photo)
async def got_photo(msg, state):
    await state.update_data(docs_file_id=msg.photo[-1].file_id)
    await ask_loading_type(msg, state)


@dp.callback_query(QueueForm.docs, F.data == "photo_skip")
async def skip_photo(callback, state):
    await state.update_data(docs_file_id=None)
    await ask_loading_type(callback.message, state)


async def ask_loading_type(msg, state):
    kb = InlineKeyboardBuilder()
    kb.button(text="📦 На палетах", callback_data="load_pal")
    kb.button(text="🧱 В розсип", callback_data="load_loose")
    kb.adjust(1)

    await msg.answer("🔹 Оберіть тип завантаження:", reply_markup=kb.as_markup())
    await state.set_state(QueueForm.loading_type)


@dp.callback_query(QueueForm.loading_type)
async def step_load(callback, state):
    loading = "Палети" if callback.data == "load_pal" else "Розсип"
    await state.update_data(loading_type=loading)

    await callback.message.answer("🔹 Оберіть дату:", reply_markup=build_calendar())
    await state.set_state(QueueForm.calendar)


###############################################################
#                   CALENDAR + TIME
###############################################################

@dp.callback_query(QueueForm.calendar, F.data.startswith("cal_prev_"))
async def cal_prev(callback):
    _, _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(build_calendar(int(y), int(m)))


@dp.callback_query(QueueForm.calendar, F.data.startswith("cal_next_"))
async def cal_next(callback):
    _, _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(build_calendar(int(y), int(m)))


@dp.callback_query(QueueForm.calendar, F.data.startswith("cal_day_"))
async def cal_day(callback, state):
    _, _, y, m, d = callback.data.split("_")
    dt = date(int(y), int(m), int(d))
    await state.update_data(date=dt)

    await callback.message.answer("⏰ Оберіть годину:", reply_markup=build_hour_keyboard())
    await state.set_state(QueueForm.hour)


@dp.callback_query(QueueForm.calendar, F.data == "cal_close")
async def cancel_calendar(callback, state):
    await state.clear()
    await callback.message.answer("❌ Створення заявки скасовано.")


@dp.callback_query(QueueForm.hour, F.data.startswith("hour_"))
async def pick_hour(callback, state):
    await state.update_data(hour=callback.data.replace("hour_", ""))
    await callback.message.answer("🕒 Оберіть хвилини:", reply_markup=build_minute_keyboard())
    await state.set_state(QueueForm.minute)


@dp.callback_query(QueueForm.minute, F.data.startswith("min_"))
async def pick_min(callback, state):
    minute = callback.data.replace("min_", "")
    await state.update_data(minute=minute)

    data = await state.get_data()

    # формируем предпросмотр
    text = (
        "<b>🔍 Перевірте дані перед відправкою:</b>\n\n"
        f"🏢 Постачальник: <b>{data['supplier']}</b>\n"
        f"👤 Водій: <b>{data['driver_name']}</b>\n"
        f"📞 Телефон: <b>{data['phone']}</b>\n"
        f"🚚 Авто: <b>{data['car']}</b>\n"
        f"🧱 Завантаження: <b>{data['loading_type']}</b>\n"
        f"🗓 Дата: <b>{data['date'].strftime('%d.%m.%Y')}</b>\n"
        f"⏰ Час: <b>{data['hour']}:{data['minute']}</b>\n"
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="📨 Відправити адмінам", callback_data="send_final")
    kb.button(text="⬅ Редагувати", callback_data="edit_restart")
    kb.adjust(1)

    await callback.message.answer(text, reply_markup=kb.as_markup())
    await state.set_state(QueueForm.preview)


###############################################################
#                 PREVIEW → SUBMIT
###############################################################

@dp.callback_query(QueueForm.preview, F.data == "edit_restart")
async def edit_restart(callback, state):
    await state.clear()
    await callback.message.answer("🔄 Почнемо знову. Введіть постачальника:")
    await state.set_state(QueueForm.supplier)


@dp.callback_query(QueueForm.preview, F.data == "send_final")
async def send_final(callback, state):
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
            time=f"{data['hour']}:{data['minute']}",
            status="new",
            created_at=datetime.utcnow()
        )
        session.add(req)
        await session.commit()
        await session.refresh(req)

    await send_request_to_admins(req)

    await callback.message.answer("✅ Заявку відправлено!")
    await state.clear()


###############################################################
#               SEND TO ADMINS
###############################################################

async def send_request_to_admins(req):
    admins = await get_admins()
    if not admins:
        return

    text = (
        f"<b>📦 Нова заявка #{req.id}</b>\n\n"
        f"🏢 Постачальник: {req.supplier}\n"
        f"👤 Водій: {req.driver_name}\n"
        f"📞 Телефон: {req.phone}\n"
        f"🚚 Авто: {req.car}\n"
        f"🧱 Завантаження: {req.loading_type}\n"
        f"🗓 Дата: {req.date.strftime('%d.%m.%Y')}\n"
        f"⏰ Час: {req.time}\n"
    )

    for adm in admins:
        kb = InlineKeyboardBuilder()
        kb.button(text="✔ Підтвердити", callback_data=f"adm_ok_{req.id}")
        kb.button(text="🔁 Змінити", callback_data=f"adm_change_{req.id}")
        kb.button(text="❌ Відхилити", callback_data=f"adm_rej_{req.id}")
        kb.adjust(1)

        try:
            await bot.send_message(adm.telegram_id, text, reply_markup=kb.as_markup())
            if req.docs_file_id:
                await bot.send_photo(adm.telegram_id, req.docs_file_id)
        except:
            pass


###############################################################
#               ADMIN PANEL — LISTS
###############################################################

@dp.callback_query(F.data == "admin_new")
async def admin_new(callback):
    async with SessionLocal() as session:
        result = await session.execute(select(Request).where(Request.status == "new"))
        rows = result.scalars().all()

    if not rows:
        await callback.message.answer("🟢 Немає нових заявок.")
        return

    kb = InlineKeyboardBuilder()
    for r in rows:
        kb.button(text=f"🆕 Заявка #{r.id}", callback_data=f"view_{r.id}")
    kb.adjust(1)

    await callback.message.answer("🔽 Нові заявки:", reply_markup=kb.as_markup())


@dp.callback_query(F.data == "admin_all")
async def admin_all(callback):
    async with SessionLocal() as session:
        result = await session.execute(select(Request).order_by(Request.id.desc()))
        rows = result.scalars().all()

    if not rows:
        await callback.message.answer("Поки заявок немає.")
        return

    kb = InlineKeyboardBuilder()
    for r in rows:
        status = "🆕" if r.status == "new" else "✔" if r.status == "approved" else "❌"
        kb.button(text=f"{status} #{r.id}", callback_data=f"view_{r.id}")
    kb.adjust(1)

    await callback.message.answer("🔽 Всі заявки:", reply_markup=kb.as_markup())


###############################################################
#               VIEW REQUEST DETAILS
###############################################################

@dp.callback_query(F.data.startswith("view_"))
async def view_request(callback):
    req_id = int(callback.data.split("_")[1])

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)

    if not req:
        await callback.message.answer("Заявку не знайдено.")
        return

    text = (
        f"<b>Заявка #{req.id}</b>\n\n"
        f"🏢 Постачальник: {req.supplier}\n"
        f"👤 Водій: {req.driver_name}\n"
        f"📞 Телефон: {req.phone}\n"
        f"🚚 Авто: {req.car}\n"
        f"🧱 Завантаження: {req.loading_type}\n"
        f"🗓 Дата: {req.date.strftime('%d.%m.%Y')}\n"
        f"⏰ Час: {req.time}\n"
        f"📌 Статус: <b>{req.status}</b>\n"
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="✔ Підтвердити", callback_data=f"adm_ok_{req.id}")
    kb.button(text="🔁 Змінити", callback_data=f"adm_change_{req.id}")
    kb.button(text="❌ Відхилити", callback_data=f"adm_rej_{req.id}")
    kb.adjust(1)

    await callback.message.answer(text, reply_markup=kb.as_markup())

    if req.docs_file_id:
        await callback.message.answer("📄 Документи:")
        await bot.send_photo(callback.from_user.id, req.docs_file_id)


###############################################################
#               ADMIN ACTIONS: APPROVE / REJECT
###############################################################

@dp.callback_query(F.data.startswith("adm_ok_"))
async def adm_ok(callback):
    req_id = int(callback.data.split("_")[2])

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req:
            await callback.answer("Заявка не знайдена.")
            return

        req.status = "approved"
        req.admin_id = callback.from_user.id
        await session.commit()

    await callback.message.answer("✔ Заявку підтверджено.")
    await bot.send_message(req.user_id, f"🎉 Ваша заявка #{req_id} підтверджена!")

    await notify_all_admins(f"ℹ️ Адмін {callback.from_user.id} підтвердив заявку #{req_id}", exclude=callback.from_user.id)


@dp.callback_query(F.data.startswith("adm_rej_"))
async def adm_rej(callback):
    req_id = int(callback.data.split("_")[2])

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req:
            await callback.answer("Заявка не знайдена.")
            return

        req.status = "rejected"
        req.admin_id = callback.from_user.id
        await session.commit()

    await callback.message.answer("❌ Заявку відхилено.")
    await bot.send_message(req.user_id, f"❗ Вашу заявку #{req_id} відхилено.")

    await notify_all_admins(f"ℹ️ Адмін {callback.from_user.id} відхилив заявку #{req_id}", exclude=callback.from_user.id)


###############################################################
#               ADMIN CHANGE DATE/TIME
###############################################################

@dp.callback_query(F.data.startswith("adm_change_"))
async def adm_change(callback, state):
    req_id = int(callback.data.split("_")[2])
    await state.set_state(AdminChangeForm.calendar)
    await state.update_data(req_id=req_id)

    await callback.message.answer("🔄 Оберіть нову дату:", reply_markup=build_calendar())


@dp.callback_query(AdminChangeForm.calendar, F.data.startswith("cal_prev_"))
async def ac_prev(callback):
    _, _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(build_calendar(int(y), int(m)))


@dp.callback_query(AdminChangeForm.calendar, F.data.startswith("cal_next_"))
async def ac_next(callback):
    _, _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(build_calendar(int(y), int(m)))


@dp.callback_query(AdminChangeForm.calendar, F.data.startswith("cal_day_"))
async def ac_day(callback, state):
    _, _, y, m, d = callback.data.split("_")
    await state.update_data(new_date=date(int(y), int(m), int(d)))

    await callback.message.answer("⏰ Оберіть годину:", reply_markup=build_hour_keyboard())
    await state.set_state(AdminChangeForm.hour)


@dp.callback_query(AdminChangeForm.hour, F.data.startswith("hour_"))
async def ac_hour(callback, state):
    await state.update_data(new_hour=callback.data.replace("hour_", ""))
    await callback.message.answer("🕒 Оберіть хвилини:", reply_markup=build_minute_keyboard())
    await state.set_state(AdminChangeForm.minute)


@dp.callback_query(AdminChangeForm.minute, F.data.startswith("min_"))
async def ac_min(callback, state):
    minute = callback.data.replace("min_", "")
    data = await state.get_data()

    async with SessionLocal() as session:
        req = await session.get(Request, data["req_id"])

        req.date = data["new_date"]
        req.time = f"{data['new_hour']}:{minute}"
        req.status = "approved"
        req.admin_id = callback.from_user.id
        await session.commit()

    await callback.message.answer("🔁 Дату/час оновлено.")
    await bot.send_message(
        req.user_id,
        f"ℹ️ Заявка #{req.id} оновлена:\n"
        f"🗓 {req.date.strftime('%d.%m.%Y')}  ⏰ {req.time}"
    )

    await notify_all_admins(
        f"ℹ️ Адмін {callback.from_user.id} змінив дату/час заявки #{req.id}",
        exclude=callback.from_user.id
    )

    await state.clear()


###############################################################
#               SUPERADMIN — MANAGE ADMINS
###############################################################

@dp.callback_query(F.data == "adm_add")
async def adm_add(callback, state):
    await state.update_data(action="add_admin")
    await callback.message.answer("Введіть Telegram ID користувача:")
    await state.set_state(QueueForm.supplier)  # используем как временный ввод ID


@dp.callback_query(F.data == "adm_delete")
async def adm_delete(callback, state):
    await state.update_data(action="del_admin")
    await callback.message.answer("Введіть ID адміністратора для видалення:")
    await state.set_state(QueueForm.supplier)


@dp.callback_query(F.data == "adm_clear")
async def adm_clear(callback):
    kb = InlineKeyboardBuilder()
    kb.button(text="🗑 Очистити ВСІ заявки", callback_data="clear_all")
    kb.button(text="❌ Скасувати", callback_data="clear_cancel")
    kb.adjust(1)
    await callback.message.answer("Підтвердіть дію:", reply_markup=kb.as_markup())


@dp.callback_query(F.data == "clear_all")
async def clear_db(callback):
    async with SessionLocal() as session:
        await session.execute(delete(Request))
        await session.commit()

    await callback.message.answer("🗑 Всі заявки очищені.")


@dp.callback_query(F.data == "clear_cancel")
async def clear_cancel(callback):
    await callback.message.answer("❌ Скасовано.")


###############################################################
#                     BOT START
###############################################################

async def main():
    await init_db()

    async with SessionLocal() as session:
        res = await session.execute(select(Admin).where(Admin.telegram_id == SUPERADMIN_ID))
        if not res.scalar_one_or_none():
            sa = Admin(telegram_id=SUPERADMIN_ID, is_superadmin=True)
            session.add(sa)
            await session.commit()

    print("Bot started...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
