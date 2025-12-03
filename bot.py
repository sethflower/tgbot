###############################################################
#                      DRIVER QUEUE BOT                       #
#                   Aiogram3 + Railway + PostgreSQL           #
#                      by ChatGPT (Українська)                #
###############################################################

import os
import asyncio
import logging
from datetime import datetime, date, timedelta

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

from sqlalchemy.ext.asyncio import (
    create_async_engine, async_sessionmaker, AsyncSession
)
from sqlalchemy import (
    Column, Integer, BigInteger, String, Boolean, Date, Text, TIMESTAMP
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

logging.basicConfig(level=logging.INFO)

from aiogram.client.default import DefaultBotProperties

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

###############################################################
#                       DATABASE (SQLAlchemy)
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
    admin_comment = Column(Text, nullable=True)


# engine
engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def init_db():
    """Створення таблиць у БД."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


###############################################################
#                      HELPERS & UTILS
###############################################################

async def is_admin(tg_id: int) -> bool:
    """Перевірка чи юзер — адмін."""
    async with SessionLocal() as session:
        a = await session.get(Admin, {"telegram_id": tg_id})
        return a is not None


async def is_superadmin(tg_id: int) -> bool:
    """Перевірка чи юзер — суперадмін."""
    async with SessionLocal() as session:
        q = await session.execute(
            Admin.__table__.select().where(Admin.telegram_id == tg_id)
        )
        row = q.fetchone()
        return bool(row and row.is_superadmin)


###############################################################
#                        INLINE CALENDAR
###############################################################

def build_calendar(year=None, month=None):
    """Повертає клавіатуру календаря."""
    now = datetime.now()
    year = year or now.year
    month = month or now.month

    kb = InlineKeyboardBuilder()

    month_name = datetime(year, month, 1).strftime("%B %Y")
    kb.row(types.InlineKeyboardButton(text=f"📅 {month_name}", callback_data="ignore"))

    # кнопки днів тижня
    kb.row(
        *[
            types.InlineKeyboardButton(text=day, callback_data="ignore")
            for day in ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]
        ]
    )

    # початок місяця
    start_weekday = datetime(year, month, 1).weekday()  # 0 = Monday

    # відступи
    if start_weekday != 0:
        kb.row(*[types.InlineKeyboardButton(text=" ", callback_data="ignore")] * start_weekday)

    # дні
    days_in_month = (datetime(year + (month == 12), (month % 12) + 1, 1) - timedelta(days=1)).day

    buttons = []
    for d in range(1, days_in_month + 1):
        buttons.append(
            types.InlineKeyboardButton(
                text=str(d),
                callback_data=f"cal_day_{year}_{month}_{d}"
            )
        )

        if len(buttons) == 7:
            kb.row(*buttons)
            buttons = []
    if buttons:
        kb.row(*buttons)

    # навігація
    prev_month = month - 1 or 12
    prev_year = year - 1 if month == 1 else year

    next_month = month + 1 if month < 12 else 1
    next_year = year + 1 if month == 12 else year

    kb.row(
        types.InlineKeyboardButton(
            text="⬅", callback_data=f"cal_prev_{prev_year}_{prev_month}"
        ),
        types.InlineKeyboardButton(text="Закрити", callback_data="cal_close"),
        types.InlineKeyboardButton(
            text="➡", callback_data=f"cal_next_{next_year}_{next_month}"
        )
    )

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


###############################################################
#                        START / NEW REQUEST
###############################################################

@dp.message(CommandStart())
async def start(message: types.Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="📄 Створити заявку", callback_data="new_request")
    await message.answer(
        "Вітаю! Це бот електронної черги для водіїв.\nНатисніть кнопку нижче, щоб створити заявку.",
        reply_markup=kb.as_markup()
    )


@dp.callback_query(F.data == "new_request")
async def create_new_request(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("🔹 Введіть постачальника:")
    await state.set_state(QueueForm.supplier)


###############################################################
#                           DRIVER INPUT
###############################################################

@dp.message(QueueForm.supplier)
async def step_supplier(message: types.Message, state: FSMContext):
    await state.update_data(supplier=message.text)
    await message.answer("🔹 Введіть ПІБ водія:")
    await state.set_state(QueueForm.driver_name)


@dp.message(QueueForm.driver_name)
async def step_driver_name(message: types.Message, state: FSMContext):
    await state.update_data(driver_name=message.text)
    await message.answer("🔹 Введіть номер телефону:")
    await state.set_state(QueueForm.phone)


@dp.message(QueueForm.phone)
async def step_phone(message: types.Message, state: FSMContext):
    await state.update_data(phone=message.text)
    await message.answer("🔹 Введіть марку та номер авто:")
    await state.set_state(QueueForm.car)


@dp.message(QueueForm.car)
async def step_car(message: types.Message, state: FSMContext):
    await state.update_data(car=message.text)

    kb = InlineKeyboardBuilder()
    kb.button(text="📸 Завантажити фото", callback_data="photo_upload")
    kb.button(text="⏭ Пропустити", callback_data="photo_skip")
    kb.adjust(1)

    await message.answer("🔹 Завантажте фото документів або пропустіть:", reply_markup=kb.as_markup())
    await state.set_state(QueueForm.docs)


@dp.callback_query(QueueForm.docs, F.data == "photo_upload")
async def ask_photo(callback: types.CallbackQuery):
    await callback.message.answer("Надішліть фото документів:")
    # чекаємо фото


@dp.message(QueueForm.docs, F.photo)
async def step_photo(message: types.Message, state: FSMContext):
    file_id = message.photo[-1].file_id
    await state.update_data(docs_file_id=file_id)
    await ask_loading_type(message, state)


@dp.callback_query(QueueForm.docs, F.data == "photo_skip")
async def skip_photo(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(docs_file_id=None)
    await ask_loading_type(callback.message, state)


async def ask_loading_type(message_or_callback, state: FSMContext):
    kb = InlineKeyboardBuilder()
    kb.button(text="📦 На палетах", callback_data="load_pal")
    kb.button(text="🧱 В розсип", callback_data="load_loose")
    kb.adjust(1)

    await message_or_callback.answer("🔹 Оберіть тип завантаження:", reply_markup=kb.as_markup())
    await state.set_state(QueueForm.loading_type)


@dp.callback_query(QueueForm.loading_type)
async def step_loading(callback: types.CallbackQuery, state: FSMContext):
    loading = "Палети" if callback.data == "load_pal" else "Розсип"
    await state.update_data(loading_type=loading)

    # календар
    await callback.message.answer("🔹 Оберіть дату:", reply_markup=build_calendar())
    await state.set_state(QueueForm.calendar)


###############################################################
#                       CALENDAR HANDLERS
###############################################################

@dp.callback_query(QueueForm.calendar, F.data.startswith("cal_prev_"))
async def cal_prev(callback: types.CallbackQuery):
    _, _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(reply_markup=build_calendar(int(y), int(m)))


@dp.callback_query(QueueForm.calendar, F.data.startswith("cal_next_"))
async def cal_next(callback: types.CallbackQuery):
    _, _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(reply_markup=build_calendar(int(y), int(m)))


@dp.callback_query(QueueForm.calendar, F.data.startswith("cal_day_"))
async def cal_day(callback: types.CallbackQuery, state: FSMContext):
    _, _, y, m, d = callback.data.split("_")
    chosen = date(int(y), int(m), int(d))
    await state.update_data(date=chosen)

    # Вибір години
    kb = InlineKeyboardBuilder()
    for h in range(0, 24):
        kb.button(text=f"{h:02d}", callback_data=f"hour_{h:02d}")
    kb.adjust(6)
    await callback.message.answer("⏰ Оберіть годину:", reply_markup=kb.as_markup())
    await state.set_state(QueueForm.hour)


@dp.callback_query(QueueForm.calendar, F.data == "cal_close")
async def cal_close(callback: types.CallbackQuery):
    await callback.message.delete()


###############################################################
#               HOUR → MINUTE SELECTION
###############################################################

@dp.callback_query(QueueForm.hour, F.data.startswith("hour_"))
async def step_hour(callback: types.CallbackQuery, state: FSMContext):
    hour = callback.data.replace("hour_", "")
    await state.update_data(hour=hour)

    # Вибір хвилин
    kb = InlineKeyboardBuilder()
    for m in range(0, 60, 5):
        kb.button(text=f"{m:02d}", callback_data=f"min_{m:02d}")
    kb.adjust(6)

    await callback.message.answer("🕒 Оберіть хвилини:", reply_markup=kb.as_markup())
    await state.set_state(QueueForm.minute)


@dp.callback_query(QueueForm.minute, F.data.startswith("min_"))
async def step_minute(callback: types.CallbackQuery, state: FSMContext):
    minute = callback.data.replace("min_", "")
    data = await state.get_data()

    # збереження в БД
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

        req_id = req.id

    await send_admin_request(req_id)
    await callback.message.answer("✅ Заявку відправлено адміністратору!")
    await state.clear()


async def send_admin_request(request_id: int):
    """Відправка заявки всім адміністраторам."""
    async with SessionLocal() as session:
        req = await session.get(Request, request_id)

        # шукаємо адмінів
        q = await session.execute(Admin.__table__.select())
        admins = q.fetchall()

        text = (
            f"<b>📦 Нова заявка #{req.id}</b>\n\n"
            f"<b>Постачальник:</b> {req.supplier}\n"
            f"<b>Водій:</b> {req.driver_name}\n"
            f"<b>Телефон:</b> {req.phone}\n"
            f"<b>Авто:</b> {req.car}\n"
            f"<b>Тип завантаження:</b> {req.loading_type}\n"
            f"<b>Дата:</b> {req.date}\n"
            f"<b>Час:</b> {req.time}\n"
        )

        kb = InlineKeyboardBuilder()
        kb.button(text="✔ Підтвердити", callback_data=f"adm_ok_{req.id}")
        kb.button(text="🔁 Змінити дату/час", callback_data=f"adm_change_{req.id}")
        kb.button(text="❌ Відхилити", callback_data=f"adm_rej_{req.id}")
        kb.adjust(1)

        for admin in admins:
            try:
                msg = await bot.send_message(admin.telegram_id, text, reply_markup=kb.as_markup())

                # надсилаємо фото, якщо є
                if req.docs_file_id:
                    await bot.send_photo(admin.telegram_id, req.docs_file_id)
                else:
                    await bot.send_message(admin.telegram_id, "❗ Документи не завантажені.")

            except:
                pass


###############################################################
#                   ADMIN ACTIONS
###############################################################

@dp.callback_query(F.data.startswith("adm_ok_"))
async def admin_approve(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[2])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        req.status = "approved"
        req.admin_id = callback.from_user.id
        await session.commit()

    await callback.message.answer("✔ Заявку підтверджено.")
    await bot.send_message(req.user_id, f"🎉 Ваша заявка #{req.id} підтверджена!")

@dp.callback_query(F.data.startswith("adm_rej_"))
async def admin_reject(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[2])
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        req.status = "rejected"
        req.admin_id = callback.from_user.id
        await session.commit()

    await callback.message.answer("❌ Заявку відхилено.")
    await bot.send_message(req.user_id, f"❗ Ваша заявка #{req.id} відхилена адміністратором.")

###############################################################
#         ADMIN CHANGE DATE/TIME → NEW CALENDAR
###############################################################

@dp.callback_query(F.data.startswith("adm_change_"))
async def admin_change(callback: types.CallbackQuery, state: FSMContext):
    req_id = int(callback.data.split("_")[2])
    await state.update_data(req_id=req_id)

    await callback.message.answer("🔄 Оберіть нову дату:", reply_markup=build_calendar())
    await state.set_state(QueueForm.calendar)


###############################################################
#      SUPERADMIN: ADD / REMOVE ADMIN
###############################################################

@dp.message(Command("add_admin"))
async def add_admin(message: types.Message):
    if message.from_user.id != SUPERADMIN_ID:
        return await message.answer("⛔ Доступ заборонено.")

    try:
        tg_id = int(message.text.split()[1])
    except:
        return await message.answer("Формат: /add_admin 123456789")

    async with SessionLocal() as session:
        a = Admin(telegram_id=tg_id, is_superadmin=False)
        session.add(a)
        await session.commit()

    await message.answer(f"✔ Адміністратора {tg_id} додано.")


@dp.message(Command("remove_admin"))
async def remove_admin(message: types.Message):
    if message.from_user.id != SUPERADMIN_ID:
        return await message.answer("⛔ Доступ заборонено.")

    try:
        tg_id = int(message.text.split()[1])
    except:
        return await message.answer("Формат: /remove_admin 123456789")

    async with SessionLocal() as session:
        await session.execute(Admin.__table__.delete().where(Admin.telegram_id == tg_id))
        await session.commit()

    await message.answer(f"🗑 Адміністратора {tg_id} видалено.")


@dp.message(Command("admins"))
async def list_admins(message: types.Message):
    if message.from_user.id != SUPERADMIN_ID:
        return await message.answer("⛔ Доступ заборонено.")

    async with SessionLocal() as session:
        q = await session.execute(Admin.__table__.select())
        rows = q.fetchall()

    text = "<b>📋 Список адміністраторів:</b>\n\n"
    for r in rows:
        role = "SUPERADMIN" if r.is_superadmin else "admin"
        text += f"• {r.telegram_id} — {role}\n"

    await message.answer(text)


###############################################################
#                        BOT STARTUP
###############################################################

async def main():
    await init_db()

    # Додаємо супер-адміна, якщо його немає
    async with SessionLocal() as session:
        q = await session.execute(Admin.__table__.select().where(Admin.telegram_id == SUPERADMIN_ID))
        if not q.first():
            sa = Admin(telegram_id=SUPERADMIN_ID, is_superadmin=True)
            session.add(sa)
            await session.commit()

    print("Bot started...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
