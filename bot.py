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

from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)

from sqlalchemy.ext.asyncio import (
    create_async_engine, async_sessionmaker, AsyncSession
)
from sqlalchemy import (
    Column, Integer, BigInteger, String, Boolean, Date, Text, TIMESTAMP, select, delete
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

# если вдруг кто-то положит обычный postgres:// — поправим префикс
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


engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def init_db():
    """Створення таблиць у БД."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


###############################################################
#                      HELPERS & UTILS
###############################################################

BACK_TEXT = "⬅ Назад"


def back_keyboard(include_back: bool = True) -> ReplyKeyboardMarkup | ReplyKeyboardRemove:
    """Клавіатура з кнопкою 'Назад' для текстових кроків."""
    if not include_back:
        # на первом шаге без 'Назад'
        return ReplyKeyboardRemove()
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BACK_TEXT)]],
        resize_keyboard=True
    )


async def is_admin(tg_id: int) -> bool:
    """Перевірка чи юзер — адмін."""
    async with SessionLocal() as session:
        result = await session.execute(
            select(Admin).where(Admin.telegram_id == tg_id)
        )
        admin = result.scalar_one_or_none()
        return admin is not None


async def is_superadmin(tg_id: int) -> bool:
    """Перевірка чи юзер — суперадмін."""
    async with SessionLocal() as session:
        result = await session.execute(
            select(Admin).where(
                Admin.telegram_id == tg_id,
                Admin.is_superadmin.is_(True)
            )
        )
        admin = result.scalar_one_or_none()
        return admin is not None


###############################################################
#                        INLINE CALENDAR
###############################################################

def build_calendar(year: int | None = None, month: int | None = None):
    """Повертає клавіатуру календаря (inline)."""
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

    start_weekday = datetime(year, month, 1).weekday()  # 0 = Monday

    # відступи
    if start_weekday != 0:
        kb.row(*[types.InlineKeyboardButton(text=" ", callback_data="ignore")] * start_weekday)

    # дні місяця
    days_in_month = (datetime(year + (month == 12), (month % 12) + 1, 1) - timedelta(days=1)).day

    buttons: list[types.InlineKeyboardButton] = []
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


def build_hour_keyboard():
    kb = InlineKeyboardBuilder()
    for h in range(0, 24):
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
#                        FSM STATES
###############################################################

class QueueForm(StatesGroup):
    """Стан машини для водія (створення нової заявки)."""
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
    """Окремі стани для адміна (зміна дати/часу заявки)."""
    calendar = State()
    hour = State()
    minute = State()


###############################################################
#                        START / NEW REQUEST
###############################################################

@dp.message(CommandStart())
async def start(message: types.Message, state: FSMContext):
    await state.clear()
    kb = InlineKeyboardBuilder()
    kb.button(text="📄 Створити заявку", callback_data="new_request")
    await message.answer(
        "Вітаю! Це бот електронної черги для водіїв.\n"
        "Натисніть кнопку нижче, щоб створити нову заявку.",
        reply_markup=kb.as_markup()
    )


@dp.callback_query(F.data == "new_request")
async def create_new_request(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer(
        "🔹 Введіть постачальника:",
        reply_markup=back_keyboard(include_back=False)
    )
    await state.set_state(QueueForm.supplier)


###############################################################
#                           DRIVER INPUT
###############################################################

@dp.message(QueueForm.supplier)
async def step_supplier(message: types.Message, state: FSMContext):
    # на первом шаге 'Назад' не обрабатываем
    text = message.text.strip()
    if not text:
        await message.answer("⚠ Введіть, будь ласка, назву постачальника.")
        return

    await state.update_data(supplier=text)
    await message.answer(
        "🔹 Введіть ПІБ водія:",
        reply_markup=back_keyboard(include_back=True)
    )
    await state.set_state(QueueForm.driver_name)


@dp.message(QueueForm.driver_name)
async def step_driver_name(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.supplier)
        await message.answer(
            "🔹 Введіть постачальника:",
            reply_markup=back_keyboard(include_back=False)
        )
        return

    text = message.text.strip()
    if not text:
        await message.answer("⚠ Введіть ПІБ водія.")
        return

    await state.update_data(driver_name=text)
    await message.answer(
        "🔹 Введіть номер телефону:",
        reply_markup=back_keyboard(include_back=True)
    )
    await state.set_state(QueueForm.phone)


@dp.message(QueueForm.phone)
async def step_phone(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.driver_name)
        await message.answer(
            "🔹 Введіть ПІБ водія:",
            reply_markup=back_keyboard(include_back=True)
        )
        return

    text = message.text.strip()
    if not text:
        await message.answer("⚠ Введіть номер телефону.")
        return

    await state.update_data(phone=text)
    await message.answer(
        "🔹 Введіть марку та номер авто:",
        reply_markup=back_keyboard(include_back=True)
    )
    await state.set_state(QueueForm.car)


@dp.message(QueueForm.car)
async def step_car(message: types.Message, state: FSMContext):
    if message.text == BACK_TEXT:
        await state.set_state(QueueForm.phone)
        await message.answer(
            "🔹 Введіть номер телефону:",
            reply_markup=back_keyboard(include_back=True)
        )
        return

    text = message.text.strip()
    if not text:
        await message.answer("⚠ Введіть марку та номер авто.")
        return

    await state.update_data(car=text)

    kb = InlineKeyboardBuilder()
    kb.button(text="📸 Завантажити фото документів", callback_data="photo_upload")
    kb.button(text="⏭ Пропустити", callback_data="photo_skip")
    kb.adjust(1)

    await message.answer(
        "🔹 Завантажте фото документів або натисніть «Пропустити».",
        reply_markup=kb.as_markup()
    )
    await state.set_state(QueueForm.docs)


@dp.callback_query(QueueForm.docs, F.data == "photo_upload")
async def ask_photo(callback: types.CallbackQuery):
    await callback.message.answer(
        "📸 Надішліть фото документів одним або кількома повідомленнями.\n"
        "Після останнього фото просто натисніть кнопку «Далі» нижче."
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="⏭ Далі без додаткових фото", callback_data="photo_done")
    kb.adjust(1)
    await callback.message.answer("Коли закінчите — натисніть «Далі».", reply_markup=kb.as_markup())


@dp.message(QueueForm.docs, F.photo)
async def step_photo(message: types.Message, state: FSMContext):
    file_id = message.photo[-1].file_id
    data = await state.get_data()
    # збережемо останнє фото (можна при бажанні зробити список)
    await state.update_data(docs_file_id=file_id)


@dp.callback_query(QueueForm.docs, F.data == "photo_done")
async def photo_done(callback: types.CallbackQuery, state: FSMContext):
    await ask_loading_type(callback.message, state)


@dp.callback_query(QueueForm.docs, F.data == "photo_skip")
async def skip_photo(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(docs_file_id=None)
    await ask_loading_type(callback.message, state)


async def ask_loading_type(message: types.Message, state: FSMContext):
    kb = InlineKeyboardBuilder()
    kb.button(text="📦 На палетах", callback_data="load_pal")
    kb.button(text="🧱 В розсип", callback_data="load_loose")
    kb.adjust(1)

    await message.answer(
        "🔹 Оберіть тип завантаження:",
        reply_markup=kb.as_markup()
    )
    await state.set_state(QueueForm.loading_type)


@dp.callback_query(QueueForm.loading_type)
async def step_loading(callback: types.CallbackQuery, state: FSMContext):
    if callback.data == "load_pal":
        loading = "Палети"
    elif callback.data == "load_loose":
        loading = "Розсип"
    else:
        await callback.answer("Невідома дія.", show_alert=True)
        return

    await state.update_data(loading_type=loading)

    # календар
    await callback.message.answer(
        "🔹 Оберіть дату:",
        reply_markup=build_calendar()
    )
    await state.set_state(QueueForm.calendar)


###############################################################
#                       CALENDAR HANDLERS (DRIVER)
###############################################################

@dp.callback_query(QueueForm.calendar, F.data.startswith("cal_prev_"))
async def cal_prev_driver(callback: types.CallbackQuery):
    _, _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(
        reply_markup=build_calendar(int(y), int(m))
    )


@dp.callback_query(QueueForm.calendar, F.data.startswith("cal_next_"))
async def cal_next_driver(callback: types.CallbackQuery):
    _, _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(
        reply_markup=build_calendar(int(y), int(m))
    )


@dp.callback_query(QueueForm.calendar, F.data.startswith("cal_day_"))
async def cal_day_driver(callback: types.CallbackQuery, state: FSMContext):
    _, _, y, m, d = callback.data.split("_")
    chosen = date(int(y), int(m), int(d))
    await state.update_data(date=chosen)

    await callback.message.answer(
        "⏰ Оберіть годину:",
        reply_markup=build_hour_keyboard()
    )
    await state.set_state(QueueForm.hour)


@dp.callback_query(QueueForm.calendar, F.data == "cal_close")
async def cal_close_driver(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("❌ Вибір дати скасовано. Ви можете почати знову командою /start.")


###############################################################
#               HOUR → MINUTE SELECTION (DRIVER)
###############################################################

@dp.callback_query(QueueForm.hour, F.data.startswith("hour_"))
async def step_hour_driver(callback: types.CallbackQuery, state: FSMContext):
    hour = callback.data.replace("hour_", "")
    await state.update_data(hour=hour)

    await callback.message.answer(
        "🕒 Оберіть хвилини:",
        reply_markup=build_minute_keyboard()
    )
    await state.set_state(QueueForm.minute)


@dp.callback_query(QueueForm.minute, F.data.startswith("min_"))
async def step_minute_driver(callback: types.CallbackQuery, state: FSMContext):
    minute = callback.data.replace("min_", "")
    data = await state.get_data()

    # захист від KeyError — перевіряємо обов'язкові поля
    required = ["supplier", "driver_name", "phone", "car",
                "loading_type", "date", "hour"]
    missing = [k for k in required if k not in data]
    if missing:
        await callback.message.answer(
            "⚠ Виникла помилка збереження даних (відсутні поля).\n"
            "Будь ласка, почніть створення заявки знову командою /start."
        )
        await state.clear()
        return

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
        req_id = req.id

    await send_admin_request(req_id)

    # водителю — красивая карточка с его заявкой
    text = (
        f"✅ <b>Заявку відправлено адміністратору!</b>\n\n"
        f"📦 <b>Заявка №{req_id}</b>\n"
        f"👤 Водій: <b>{data['driver_name']}</b>\n"
        f"🏢 Постачальник: <b>{data['supplier']}</b>\n"
        f"📞 Телефон: <b>{data['phone']}</b>\n"
        f"🚚 Авто: <b>{data['car']}</b>\n"
        f"🧱 Тип завантаження: <b>{data['loading_type']}</b>\n"
        f"🗓 Дата: <b>{data['date'].strftime('%d.%m.%Y')}</b>\n"
        f"⏰ Час: <b>{data['hour']}:{minute}</b>\n"
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="📄 Створити ще одну заявку", callback_data="new_request")
    kb.adjust(1)

    await callback.message.answer(text, reply_markup=kb.as_markup())
    await state.clear()


async def send_admin_request(request_id: int):
    """Відправка заявки всім адміністраторам."""
    async with SessionLocal() as session:
        req = await session.get(Request, request_id)
        if not req:
            return

        # шукаємо адмінів
        result = await session.execute(select(Admin))
        admins = result.scalars().all()

        text = (
            f"<b>📦 Нова заявка #{req.id}</b>\n\n"
            f"🏢 <b>Постачальник:</b> {req.supplier}\n"
            f"👤 <b>Водій:</b> {req.driver_name}\n"
            f"📞 <b>Телефон:</b> {req.phone}\n"
            f"🚚 <b>Авто:</b> {req.car}\n"
            f"🧱 <b>Тип завантаження:</b> {req.loading_type}\n"
            f"🗓 <b>Дата:</b> {req.date.strftime('%d.%m.%Y')}\n"
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
                # надсилаємо фото, якщо є
                if req.docs_file_id:
                    await bot.send_photo(admin.telegram_id, req.docs_file_id)
                else:
                    await bot.send_message(admin.telegram_id, "❗ Документи не завантажені.")
            except TelegramBadRequest:
                pass
            except Exception:
                pass


###############################################################
#                   ADMIN ACTIONS
###############################################################

@dp.callback_query(F.data.startswith("adm_ok_"))
async def admin_approve(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[2])

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req:
            await callback.answer("Заявка не знайдена.", show_alert=True)
            return

        req.status = "approved"
        req.admin_id = callback.from_user.id
        await session.commit()

    await callback.message.answer("✔ Заявку підтверджено.")

    # повідомлення водію — з картою
    text = (
        f"🎉 <b>Ваша заявка #{req_id} підтверджена!</b>\n\n"
        f"🗓 Дата: <b>{req.date.strftime('%d.%m.%Y')}</b>\n"
        f"⏰ Час: <b>{req.time}</b>\n"
        f"🏢 Постачальник: <b>{req.supplier}</b>\n"
        f"🚚 Авто: <b>{req.car}</b>\n"
        f"🧱 Тип завантаження: <b>{req.loading_type}</b>\n"
    )
    await bot.send_message(req.user_id, text)


@dp.callback_query(F.data.startswith("adm_rej_"))
async def admin_reject(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[2])

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req:
            await callback.answer("Заявка не знайдена.", show_alert=True)
            return

        req.status = "rejected"
        req.admin_id = callback.from_user.id
        await session.commit()

    await callback.message.answer("❌ Заявку відхилено.")

    text = (
        f"❗ <b>Ваша заявка #{req_id} відхилена адміністратором.</b>\n\n"
        f"Якщо потрібно, створіть нову заявку з коректними даними."
    )
    await bot.send_message(req.user_id, text)


###############################################################
#         ADMIN CHANGE DATE/TIME → NEW CALENDAR
###############################################################

@dp.callback_query(F.data.startswith("adm_change_"))
async def admin_change(callback: types.CallbackQuery, state: FSMContext):
    req_id = int(callback.data.split("_")[2])
    await state.clear()
    await state.update_data(req_id=req_id)

    await callback.message.answer(
        "🔄 Оберіть нову дату для цієї заявки:",
        reply_markup=build_calendar()
    )
    await state.set_state(AdminChangeForm.calendar)


# --- календарь для админа ---

@dp.callback_query(AdminChangeForm.calendar, F.data.startswith("cal_prev_"))
async def cal_prev_admin(callback: types.CallbackQuery):
    _, _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(
        reply_markup=build_calendar(int(y), int(m))
    )


@dp.callback_query(AdminChangeForm.calendar, F.data.startswith("cal_next_"))
async def cal_next_admin(callback: types.CallbackQuery):
    _, _, y, m = callback.data.split("_")
    await callback.message.edit_reply_markup(
        reply_markup=build_calendar(int(y), int(m))
    )


@dp.callback_query(AdminChangeForm.calendar, F.data.startswith("cal_day_"))
async def cal_day_admin(callback: types.CallbackQuery, state: FSMContext):
    _, _, y, m, d = callback.data.split("_")
    chosen = date(int(y), int(m), int(d))
    await state.update_data(new_date=chosen)

    await callback.message.answer(
        "⏰ Оберіть нову годину:",
        reply_markup=build_hour_keyboard()
    )
    await state.set_state(AdminChangeForm.hour)


@dp.callback_query(AdminChangeForm.calendar, F.data == "cal_close")
async def cal_close_admin(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("❌ Зміна дати скасована.")


# --- час/хвилини для админа ---

@dp.callback_query(AdminChangeForm.hour, F.data.startswith("hour_"))
async def admin_step_hour(callback: types.CallbackQuery, state: FSMContext):
    hour = callback.data.replace("hour_", "")
    await state.update_data(new_hour=hour)

    await callback.message.answer(
        "🕒 Оберіть нові хвилини:",
        reply_markup=build_minute_keyboard()
    )
    await state.set_state(AdminChangeForm.minute)


@dp.callback_query(AdminChangeForm.minute, F.data.startswith("min_"))
async def admin_step_minute(callback: types.CallbackQuery, state: FSMContext):
    minute = callback.data.replace("min_", "")
    data = await state.get_data()

    if "req_id" not in data or "new_date" not in data or "new_hour" not in data:
        await callback.message.answer(
            "⚠ Помилка збереження стану. Спробуйте ще раз натиснути «Змінити дату/час»."
        )
        await state.clear()
        return

    req_id = data["req_id"]
    new_date: date = data["new_date"]
    new_time_str = f"{data['new_hour']}:{minute}"

    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if not req:
            await callback.message.answer("Заявка не знайдена.")
            await state.clear()
            return

        req.date = new_date
        req.time = new_time_str
        req.status = "approved"   # можна одразу вважати підтвердженою
        req.admin_id = callback.from_user.id
        await session.commit()

    await callback.message.answer(
        f"🔁 Нові дата/час для заявки #{req_id} збережені:\n"
        f"🗓 {new_date.strftime('%d.%m.%Y')}  ⏰ {new_time_str}"
    )

    # повідомляємо водія
    text = (
        f"ℹ️ <b>Оновлення по заявці #{req_id}</b>\n\n"
        f"🗓 Нова дата: <b>{new_date.strftime('%d.%m.%Y')}</b>\n"
        f"⏰ Новий час: <b>{new_time_str}</b>\n"
        f"Статус: <b>підтверджено</b> ✅"
    )
    async with SessionLocal() as session:
        req = await session.get(Request, req_id)
        if req:
            await bot.send_message(req.user_id, text)

    await state.clear()


###############################################################
#      SUPERADMIN: ADD / REMOVE ADMIN
###############################################################

@dp.message(Command("add_admin"))
async def add_admin(message: types.Message):
    if message.from_user.id != SUPERADMIN_ID:
        return await message.answer("⛔ Доступ заборонено. Тільки суперадмін.")

    parts = message.text.split()
    if len(parts) != 2:
        return await message.answer("Формат: <code>/add_admin 123456789</code>")

    try:
        tg_id = int(parts[1])
    except ValueError:
        return await message.answer("ID має бути числом.")

    async with SessionLocal() as session:
        result = await session.execute(select(Admin).where(Admin.telegram_id == tg_id))
        existing = result.scalar_one_or_none()
        if existing:
            await message.answer("Цей користувач вже є адміністратором.")
            return

        a = Admin(telegram_id=tg_id, is_superadmin=False)
        session.add(a)
        await session.commit()

    await message.answer(f"✔ Адміністратора <code>{tg_id}</code> додано.")


@dp.message(Command("remove_admin"))
async def remove_admin(message: types.Message):
    if message.from_user.id != SUPERADMIN_ID:
        return await message.answer("⛔ Доступ заборонено. Тільки суперадмін.")

    parts = message.text.split()
    if len(parts) != 2:
        return await message.answer("Формат: <code>/remove_admin 123456789</code>")

    try:
        tg_id = int(parts[1])
    except ValueError:
        return await message.answer("ID має бути числом.")

    async with SessionLocal() as session:
        await session.execute(
            delete(Admin).where(Admin.telegram_id == tg_id)
        )
        await session.commit()

    await message.answer(f"🗑 Адміністратора <code>{tg_id}</code> видалено.")


@dp.message(Command("admins"))
async def list_admins(message: types.Message):
    if message.from_user.id != SUPERADMIN_ID:
        return await message.answer("⛔ Доступ заборонено. Тільки суперадмін.")

    async with SessionLocal() as session:
        result = await session.execute(select(Admin))
        rows = result.scalars().all()

    if not rows:
        return await message.answer("Немає жодного адміністратора.")

    text = "<b>📋 Список адміністраторів:</b>\n\n"
    for r in rows:
        role = "SUPERADMIN" if r.is_superadmin else "admin"
        text += f"• <code>{r.telegram_id}</code> — {role}\n"

    await message.answer(text)


###############################################################
#                        BOT STARTUP
###############################################################

async def main():
    await init_db()

    # Додаємо супер-адміна, якщо його немає
    async with SessionLocal() as session:
        result = await session.execute(
            select(Admin).where(Admin.telegram_id == SUPERADMIN_ID)
        )
        if not result.scalar_one_or_none():
            sa = Admin(telegram_id=SUPERADMIN_ID, is_superadmin=True)
            session.add(sa)
            await session.commit()

    print("Bot started...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
