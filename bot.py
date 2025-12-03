###############################################################
#                     DRIVER QUEUE BOT (FULL)                 #
#                   Aiogram3 + PostgreSQL + Railway           #
#                        by ChatGPT                           #
###############################################################

import os, asyncio, logging
from datetime import datetime, date, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, BigInteger, String, Boolean, Date, Text, TIMESTAMP, select, delete

###############################################################
# INIT
###############################################################

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPERADMIN_ID = int(os.getenv("SUPERADMIN_ID"))
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

###############################################################
# DB
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
    admin_id = Column(BigInteger)
    admin_comment = Column(Text)

engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

###############################################################
# HELPERS
###############################################################

BACK = "⬅ Назад"

def back_kb(enable=True):
    return ReplyKeyboardRemove() if not enable else ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BACK)]], resize_keyboard=True
    )

async def is_admin(uid):
    async with SessionLocal() as s:
        r = await s.execute(select(Admin).where(Admin.telegram_id == uid))
        return r.scalar_one_or_none() is not None

async def is_superadmin(uid):
    async with SessionLocal() as s:
        r = await s.execute(
            select(Admin).where(Admin.telegram_id == uid, Admin.is_superadmin.is_(True))
        )
        return r.scalar_one_or_none() is not None

###############################################################
# INLINE CALENDAR
###############################################################

def build_calendar(year=None, month=None):
    now = datetime.now()
    year = year or now.year
    month = month or now.month
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text=f"📅 {datetime(year, month, 1).strftime('%B %Y')}", callback_data="ignore"))
    kb.row(*[types.InlineKeyboardButton(text=d, callback_data="ignore") for d in ["Mo","Tu","We","Th","Fr","Sa","Su"]])

    start = datetime(year, month, 1).weekday()
    if start != 0: kb.row(*[types.InlineKeyboardButton(text=" ", callback_data="ignore")] * start)

    days = (datetime(year + (month==12), (month%12)+1, 1) - timedelta(days=1)).day
    buf=[]
    for d in range(1, days+1):
        buf.append(types.InlineKeyboardButton(text=str(d), callback_data=f"cal_day_{year}_{month}_{d}"))
        if len(buf)==7: kb.row(*buf); buf=[]
    if buf: kb.row(*buf)

    pm = month-1 or 12; py = year-1 if month==1 else year
    nm = month+1 if month<12 else 1; ny = year+1 if month==12 else year

    kb.row(
        types.InlineKeyboardButton(text="⬅", callback_data=f"cal_prev_{py}_{pm}"),
        types.InlineKeyboardButton(text="Закрити", callback_data="cal_close"),
        types.InlineKeyboardButton(text="➡", callback_data=f"cal_next_{ny}_{nm}")
    )
    return kb.as_markup()

def hour_kb():
    kb=InlineKeyboardBuilder()
    for h in range(24): kb.button(text=f"{h:02d}", callback_data=f"hour_{h:02d}")
    kb.adjust(6); return kb.as_markup()

def minute_kb():
    kb=InlineKeyboardBuilder()
    for m in range(0,60,5): kb.button(text=f"{m:02d}", callback_data=f"min_{m:02d}")
    kb.adjust(6); return kb.as_markup()

###############################################################
# FSM STATES
###############################################################

class QueueForm(StatesGroup):
    supplier = State(); driver_name = State(); phone = State()
    car = State(); docs = State(); loading_type = State()
    calendar = State(); hour = State(); minute = State()

class AdminChangeForm(StatesGroup):
    calendar = State(); hour = State(); minute = State()

###############################################################
# START
###############################################################

@dp.message(CommandStart())
async def cmd_start(m: types.Message, state: FSMContext):
    await state.clear()
    kb = InlineKeyboardBuilder()
    kb.button(text="📄 Створити заявку", callback_data="new_request")
    if await is_admin(m.from_user.id):
        kb.button(text="📂 Панель адміністратора", callback_data="admin_panel")
    kb.adjust(1)
    await m.answer("Вітаю! Оберіть дію:", reply_markup=kb.as_markup())

###############################################################
# CREATE REQUEST
###############################################################

@dp.callback_query(F.data=="new_request")
async def new_request(cb, state):
    await state.clear()
    await cb.message.answer("🔹 Введіть постачальника:", reply_markup=back_kb(False))
    await state.set_state(QueueForm.supplier)

@dp.message(QueueForm.supplier)
async def step_supplier(m,state):
    t=m.text.strip()
    if not t: return await m.answer("Введіть постачальника.")
    await state.update_data(supplier=t)
    await m.answer("🔹 Введіть ПІБ водія:", reply_markup=back_kb())
    await state.set_state(QueueForm.driver_name)

@dp.message(QueueForm.driver_name)
async def step_driver(m,state):
    if m.text==BACK:
        await state.set_state(QueueForm.supplier)
        return await m.answer("🔹 Введіть постачальника:", reply_markup=back_kb(False))
    t=m.text.strip()
    if not t: return await m.answer("Введіть ПІБ водія.")
    await state.update_data(driver_name=t)
    await m.answer("🔹 Введіть номер телефону:", reply_markup=back_kb())
    await state.set_state(QueueForm.phone)

@dp.message(QueueForm.phone)
async def step_phone(m,state):
    if m.text==BACK:
        await state.set_state(QueueForm.driver_name)
        return await m.answer("🔹 Введіть ПІБ водія:", reply_markup=back_kb())
    t=m.text.strip()
    if not t: return await m.answer("Введіть номер телефону.")
    await state.update_data(phone=t)
    await m.answer("🔹 Введіть марку та номер авто:", reply_markup=back_kb())
    await state.set_state(QueueForm.car)

@dp.message(QueueForm.car)
async def step_car(m,state):
    if m.text==BACK:
        await state.set_state(QueueForm.phone)
        return await m.answer("🔹 Введіть номер телефону:", reply_markup=back_kb())
    t=m.text.strip()
    if not t: return await m.answer("Введіть авто.")
    await state.update_data(car=t)

    kb=InlineKeyboardBuilder()
    kb.button(text="📸 Завантажити фото документів", callback_data="photo_upload")
    kb.button(text="⏭ Пропустити", callback_data="photo_skip")
    kb.adjust(1)
    await m.answer("Завантажте фото або пропустіть:", reply_markup=kb.as_markup())
    await state.set_state(QueueForm.docs)

@dp.callback_query(QueueForm.docs, F.data=="photo_upload")
async def photo_upload(cb): await cb.message.answer("Надішліть фото:")

@dp.message(QueueForm.docs, F.photo)
async def photo_save(m,state):
    await state.update_data(docs_file_id=m.photo[-1].file_id)

@dp.callback_query(QueueForm.docs, F.data=="photo_skip")
async def skip_photo(cb,state):
    await state.update_data(docs_file_id=None)
    await ask_loading(cb.message, state)

@dp.callback_query(QueueForm.docs, F.data=="photo_done")
async def photo_done(cb,state): await ask_loading(cb.message, state)

async def ask_loading(msg,state):
    kb=InlineKeyboardBuilder()
    kb.button(text="📦 На палетах", callback_data="load_pal")
    kb.button(text="🧱 В розсип", callback_data="load_loose")
    kb.adjust(1)
    await msg.answer("Обери тип завантаження:", reply_markup=kb.as_markup())
    await state.set_state(QueueForm.loading_type)

@dp.callback_query(QueueForm.loading_type)
async def step_loading(cb,state):
    ld="Палети" if cb.data=="load_pal" else "Розсип"
    await state.update_data(loading_type=ld)
    await cb.message.answer("🔹 Оберіть дату:", reply_markup=build_calendar())
    await state.set_state(QueueForm.calendar)

###############################################################
# CALENDAR DRIVER
###############################################################

@dp.callback_query(QueueForm.calendar, F.data.startswith("cal_prev_"))
async def cal_prev(cb):
    _,_,y,m=cb.data.split("_")
    await cb.message.edit_reply_markup(build_calendar(int(y), int(m)))

@dp.callback_query(QueueForm.calendar, F.data.startswith("cal_next_"))
async def cal_next(cb):
    _,_,y,m=cb.data.split("_")
    await cb.message.edit_reply_markup(build_calendar(int(y), int(m)))

@dp.callback_query(QueueForm.calendar, F.data.startswith("cal_day_"))
async def cal_day(cb,state):
    _,_,y,m,d=cb.data.split("_")
    await state.update_data(date=date(int(y),int(m),int(d)))
    await cb.message.answer("⏰ Оберіть годину:", reply_markup=hour_kb())
    await state.set_state(QueueForm.hour)

@dp.callback_query(QueueForm.calendar, F.data=="cal_close")
async def cal_close(cb,state):
    await state.clear()
    await cb.message.answer("Вибір дати скасовано. /start")

###############################################################
# TIME SELECT DRIVER
###############################################################

@dp.callback_query(QueueForm.hour, F.data.startswith("hour_"))
async def step_hour(cb,state):
    await state.update_data(hour=cb.data.replace("hour_",""))
    await cb.message.answer("🕒 Оберіть хвилини:", reply_markup=minute_kb())
    await state.set_state(QueueForm.minute)

@dp.callback_query(QueueForm.minute, F.data.startswith("min_"))
async def step_min(cb,state):
    data=await state.get_data()
    minute=cb.data.replace("min_","")
    async with SessionLocal() as s:
        req=Request(
            user_id=cb.from_user.id,
            supplier=data["supplier"], driver_name=data["driver_name"],
            phone=data["phone"], car=data["car"],
            docs_file_id=data.get("docs_file_id"),
            loading_type=data["loading_type"],
            date=data["date"], time=f"{data['hour']}:{minute}",
            status="new"
        )
        s.add(req); await s.commit(); await s.refresh(req)
        rid=req.id

    await send_admin_request(rid)

    kb=InlineKeyboardBuilder()
    kb.button(text="📄 Нова заявка", callback_data="new_request")
    kb.adjust(1)

    await cb.message.answer(
        f"✅ <b>Заявку відправлено!</b>\n\n"
        f"№{rid}\n"
        f"🗓 {data['date'].strftime('%d.%m.%Y')}  ⏰ {data['hour']}:{minute}",
        reply_markup=kb.as_markup()
    )
    await state.clear()

###############################################################
# SEND ADMIN REQUEST
###############################################################

async def send_admin_request(rid):
    async with SessionLocal() as s:
        req=await s.get(Request,rid)
        if not req: return
        r=await s.execute(select(Admin))
        admins=r.scalars().all()

        text=(
            f"<b>📦 Нова заявка #{req.id}</b>\n"
            f"🏢 Постачальник: {req.supplier}\n"
            f"👤 Водій: {req.driver_name}\n"
            f"📞 Телефон: {req.phone}\n"
            f"🚚 Авто: {req.car}\n"
            f"🧱 Завантаження: {req.loading_type}\n"
            f"🗓 {req.date.strftime('%d.%m.%Y')}   ⏰ {req.time}"
        )

        for ad in admins:
            kb=InlineKeyboardBuilder()
            kb.button(text="✔ Підтвердити", callback_data=f"adm_ok_{req.id}")
            kb.button(text="🔁 Змінити дату/час", callback_data=f"adm_change_{req.id}")
            kb.button(text="❌ Відхилити", callback_data=f"adm_rej_{req.id}")
            kb.adjust(1)
            try:
                await bot.send_message(ad.telegram_id, text, reply_markup=kb.as_markup())
                if req.docs_file_id:
                    await bot.send_photo(ad.telegram_id, req.docs_file_id)
            except: pass

###############################################################
# ADMIN PANEL
###############################################################

@dp.callback_query(F.data=="admin_panel")
async def admin_panel(cb):
    if not await is_admin(cb.from_user.id):
        return await cb.answer("Немає доступу.", show_alert=True)

    kb=InlineKeyboardBuilder()
    kb.button(text="🆕 Нові заявки", callback_data="admin_new")
    kb.button(text="📁 Всі заявки", callback_data="admin_all")
    kb.button(text="🗑 Очистити БД", callback_data="admin_clean")
    kb.adjust(1)
    await cb.message.answer("📂 Адмін-панель:", reply_markup=kb.as_markup())

###############################################################
# VIEW REQUEST LISTS
###############################################################

async def show_requests(cb, only_new=False):
    async with SessionLocal() as s:
        if only_new:
            r=await s.execute(select(Request).where(Request.status=="new").order_by(Request.id.desc()))
        else:
            r=await s.execute(select(Request).order_by(Request.id.desc()))
        rows=r.scalars().all()

    if not rows:
        return await cb.message.answer("Немає заявок.")

    kb=InlineKeyboardBuilder()
    for rq in rows:
        kb.button(text=f"№{rq.id} • {rq.date} {rq.time}", callback_data=f"req_{rq.id}")
    kb.adjust(1)
    await cb.message.answer("Оберіть заявку:", reply_markup=kb.as_markup())

@dp.callback_query(F.data=="admin_new")
async def admin_new(cb): await show_requests(cb, only_new=True)

@dp.callback_query(F.data=="admin_all")
async def admin_all(cb): await show_requests(cb, only_new=False)

###############################################################
# OPEN REQUEST CARD
###############################################################

@dp.callback_query(F.data.startswith("req_"))
async def open_card(cb):
    rid=int(cb.data.split("_")[1])
    async with SessionLocal() as s:
        req=await s.get(Request,rid)
        if not req: return await cb.answer("Не знайдено.", show_alert=True)

    text=(
        f"<b>📄 Заявка #{req.id}</b>\n"
        f"🏢 Постачальник: {req.supplier}\n"
        f"👤 Водій: {req.driver_name}\n"
        f"📞 {req.phone}\n"
        f"🚚 {req.car}\n"
        f"🧱 {req.loading_type}\n"
        f"🗓 {req.date.strftime('%d.%m.%Y')}   ⏰ {req.time}\n"
        f"Статус: <b>{req.status}</b>"
    )

    kb=InlineKeyboardBuilder()
    kb.button(text="✔ Підтвердити", callback_data=f"adm_ok_{req.id}")
    kb.button(text="🔁 Змінити", callback_data=f"adm_change_{req.id}")
    kb.button(text="❌ Відхилити", callback_data=f"adm_rej_{req.id}")
    kb.adjust(1)

    await cb.message.answer(text, reply_markup=kb.as_markup())
    if req.docs_file_id: await cb.message.answer_photo(req.docs_file_id)

###############################################################
# ADMIN ACTIONS
###############################################################

async def notify_admins(text):
    async with SessionLocal() as s:
        r=await s.execute(select(Admin))
        for ad in r.scalars().all():
            try: await bot.send_message(ad.telegram_id, text)
            except: pass

@dp.callback_query(F.data.startswith("adm_ok_"))
async def adm_ok(cb):
    rid=int(cb.data.split("_")[2])
    async with SessionLocal() as s:
        req=await s.get(Request,rid)
        if not req: return await cb.answer("Не знайдено.")
        req.status="approved"; req.admin_id=cb.from_user.id
        await s.commit()

    await cb.message.answer("✔ Підтверджено.")
    await bot.send_message(req.user_id, f"🎉 Вашу заявку #{rid} підтверджено!")
    await notify_admins(f"⚡ Адмін {cb.from_user.id} підтвердив заявку #{rid}")

@dp.callback_query(F.data.startswith("adm_rej_"))
async def adm_rej(cb):
    rid=int(cb.data.split("_")[2])
    async with SessionLocal() as s:
        req=await s.get(Request,rid)
        if not req: return await cb.answer("Не знайдено.")
        req.status="rejected"; req.admin_id=cb.from_user.id
        await s.commit()

    await cb.message.answer("❌ Відхилено.")
    await bot.send_message(req.user_id, f"❗ Заявку #{rid} відхилено.")
    await notify_admins(f"⚠️ Адмін {cb.from_user.id} відхилив заявку #{rid}")

###############################################################
# CHANGE DATE/TIME
###############################################################

@dp.callback_query(F.data.startswith("adm_change_"))
async def adm_change(cb,state):
    rid=int(cb.data.split("_")[2])
    await state.clear(); await state.update_data(req_id=rid)
    await cb.message.answer("Оберіть нову дату:", reply_markup=build_calendar())
    await state.set_state(AdminChangeForm.calendar)

@dp.callback_query(AdminChangeForm.calendar, F.data.startswith("cal_day_"))
async def ac_day(cb,state):
    _,_,y,m,d=cb.data.split("_")
    await state.update_data(new_date=date(int(y),int(m),int(d)))
    await cb.message.answer("⏰ Оберіть годину:", reply_markup=hour_kb())
    await state.set_state(AdminChangeForm.hour)

@dp.callback_query(AdminChangeForm.hour, F.data.startswith("hour_"))
async def ac_hour(cb,state):
    await state.update_data(new_hour=cb.data.replace("hour_",""))
    await cb.message.answer("🕒 Оберіть хвилини:", reply_markup=minute_kb())
    await state.set_state(AdminChangeForm.minute)

@dp.callback_query(AdminChangeForm.minute, F.data.startswith("min_"))
async def ac_min(cb,state):
    data=await state.get_data()
    rid=data["req_id"]
    new_time=f"{data['new_hour']}:{cb.data.replace('min_','')}"
    async with SessionLocal() as s:
        req=await s.get(Request,rid)
        req.date=data["new_date"]; req.time=new_time
        req.status="approved"; req.admin_id=cb.from_user.id
        await s.commit()

    await cb.message.answer(f"🔁 Змінено: {req.date} {req.time}")
    await bot.send_message(req.user_id, f"ℹ️ Нова дата/час заявки #{rid}: {req.date} {req.time}")
    await notify_admins(f"🔧 Адмін {cb.from_user.id} змінив заявку #{rid}")
    await state.clear()

###############################################################
# ADMIN DB CLEAN
###############################################################

@dp.callback_query(F.data=="admin_clean")
async def admin_clean(cb):
    if not await is_superadmin(cb.from_user.id):
        return await cb.answer("Тільки суперадмін.", show_alert=True)

    kb=InlineKeyboardBuilder()
    kb.button(text="🗑 Очистити ВСІ заявки", callback_data="clean_all")
    kb.button(text="🗑 Очистити ОБРОБЛЕНІ", callback_data="clean_done")
    kb.adjust(1)
    await cb.message.answer("Оберіть варіант очищення:", reply_markup=kb.as_markup())

@dp.callback_query(F.data=="clean_all")
async def clean_all(cb):
    async with SessionLocal() as s:
        await s.execute(delete(Request)); await s.commit()
    await cb.message.answer("🗑 ВСІ заявки видалено.")
    await notify_admins("⚠️ СУПЕРАДМІН очистив ВСЮ базу!")

@dp.callback_query(F.data=="clean_done")
async def clean_done(cb):
    async with SessionLocal() as s:
        await s.execute(delete(Request).where(Request.status!="new"))
        await s.commit()
    await cb.message.answer("🧹 Оброблені заявки видалено.")
    await notify_admins("ℹ️ Суперадмін очистив оброблені заявки.")

###############################################################
# SUPERADMIN ADMIN MANAGEMENT
###############################################################

@dp.message(Command("add_admin"))
async def add_admin_cmd(m):
    if not await is_superadmin(m.from_user.id):
        return await m.answer("⛔ Немає доступу.")
    parts=m.text.split()
    if len(parts)!=2: return await m.answer("/add_admin 123456789")
    try: tid=int(parts[1])
    except: return await m.answer("ID має бути числом.")
    async with SessionLocal() as s:
        r=await s.execute(select(Admin).where(Admin.telegram_id==tid))
        if r.scalar_one_or_none(): return await m.answer("Вже адмін.")
        s.add(Admin(telegram_id=tid)); await s.commit()
    await m.answer("Додано.")

@dp.message(Command("remove_admin"))
async def remove_admin_cmd(m):
    if not await is_superadmin(m.from_user.id):
        return await m.answer("⛔ Немає доступу.")
    parts=m.text.split()
    if len(parts)!=2: return await m.answer("/remove_admin 123456789")
    try: tid=int(parts[1])
    except: return await m.answer("ID має бути числом.")
    async with SessionLocal() as s:
        await s.execute(delete(Admin).where(Admin.telegram_id==tid)); await s.commit()
    await m.answer("Видалено.")

@dp.message(Command("admins"))
async def admins_cmd(m):
    if not await is_superadmin(m.from_user.id):
        return await m.answer("⛔")
    async with SessionLocal() as s:
        r=await s.execute(select(Admin)); rows=r.scalars().all()
    txt="📋 <b>Адміни:</b>\n"+"\n".join(
        [f"{a.telegram_id} ({'SUPERADMIN' if a.is_superadmin else 'admin'})" for a in rows]
    )
    await m.answer(txt)

###############################################################
# START BOT
###############################################################

async def main():
    await init_db()
    async with SessionLocal() as s:
        r=await s.execute(select(Admin).where(Admin.telegram_id==SUPERADMIN_ID))
        if not r.scalar_one_or_none():
            s.add(Admin(telegram_id=SUPERADMIN_ID, is_superadmin=True))
            await s.commit()
    print("Bot started...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
