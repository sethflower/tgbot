import os
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram import Router
from aiogram import html

import asyncio

# -------------------------------------------------------------
# Настройки
# -------------------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))  # ID администратора

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Хранилище заявок (в памяти)
requests_db = {}

# -------------------------------------------------------------
# Машина состояний для водителя
# -------------------------------------------------------------
class Form(StatesGroup):
    supplier = State()
    driver_name = State()
    driver_phone = State()
    car_info = State()
    cargo_type = State()
    doc_file = State()
    load_type = State()
    plan_date = State()
    plan_time = State()

# -------------------------------------------------------------
# Старт
# -------------------------------------------------------------
@dp.message(Command("start"))
async def start_cmd(msg: types.Message):
    await msg.answer("👋 Вітаю! Це бот запису в електронну чергу.\n"
                     "Натисніть /queue щоб створити заявку.")

# -------------------------------------------------------------
# Создание заявки
# -------------------------------------------------------------
@dp.message(Command("queue"))
async def queue_cmd(msg: types.Message, state: FSMContext):
    await msg.answer("Постачальник:")
    await state.set_state(Form.supplier)

@dp.message(Form.supplier)
async def supplier_step(msg: types.Message, state: FSMContext):
    await state.update_data(supplier=msg.text)
    await msg.answer("ПІБ водія:")
    await state.set_state(Form.driver_name)

@dp.message(Form.driver_name)
async def driver_name_step(msg: types.Message, state: FSMContext):
    await state.update_data(driver_name=msg.text)
    await msg.answer("Телефон водія:")
    await state.set_state(Form.driver_phone)

@dp.message(Form.driver_phone)
async def phone_step(msg: types.Message, state: FSMContext):
    await state.update_data(driver_phone=msg.text)
    await msg.answer("Марка/держ. номер авто:")
    await state.set_state(Form.car_info)

@dp.message(Form.car_info)
async def car_step(msg: types.Message, state: FSMContext):
    await state.update_data(car_info=msg.text)

    kb = InlineKeyboardBuilder()
    kb.button(text="Велика габаритна техніка", callback_data="cargo_big")
    kb.button(text="Мала техніка", callback_data="cargo_small")
    kb.adjust(1)

    await msg.answer("Вид груза:", reply_markup=kb.as_markup())
    await state.set_state(Form.cargo_type)

@dp.callback_query(Form.cargo_type)
async def cargo_selected(callback: types.CallbackQuery, state: FSMContext):
    cargo = "Велика габаритна" if callback.data == "cargo_big" else "Мала техніка"
    await state.update_data(cargo_type=cargo)
    await callback.message.answer("Завантажте фото/скан документів (1 файл):")
    await state.set_state(Form.doc_file)
    await callback.answer()

@dp.message(Form.doc_file, F.document)
async def docs_step(msg: types.Message, state: FSMContext):
    file_id = msg.document.file_id
    await state.update_data(doc_file=file_id)

    kb = InlineKeyboardBuilder()
    kb.button(text="На палетах", callback_data="lt_pallet")
    kb.button(text="В розсип", callback_data="lt_bulk")
    kb.adjust(1)

    await msg.answer("Тип завантаження:", reply_markup=kb.as_markup())
    await state.set_state(Form.load_type)

@dp.callback_query(Form.load_type)
async def load_type_step(callback: types.CallbackQuery, state: FSMContext):
    load_type = "Палети" if callback.data == "lt_pallet" else "В розсип"
    await state.update_data(load_type=load_type)

    await callback.message.answer("План дата вивантаження (дд.мм.рррр):")
    await state.set_state(Form.plan_date)
    await callback.answer()

@dp.message(Form.plan_date)
async def date_step(msg: types.Message, state: FSMContext):
    await state.update_data(plan_date=msg.text)
    await msg.answer("План час вивантаження (год:хв):")
    await state.set_state(Form.plan_time)

@dp.message(Form.plan_time)
async def finish_step(msg: types.Message, state: FSMContext):
    await state.update_data(plan_time=msg.text)
    data = await state.get_data()

    # Сохраняем в "базу"
    request_id = len(requests_db) + 1
    requests_db[request_id] = {
        "user_id": msg.from_user.id,
        **data
    }

    # Отправка админу
    text = (
        f"📌 *Нова заявка #{request_id}*\n\n"
        f"Постачальник: {html.bold(data['supplier'])}\n"
        f"ПІБ водія: {data['driver_name']}\n"
        f"Телефон: {data['driver_phone']}\n"
        f"Авто: {data['car_info']}\n"
        f"Вид груза: {data['cargo_type']}\n"
        f"Тип: {data['load_type']}\n"
        f"План дата: {data['plan_date']}\n"
        f"План час: {data['plan_time']}\n"
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="Підтвердити", callback_data=f"approve_{request_id}")
    kb.button(text="Змінити", callback_data=f"edit_{request_id}")
    kb.button(text="Відмовити", callback_data=f"decline_{request_id}")
    kb.adjust(1)

    await bot.send_message(ADMIN_ID, text, parse_mode="HTML", reply_markup=kb.as_markup())

    # Уведомляем водителя
    await msg.answer("✅ Заявку відправлено адміністратору! Очікуйте підтвердження.")
    await state.clear()

# -------------------------------------------------------------
# Обработка решения администратора
# -------------------------------------------------------------
@dp.callback_query(F.data.startswith("approve_"))
async def approve(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[1])
    user_id = requests_db[req_id]["user_id"]
    await bot.send_message(user_id, f"✅ Ваша заявка #{req_id} підтверджена!")
    await callback.message.edit_text(f"Заявка #{req_id} підтверджена ✔️")
    await callback.answer()

@dp.callback_query(F.data.startswith("decline_"))
async def decline(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[1])
    user_id = requests_db[req_id]["user_id"]
    await bot.send_message(user_id, f"❌ Ваша заявка #{req_id} *відхилена*.", parse_mode="HTML")
    await callback.message.edit_text(f"Заявка #{req_id} відхилена ❌")
    await callback.answer()

@dp.callback_query(F.data.startswith("edit_"))
async def edit(callback: types.CallbackQuery, state: FSMContext):
    req_id = int(callback.data.split("_")[1])

    await state.update_data(edit_id=req_id)
    await callback.message.answer("Введіть нову дату (дд.мм.рррр):")
    await callback.answer()
    await state.set_state(Form.plan_date)

@dp.message(Form.plan_date)
async def new_date(msg: types.Message, state: FSMContext):
    data = await state.get_data()
    if "edit_id" not in data:
        return

    req_id = data["edit_id"]
    requests_db[req_id]["plan_date"] = msg.text

    await msg.answer("Введіть новий час (год:хв):")
    await state.set_state(Form.plan_time)

@dp.message(Form.plan_time)
async def new_time(msg: types.Message, state: FSMContext):
    data = await state.get_data()
    req_id = data["edit_id"]

    requests_db[req_id]["plan_time"] = msg.text
    user_id = requests_db[req_id]["user_id"]

    await bot.send_message(user_id,
        f"ℹ️ Ваша заявка #{req_id} була оновлена.\n"
        f"Нова дата: {requests_db[req_id]['plan_date']}\n"
        f"Новий час: {requests_db[req_id]['plan_time']}")

    await msg.answer("✔️ Дані оновлено.")
    await state.clear()

# -------------------------------------------------------------
# Запуск бота
# -------------------------------------------------------------
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
