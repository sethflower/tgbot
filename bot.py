import asyncio
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, FSInputFile
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder
import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


# ----------------------
# 📌 FSM состояния
# ----------------------
class QueueForm(StatesGroup):
    supplier = State()
    driver_name = State()
    driver_phone = State()
    car_number = State()
    cargo_type = State()
    docs = State()
    load_type = State()
    plan_date = State()
    plan_time = State()


# ----------------------
# Команда /start
# ----------------------
@dp.message(Command("start"))
async def start(message: Message):
    await message.answer(
        "👋 Вітаю! Це бот для запису в електронну чергу.\n"
        "Натисніть кнопку нижче щоб створити заявку.",
        reply_markup=start_keyboard()
    )


def start_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="🆕 Створити заявку", callback_data="new_request")
    return kb.as_markup()


# ----------------------
# Старт создания заявки
# ----------------------
@dp.callback_query(F.data == "new_request")
async def new_request(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Введіть Постачальника:")
    await state.set_state(QueueForm.supplier)


@dp.message(QueueForm.supplier)
async def supplier(message: Message, state: FSMContext):
    await state.update_data(supplier=message.text)
    await message.answer("ПІБ водія:")
    await state.set_state(QueueForm.driver_name)


@dp.message(QueueForm.driver_name)
async def driver_name(message: Message, state: FSMContext):
    await state.update_data(driver_name=message.text)
    await message.answer("Телефон водія:")
    await state.set_state(QueueForm.driver_phone)


@dp.message(QueueForm.driver_phone)
async def driver_phone(message: Message, state: FSMContext):
    await state.update_data(driver_phone=message.text)
    await message.answer("Марка / Держ. номер авто:")
    await state.set_state(QueueForm.car_number)


@dp.message(QueueForm.car_number)
async def car_number(message: Message, state: FSMContext):
    await state.update_data(car_number=message.text)
    await message.answer("Вид вантажу:")
    await state.set_state(QueueForm.cargo_type)


@dp.message(QueueForm.cargo_type)
async def cargo_type(message: Message, state: FSMContext):
    await state.update_data(cargo_type=message.text)
    await message.answer("Завантажте документ (фото/скан):")
    await state.set_state(QueueForm.docs)


@dp.message(QueueForm.docs, F.photo)
async def docs(message: Message, state: FSMContext):
    file_id = message.photo[-1].file_id
    await state.update_data(docs=file_id)

    kb = InlineKeyboardBuilder()
    kb.button(text="На палетах", callback_data="load_pallet")
    kb.button(text="В розсип", callback_data="load_bulk")

    await message.answer("Оберіть тип завантаження:", reply_markup=kb.as_markup())
    await state.set_state(QueueForm.load_type)


@dp.callback_query(QueueForm.load_type)
async def load_type(callback: CallbackQuery, state: FSMContext):
    load_value = "На палетах" if callback.data == "load_pallet" else "В розсип"
    await state.update_data(load_type=load_value)

    await callback.message.answer("Планова дата вивантаження (формат: 2025-11-29):")
    await state.set_state(QueueForm.plan_date)


@dp.message(QueueForm.plan_date)
async def plan_date(message: Message, state: FSMContext):
    await state.update_data(plan_date=message.text)
    await message.answer("Плановий час вивантаження (формат: 10:00):")
    await state.set_state(QueueForm.plan_time)


# ----------------------
# Финал: отправка админу
# ----------------------
@dp.message(QueueForm.plan_time)
async def finish(message: Message, state: FSMContext):
    await state.update_data(plan_time=message.text)
    data = await state.get_data()

    # Сообщение админу
    text = (
        "📩 *Нова заявка на вивантаження*\n\n"
        f"🏭 Постачальник: {data['supplier']}\n"
        f"👨‍✈️ Водій: {data['driver_name']}\n"
        f"📞 Телефон: {data['driver_phone']}\n"
        f"🚚 Авто: {data['car_number']}\n"
        f"📦 Вид вантажу: {data['cargo_type']}\n"
        f"📄 Документи: прикріплено\n"
        f"⚙️ Тип завантаження: {data['load_type']}\n"
        f"📅 План дата: {data['plan_date']}\n"
        f"⏰ План час: {data['plan_time']}\n"
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Підтвердити", callback_data=f"confirm:{message.from_user.id}")
    kb.button(text="🕒 Змінити", callback_data=f"change:{message.from_user.id}")
    kb.adjust(1)

    await bot.send_photo(
        ADMIN_ID,
        photo=data["docs"],
        caption=text,
        reply_markup=kb.as_markup(),
        parse_mode="Markdown"
    )

    await message.answer("Заявку відправлено адміністратору! Очікуйте підтвердження.")
    await state.clear()


# ----------------------
# Кнопки админа
# ----------------------
@dp.callback_query(F.data.startswith("confirm:"))
async def admin_confirm(callback: CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    await bot.send_message(user_id, "✅ Ваша заявка *підтверджена!*", parse_mode="Markdown")
    await callback.answer("Підтверджено")
    await callback.message.edit_reply_markup()


@dp.callback_query(F.data.startswith("change:"))
async def admin_change(callback: CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    await bot.send_message(user_id, "🕒 Адміністратор змінив запланований час. Очікуйте нового повідомлення.")
    await callback.answer("Ок, зміню.")
    await callback.message.edit_reply_markup()


# ----------------------
# Запуск бота
# ----------------------
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
