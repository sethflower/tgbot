import os
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


# ----------- FSM Состояния водителя --------------
class QueueForm(StatesGroup):
    supplier = State()
    driver_name = State()
    phone = State()
    car = State()
    cargo_type = State()
    docs = State()
    loading_type = State()
    date = State()
    time = State()


# ----------- START --------------
@dp.message(Command("start"))
async def start(message: types.Message, state: FSMContext):
    await message.answer("Привіт! Це бот електронної черги.\nНатисни /new щоб створити заявку.")
    await state.clear()


# ----------- /NEW – новая заявка --------------
@dp.message(Command("new"))
async def new_request(message: types.Message, state: FSMContext):
    await message.answer("Введіть *Постачальника*:", parse_mode="Markdown")
    await state.set_state(QueueForm.supplier)


@dp.message(QueueForm.supplier)
async def supplier_entered(message: types.Message, state: FSMContext):
    await state.update_data(supplier=message.text)
    await message.answer("Введіть ПІБ водія:")
    await state.set_state(QueueForm.driver_name)


@dp.message(QueueForm.driver_name)
async def driver_name_entered(message: types.Message, state: FSMContext):
    await state.update_data(driver_name=message.text)
    await message.answer("Введіть телефон водія:")
    await state.set_state(QueueForm.phone)


@dp.message(QueueForm.phone)
async def phone_entered(message: types.Message, state: FSMContext):
    await state.update_data(phone=message.text)
    await message.answer("Введіть марку та держ. номер авто:")
    await state.set_state(QueueForm.car)


@dp.message(QueueForm.car)
async def car_entered(message: types.Message, state: FSMContext):
    await state.update_data(car=message.text)
    await message.answer("Введіть вид грузу:")
    await state.set_state(QueueForm.cargo_type)


@dp.message(QueueForm.cargo_type)
async def cargo_type_entered(message: types.Message, state: FSMContext):
    await state.update_data(cargo_type=message.text)
    await message.answer("Завантажте фото/скан документів:")
    await state.set_state(QueueForm.docs)


@dp.message(QueueForm.docs, F.photo)
async def docs_entered(message: types.Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    await state.update_data(docs=photo_id)
    kb = InlineKeyboardBuilder()
    kb.button(text="На палетах", callback_data="palettes")
    kb.button(text="В розсип", callback_data="loose")
    kb.adjust(2)
    await message.answer("Оберіть тип завантаження:", reply_markup=kb.as_markup())
    await state.set_state(QueueForm.loading_type)


@dp.callback_query(QueueForm.loading_type)
async def loading_type_selected(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(loading_type=callback.data)
    await callback.message.answer("Введіть планову дату (формат: 2025-12-01):")
    await state.set_state(QueueForm.date)


@dp.message(QueueForm.date)
async def date_entered(message: types.Message, state: FSMContext):
    await state.update_data(date=message.text)
    await message.answer("Введіть плановий час (наприклад 10:00):")
    await state.set_state(QueueForm.time)


@dp.message(QueueForm.time)
async def time_entered(message: types.Message, state: FSMContext):
    await state.update_data(time=message.text)

    data = await state.get_data()

    text = (
        f"📌 *Нова заявка від водія*\n"
        f"Постачальник: {data['supplier']}\n"
        f"ПІБ: {data['driver_name']}\n"
        f"Телефон: {data['phone']}\n"
        f"Авто: {data['car']}\n"
        f"Вид грузу: {data['cargo_type']}\n"
        f"Тип завантаження: {data['loading_type']}\n"
        f"План дата: {data['date']}\n"
        f"План час: {data['time']}\n"
    )

    # отправляем админу
    kb = InlineKeyboardBuilder()
    kb.button(text="Підтвердити", callback_data="approve")
    kb.button(text="Змінити дату/час", callback_data="change")
    kb.button(text="Відхилити", callback_data="reject")
    kb.adjust(1)

    await bot.send_message(
        ADMIN_ID,
        text,
        parse_mode="Markdown",
        reply_markup=kb.as_markup()
    )

    await bot.send_photo(ADMIN_ID, data["docs"])

    await message.answer("Дякую! Заявку відправлено адміністратору.")
    await state.clear()


# ----------- РЕАКЦИЯ АДМИНА -----------------
@dp.callback_query(F.data == "approve")
async def approve(callback: types.CallbackQuery):
    await callback.message.answer("Заявка підтверджена ✔️")
    # тут можно отправить водителю, если мы сохраним его ID


@dp.callback_query(F.data == "reject")
async def reject(callback: types.CallbackQuery):
    await callback.message.answer("Заявку відхилено ❌")


@dp.callback_query(F.data == "change")
async def change(callback: types.CallbackQuery):
    await callback.message.answer("Напишіть нову дату/час повідомленням.")


# ----------- ЗАПУСК -----------------
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
