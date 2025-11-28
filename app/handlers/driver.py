from datetime import date
from typing import Any, Dict

from aiogram import F, Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

from app import keyboards
from app.config import settings
from app.database import Database

router = Router()


class RequestForm(StatesGroup):
    supplier = State()
    driver_name = State()
    driver_phone = State()
    car_info = State()
    cargo_type = State()
    document = State()
    loading_type = State()
    planned_date = State()
    planned_time = State()


async def _save_request(message: Message, state: FSMContext, db: Database) -> None:
    data = await state.get_data()
    payload: Dict[str, Any] = {
        "user_id": message.from_user.id,
        "supplier": data["supplier"],
        "driver_name": data["driver_name"],
        "driver_phone": data["driver_phone"],
        "car_info": data["car_info"],
        "cargo_type": data["cargo_type"],
        "document_file_id": data.get("document_file_id"),
        "loading_type": data["loading_type"],
        "planned_date": data["planned_date"],
        "planned_time": data["planned_time"],
    }
    request_id = await db.add_request(payload)
    await state.clear()
    await message.answer(
        f"Дякуємо! Заявка #{request_id} створена. Адміністратор розгляне її найближчим часом.",
        reply_markup=keyboards.main_menu(),
    )

    preview = (
        f"🆕 Нова заявка #{request_id}\n"
        f"Постачальник: {payload['supplier']}\n"
        f"ПІБ: {payload['driver_name']}\n"
        f"Телефон: {payload['driver_phone']}\n"
        f"Авто: {payload['car_info']}\n"
        f"Вантаж: {payload['cargo_type']}\n"
        f"Тип завантаження: {payload['loading_type']}\n"
        f"План: {payload['planned_date']} {payload['planned_time']}\n"
    )
    for admin_id in settings.admin_ids:
        await message.bot.send_message(admin_id, preview, reply_markup=keyboards.status_buttons(request_id))
        if payload.get("document_file_id"):
            await message.bot.send_document(admin_id, payload["document_file_id"], caption=f"Документи #{request_id}")


@router.message(CommandStart())
async def start(message: Message, state: FSMContext) -> None:
    await state.clear()
    text = (
        "👋 Вітаємо в електронній черзі водіїв!\n"
        "Натисніть \"Створити заявку\", щоб подати дані на в'їзд."
    )
    await message.answer(text, reply_markup=keyboards.main_menu())


@router.message(F.text == "🚛 Створити заявку")
async def create_request(message: Message, state: FSMContext) -> None:
    await state.set_state(RequestForm.supplier)
    await message.answer("Вкажіть постачальника:")


@router.message(RequestForm.supplier)
async def supplier(message: Message, state: FSMContext) -> None:
    await state.update_data(supplier=message.text)
    await state.set_state(RequestForm.driver_name)
    await message.answer("ПІБ водія:")


@router.message(RequestForm.driver_name)
async def driver_name(message: Message, state: FSMContext) -> None:
    await state.update_data(driver_name=message.text)
    await state.set_state(RequestForm.driver_phone)
    await message.answer("Телефон водія (+380...):")


@router.message(RequestForm.driver_phone)
async def driver_phone(message: Message, state: FSMContext) -> None:
    await state.update_data(driver_phone=message.text)
    await state.set_state(RequestForm.car_info)
    await message.answer("Марка/Держ. номер авто:")


@router.message(RequestForm.car_info)
async def car_info(message: Message, state: FSMContext) -> None:
    await state.update_data(car_info=message.text)
    await state.set_state(RequestForm.cargo_type)
    await message.answer("Вид вантажу:")


@router.message(RequestForm.cargo_type)
async def cargo_type(message: Message, state: FSMContext) -> None:
    await state.update_data(cargo_type=message.text)
    await state.set_state(RequestForm.document)
    await message.answer("Завантажте фото/скан документів (jpg/png/pdf).")


@router.message(RequestForm.document, F.document)
async def document_upload(message: Message, state: FSMContext) -> None:
    await state.update_data(document_file_id=message.document.file_id)
    await state.set_state(RequestForm.loading_type)
    await message.answer("Тип завантаження (палети / россыпь):")


@router.message(RequestForm.document)
async def document_skip(message: Message, state: FSMContext) -> None:
    await state.update_data(document_file_id=None)
    await state.set_state(RequestForm.loading_type)
    await message.answer("Тип завантаження (палети / россыпь):")


@router.message(RequestForm.loading_type)
async def loading_type(message: Message, state: FSMContext) -> None:
    await state.update_data(loading_type=message.text)
    await state.set_state(RequestForm.planned_date)
    await message.answer("Оберіть планову дату:", reply_markup=keyboards.calendar_keyboard(date.today()))


@router.callback_query(F.data.startswith("cal:"))
async def process_calendar(callback: CallbackQuery, state: FSMContext) -> None:
    _, action, payload = callback.data.split(":", maxsplit=2)
    data = await state.get_state()
    if not data:
        await callback.answer()
        return

    current = date.fromisoformat(payload)
    if action == "prev":
        new_date = (current.replace(day=1) - date.resolution).replace(day=1)
        await callback.message.edit_reply_markup(reply_markup=keyboards.calendar_keyboard(new_date))
    elif action == "next":
        if current.month == 12:
            new_date = current.replace(year=current.year + 1, month=1, day=1)
        else:
            new_date = current.replace(month=current.month + 1, day=1)
        await callback.message.edit_reply_markup(reply_markup=keyboards.calendar_keyboard(new_date))
    elif action == "pick":
        await state.update_data(planned_date=payload)
        await state.set_state(RequestForm.planned_time)
        await callback.message.answer("Оберіть плановий час:", reply_markup=keyboards.time_keyboard())
        await callback.message.delete()
    await callback.answer()


@router.callback_query(F.data.startswith("time:"))
async def choose_time(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    await state.update_data(planned_time=callback.data.split(":", maxsplit=1)[1])
    await callback.message.delete_reply_markup()
    await callback.message.answer("Підтверджую заявку, відправляю адміністратору.")
    await _save_request(callback.message, state, db)
    await callback.answer("Час збережено")
