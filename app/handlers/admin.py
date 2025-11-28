from datetime import date
from typing import List

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from app import keyboards
from app.config import settings
from app.database import (
    Database,
    STATUS_CANCELLED,
    STATUS_CONFIRMED,
    STATUS_DONE,
    STATUS_RESCHEDULED,
)
from app.utils.export import export_requests

router = Router()


def is_admin(user_id: int) -> bool:
    return user_id in settings.admin_ids


def _prefix(request_id: int) -> str:
    return f"r{request_id}"


async def _render_request_text(record: dict) -> str:
    return (
        f"Заявка #{record['id']} ({record['status']})\n"
        f"Постачальник: {record['supplier']}\n"
        f"ПІБ: {record['driver_name']}\n"
        f"Телефон: {record['driver_phone']}\n"
        f"Авто: {record['car_info']}\n"
        f"Вантаж: {record['cargo_type']}\n"
        f"Тип завантаження: {record['loading_type']}\n"
        f"План: {record['planned_date']} {record['planned_time']}"
    )


@router.message(Command("admin"))
async def admin_panel(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("У вас немає доступу до адмін-панелі.")
        return
    await message.answer("Адмін-панель", reply_markup=keyboards.admin_menu())


@router.message(F.text == "🗂 Список заявок")
async def list_requests(message: Message, db: Database) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("У вас немає доступу.")
        return
    records = await db.list_requests()
    if not records:
        await message.answer("Заявок поки немає.")
        return
    ids: List[int] = [r["id"] for r in records]
    await message.answer("Оберіть заявку:", reply_markup=keyboards.requests_list(ids))


@router.callback_query(F.data.startswith("open:"))
async def open_request(callback: CallbackQuery, db: Database) -> None:
    request_id = int(callback.data.split(":", maxsplit=1)[1])
    record = await db.get_request(request_id)
    if not record:
        await callback.answer("Заявка не знайдена")
        return
    text = await _render_request_text(record)
    await callback.message.answer(text, reply_markup=keyboards.status_buttons(request_id))
    await callback.answer()


@router.callback_query(F.data.startswith("confirm:"))
async def confirm(callback: CallbackQuery, db: Database) -> None:
    request_id = int(callback.data.split(":", maxsplit=1)[1])
    await db.update_status(request_id, STATUS_CONFIRMED)
    record = await db.get_request(request_id)
    if record:
        await callback.bot.send_message(
            record["user_id"],
            f"Ваша заявка підтверджена на {record['planned_time']} {record['planned_date']}",
        )
    await callback.answer("Підтверджено")


@router.callback_query(F.data.startswith("cancel:"))
async def cancel(callback: CallbackQuery, db: Database) -> None:
    request_id = int(callback.data.split(":", maxsplit=1)[1])
    await db.update_status(request_id, STATUS_CANCELLED)
    record = await db.get_request(request_id)
    if record:
        await callback.bot.send_message(record["user_id"], "Заявку скасовано адміністратором.")
    await callback.answer("Скасовано")


@router.callback_query(F.data.startswith("done:"))
async def done(callback: CallbackQuery, db: Database) -> None:
    request_id = int(callback.data.split(":", maxsplit=1)[1])
    await db.set_done(request_id)
    await callback.answer("Позначено як завершено")


@router.callback_query(F.data.startswith("reschedule:"))
async def reschedule(callback: CallbackQuery) -> None:
    request_id = int(callback.data.split(":", maxsplit=1)[1])
    prefix = _prefix(request_id)
    await callback.message.answer(
        f"Оберіть нову дату для заявки #{request_id}",
        reply_markup=keyboards.calendar_keyboard(date.today(), prefix=prefix),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("r"))
async def reschedule_calendar(callback: CallbackQuery) -> None:
    # Format: r<id>:action:payload
    parts = callback.data.split(":")
    if len(parts) < 3:
        await callback.answer()
        return
    prefix = parts[0]
    request_id = int(prefix[1:])
    action = parts[1]
    payload = parts[2]
    current = date.fromisoformat(payload) if payload.count("-") == 2 else date.today()

    if action == "prev":
        new_date = (current.replace(day=1) - date.resolution).replace(day=1)
        await callback.message.edit_reply_markup(reply_markup=keyboards.calendar_keyboard(new_date, prefix=prefix))
    elif action == "next":
        if current.month == 12:
            new_date = current.replace(year=current.year + 1, month=1, day=1)
        else:
            new_date = current.replace(month=current.month + 1, day=1)
        await callback.message.edit_reply_markup(reply_markup=keyboards.calendar_keyboard(new_date, prefix=prefix))
    elif action == "pick":
        await callback.message.answer(
            f"Оберіть час для заявки #{request_id}",
            reply_markup=keyboards.time_keyboard(prefix=f"t{request_id}:{payload}"),
        )
        await callback.message.delete()
    await callback.answer()


@router.callback_query(F.data.startswith("t"))
async def reschedule_time(callback: CallbackQuery, db: Database) -> None:
    # Format: t<id>:YYYY-MM-DD:HH:MM
    parts = callback.data.split(":")
    if len(parts) < 3:
        await callback.answer()
        return
    request_id = int(parts[0][1:])
    planned_date = parts[1]
    planned_time = parts[2]
    await db.update_status(request_id, STATUS_RESCHEDULED, planned_date, planned_time)
    record = await db.get_request(request_id)
    if record:
        await callback.bot.send_message(
            record["user_id"],
            f"Ваша заявка перенесена на {planned_time} {planned_date}",
        )
    await callback.answer("Перенесено")


@router.message(F.text == "📤 Експорт в Excel")
async def export_excel(message: Message, db: Database) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Доступ заборонено")
        return
    path = await export_requests(db)
    await message.answer_document(open(path, "rb"), caption="Експорт заявок")
