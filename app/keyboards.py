import calendar
from datetime import date
from typing import Iterable

from aiogram.types import InlineKeyboardMarkup, ReplyKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder


MAIN_MENU_TEXT = "Виберіть дію"


def main_menu() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.button(text="🚛 Створити заявку")
    builder.button(text="📋 Мої заявки")
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True)


def admin_menu() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.button(text="🗂 Список заявок")
    builder.button(text="📤 Експорт в Excel")
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True)


def status_buttons(request_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Підтвердити", callback_data=f"confirm:{request_id}")
    builder.button(text="🕒 Змінити дату/час", callback_data=f"reschedule:{request_id}")
    builder.button(text="❌ Скасувати", callback_data=f"cancel:{request_id}")
    builder.button(text="✅ Завершити", callback_data=f"done:{request_id}")
    builder.adjust(2)
    return builder.as_markup()


def requests_list(request_ids: Iterable[int]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for req_id in request_ids:
        builder.button(text=f"Заявка #{req_id}", callback_data=f"open:{req_id}")
    builder.adjust(1)
    return builder.as_markup()


def calendar_keyboard(target: date, prefix: str = "cal") -> InlineKeyboardMarkup:
    cal = calendar.Calendar(firstweekday=0)
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️", callback_data=f"{prefix}:prev:{target.isoformat()}")
    builder.button(text=target.strftime("%B %Y"), callback_data=f"{prefix}:ignore")
    builder.button(text="▶️", callback_data=f"{prefix}:next:{target.isoformat()}")

    month_days = cal.monthdayscalendar(target.year, target.month)
    for week in month_days:
        for day in week:
            if day == 0:
                builder.button(text=" ", callback_data=f"{prefix}:ignore")
            else:
                chosen = target.replace(day=day)
                builder.button(text=str(day), callback_data=f"{prefix}:pick:{chosen.isoformat()}")
    builder.adjust(3, 7, 7, 7, 7, 7, 7)
    return builder.as_markup()


def time_keyboard(prefix: str = "time") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for hour in range(8, 19):
        builder.button(text=f"{hour:02d}:00", callback_data=f"{prefix}:{hour:02d}:00")
        builder.button(text=f"{hour:02d}:30", callback_data=f"{prefix}:{hour:02d}:30")
    builder.adjust(4)
    return builder.as_markup()
