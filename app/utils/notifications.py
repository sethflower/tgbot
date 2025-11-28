from datetime import datetime

from aiogram import Bot

from app.database import Database, STATUS_DONE


async def notify_upcoming(bot: Bot, db: Database) -> None:
    requests = await db.list_pending_notifications()
    for r in requests:
        planned_at = f"{r['planned_time']} {r['planned_date']}"
        await bot.send_message(
            r["user_id"],
            f"Нагадування: ваш в'їзд заплановано о {planned_at} (через годину)",
        )
        await db.add_log(r["user_id"], f"Notification sent for request #{r['id']}")


async def mark_done_if_past(bot: Bot, db: Database) -> None:
    now = datetime.utcnow().isoformat()
    records = await db.list_requests(limit=500)
    for r in records:
        planned = f"{r['planned_date']} {r['planned_time']}"
        if r["status"] != STATUS_DONE and planned < now:
            await db.set_done(r["id"])
