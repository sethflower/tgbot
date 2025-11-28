import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.config import settings
from app.database import Database
from app.handlers import admin, driver
from app.middlewares import DbMiddleware
from app.utils.notifications import mark_done_if_past, notify_upcoming

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def configure_routes(dp: Dispatcher, db: Database) -> None:
    db_middleware = DbMiddleware(db)
    dp.message.middleware(db_middleware)
    dp.callback_query.middleware(db_middleware)
    dp.include_router(driver.router)
    dp.include_router(admin.router)


async def main() -> None:
    if not settings.bot_token:
        raise RuntimeError("BOT_TOKEN is required")

    bot = Bot(token=settings.bot_token, parse_mode="HTML")
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)

    db = Database(settings.database_path)
    await db.setup()

    configure_routes(dp, db)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(lambda: asyncio.create_task(notify_upcoming(bot, db)), "interval", minutes=20)
    scheduler.add_job(lambda: asyncio.create_task(mark_done_if_past(bot, db)), "interval", hours=1)
    scheduler.start()

    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Bot started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
