from aiogram import BaseMiddleware
from app.database import Database


class DbMiddleware(BaseMiddleware):
    def __init__(self, db: Database):
        super().__init__()
        self.db = db

    async def __call__(self, handler, event, data):
        data["db"] = self.db
        return await handler(event, data)
