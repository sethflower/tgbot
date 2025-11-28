import os

from dataclasses import dataclass
from typing import List


def _split_env_list(value: str | None) -> List[int]:
    if not value:
        return []
    ids: List[int] = []
    for raw in value.replace(";", ",").split(","):
        raw = raw.strip()
        if raw:
            try:
                ids.append(int(raw))
            except ValueError:
                continue
    return ids


@dataclass
class Settings:
    bot_token: str
    admin_ids: List[int]
    database_path: str = "data/bot.db"
    log_path: str = "data/bot.log"
    timezone: str = "Europe/Kyiv"


settings = Settings(
    bot_token=os.getenv("BOT_TOKEN", ""),
    admin_ids=_split_env_list(os.getenv("ADMIN_IDS")),
)
