import aioredis
from fastapi import Depends

from .config import Settings, get_settings
from .logger import logging, configure_logging

configure_logging()
logger = logging.getLogger(__name__)

class RedisService:
    def __init__(self, settings: Settings):
        self._settings = settings
        self.client = None

    async def connect(self):
        self.client = await aioredis.from_url(
            f"redis://{self._settings.server_ip}:{self._settings.redis_port}/{self._settings.redis_db}",
            encoding="utf-8", decode_responses=True
        )

    async def push(self, session: str, message: dict):
        await self.client.rpush(f"log_data:{session}", message.json())
        await self.client.expire(f"log_data:{session}", 3600)

    async def keys(self):
        return await self.client.keys("log_data:*")

    async def fetch_all(self, key: str):
        return await self.client.lrange(key, 0, -1)

    async def delete(self, key: str):
        await self.client.delete(key)
def get_redis_service(
    settings: Settings = Depends(get_settings)
):
    return RedisService(settings)  # RedisService.__init__ takes no args
