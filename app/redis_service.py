from __future__ import annotations

import asyncio
import redis.asyncio as redis  # ✅ updated import
from fastapi import Depends
from app.config import Settings, get_settings
from app.logger import configure_logging, logging

configure_logging()
logger = logging.getLogger(__name__)

class RedisService:
    def __init__(self, settings: Settings):
        self._settings = settings
        self.client: redis.Redis | None = None

    async def connect(self):
        url = f"redis://{self._settings.server_ip}:{self._settings.redis_port}/{self._settings.redis_db}"
        for attempt in range(1, self._settings.redis_retries + 1):
            try:
                self.client = redis.Redis.from_url(
                    url, encoding="utf-8", decode_responses=True
                )
                await self.client.ping()  # ✅ confirm connection works
                logger.info(f"Connected to Redis ({url}) on attempt {attempt}")
                return
            except Exception as e:
                logger.error(f"Redis connect attempt {attempt} failed: {e}")
                if attempt < self._settings.redis_retries:
                    await asyncio.sleep(self._settings.redis_backoff)
        raise ConnectionError(f"Could not connect to Redis after {self._settings.redis_retries} attempts")

    async def _ensure(self):
        if self.client is None:
            await self.connect()

    async def push(self, session: str, message: object):
        await self._ensure()
        key = f"log_data:{session}"
        for attempt in range(1, self._settings.redis_retries + 1):
            try:
                await self.client.rpush(key, message.json())
                await self.client.expire(key, 3600)
                logger.debug(f"Redis push to {key}, attempt {attempt}")
                return
            except Exception as e:
                logger.error(f"Redis push attempt {attempt} failed: {e}")
                await asyncio.sleep(self._settings.redis_backoff)
        raise RuntimeError(f"Redis push ultimately failed for key {key}")

    async def keys(self):
        await self._ensure()
        return await self.client.keys("log_data:*")

    async def fetch_all(self, key: str):
        await self._ensure()
        return await self.client.lrange(key, 0, -1)

    async def delete(self, key: str):
        await self._ensure()
        return await self.client.delete(key)

def get_redis_service(
    settings: Settings = Depends(get_settings),
):
    return RedisService(settings)
