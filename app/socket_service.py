import asyncio
import logging

from app.logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

class SocketService:
    def __init__(self, settings):
        self.host = settings.server_ip
        self._settings = settings

    async def connect(self, port: int):
        for attempt in range(1, self._settings.socket_retries + 1):
            try:
                reader, writer = await asyncio.open_connection(self.host, port)
                logger.info(f"Socket connected → {self.host}:{port} (attempt {attempt})")
                return reader, writer
            except Exception as e:
                logger.error(f"Socket connect attempt {attempt} to {self.host}:{port} failed: {e}")
                if attempt < self._settings.socket_retries:
                    await asyncio.sleep(self._settings.socket_backoff)
        logger.error(f"Socket ultimately failed to {self.host}:{port}")
        return None, None
