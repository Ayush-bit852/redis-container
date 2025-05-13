import asyncio

from .logger import logging, configure_logging

configure_logging()
logger = logging.getLogger(__name__)

class SocketService:
    def __init__(self, settings):
        self.host = settings.server_ip

    async def connect(self, port: int):
        try:
            reader, writer = await asyncio.open_connection(self.host, port)
            logger.info(f"Connected to {self.host}:{port}")
            return reader, writer
        except Exception as e:
            logger.error(f"Socket connect error {self.host}:{port}: {e}")
            return None, None
