import json

from .logger import logging, configure_logging
from .redis_service import RedisService
from .socket_service import SocketService

configure_logging()
logger = logging.getLogger(__name__)

class LogProcessor:
    def __init__(self, settings):
        self.settings = settings
        self.redis = RedisService(settings)
        self.sockets = {}  # session -> (reader, writer)
        self.socket_svc = SocketService(settings)

    async def setup(self):
        await self.redis.connect()

    async def process(self, PORT_MAP: dict):
        keys = await self.redis.keys()
        if len(keys) < self.settings.min_sessions:
            logger.warning("Fewer sessions than minimum, continuing anyway")

        for key in keys:
            session = key.split(":", 1)[1]
            data = await self.redis.fetch_all(key)
            if not data:
                continue

            first = json.loads(data[0])
            port = int(first["port"])
            protocol = PORT_MAP.get(str(port), "unknown")

            reader, writer = self.sockets.get(session, (None, None))
            if writer is None:
                reader, writer = await self.socket_svc.connect(port)
                if writer:
                    self.sockets[session] = (reader, writer)
                else:
                    continue

            for raw in data:
                try:
                    msg = json.loads(raw)
                    writer.write(bytes.fromhex(msg["hex"]))
                    await writer.drain()
                    logger.info(f"Sent HEX to {port} ({protocol}) for session {session}")
                except Exception as e:
                    logger.error(f"Send error for session {session}: {e}")
                    break

            await self.redis.delete(key)

        # close sockets
        for reader, writer in self.sockets.values():
            writer.close()
            await writer.wait_closed()
        logger.info("Processing complete, all sockets closed")
