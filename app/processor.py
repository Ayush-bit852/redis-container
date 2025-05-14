import json
import asyncio

from app.logger import configure_logging, logging
from app.redis_service import RedisService
from app.socket_service import SocketService

configure_logging()
logger = logging.getLogger(__name__)

class LogProcessor:
    def __init__(self, settings):
        self.settings = settings
        self.redis = RedisService(settings)
        self.socket_svc = SocketService(settings)
        self.sockets = {}  # session -> (reader, writer)

    async def setup(self):
        await self.redis.connect()

    async def process(self, PORT_MAP: dict):
        try:
            keys = await self.redis.keys()
        except Exception:
            logger.warning("Redis keys() failed, reconnecting…")
            await self.redis.connect()
            keys = await self.redis.keys()

        logger.info(f"Processing {len(keys)} session queue(s)")
        if len(keys) < self.settings.min_sessions:
            logger.warning("Fewer sessions than threshold, proceeding anyway")

        for key in keys:
            session = key.split(":", 1)[1]
            try:
                data = await self.redis.fetch_all(key)
            except Exception:
                logger.warning(f"Redis fetch_all({key}) failed, reconnecting…")
                await self.redis.connect()
                data = await self.redis.fetch_all(key)

            if not data:
                continue

            first = json.loads(data[0])
            port = int(first["port"])
            proto = PORT_MAP.get(str(port), "unknown")

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
                    logger.info(f"Sent HEX to {port} ({proto}) for session {session}")
                except Exception as e:
                    logger.error(f"Error sending to socket for session {session}: {e}")
                    # try reconnecting that socket once
                    r2, w2 = await self.socket_svc.connect(port)
                    if w2:
                        writer = w2
                        self.sockets[session] = (r2, w2)
                        writer.write(bytes.fromhex(msg["hex"]))
                        await writer.drain()
                        logger.info(f"Re-sent HEX after reconnect to {port} for session {session}")
                    break

            await self.redis.delete(key)

        # close all sockets
        for rdr, wtr in self.sockets.values():
            try:
                wtr.close()
                await wtr.wait_closed()
            except Exception:
                pass
        logger.info("All sockets closed, processing complete")
