import asyncio
import json
from asyncio import Queue

from app.logger import configure_logging, logging
from app.redis_service import RedisService
from app.socket_service import SocketService

configure_logging()
logger = logging.getLogger(__name__)

class LogProcessor:
    def __init__(self, settings, port_map: dict[str, int]):
        self.settings = settings
        self.redis = RedisService(settings)
        self.socket_svc = SocketService(settings)
        self.queue: Queue[tuple[str, int, bytes]] = Queue()
        self.sockets: dict[str, tuple[int, asyncio.StreamWriter]] = {}
        self.port_map = port_map

    async def setup(self):
        # Connect to Redis and launch producer + consumer loops
        await self.redis.connect()
        asyncio.create_task(self._producer_loop(), name="redis→queue")
        asyncio.create_task(self._consumer_loop(), name="queue→socket")
        logger.info("🔄 Producer & Consumer loops started")

    async def _producer_loop(self):
        """ Continuously drain Redis lists into an in-memory queue. """
        while True:
            try:
                keys = await self.redis.keys()
            except Exception:
                logger.warning("Producer: Redis.keys() failed, reconnecting…")
                await self.redis.connect()
                keys = await self.redis.keys()

            for key in keys:
                session = key.split(":", 1)[1]
                try:
                    data = await self.redis.fetch_all(key)
                except Exception:
                    logger.warning(f"Producer: fetch_all({key}) failed, reconnecting…")
                    await self.redis.connect()
                    data = await self.redis.fetch_all(key)

                if not data:
                    continue

                # Enqueue each raw entry: (session, port, payload_bytes)
                for raw in data:
                    msg = json.loads(raw)
                    port = int(msg["port"])
                    payload = bytes.fromhex(msg["hex"])
                    await self.queue.put((session, port, payload))
                    logger.debug(f"Enqueued {len(payload)}B for session={session}→port={port}")

                # Remove the Redis list once enqueued
                await self.redis.delete(key)

            await asyncio.sleep(0.5)

    async def _consumer_loop(self):
        """ Continuously read from queue and push to localhost:port. """
        while True:
            session, port, payload = await self.queue.get()

            writer = None
            # reuse existing writer if port matches
            existing = self.sockets.get(session)
            if existing and existing[0] == port:
                writer = existing[1]
            else:
                # open a new connection
                _r, w = await self.socket_svc.connect(port)
                if w:
                    writer = w
                    self.sockets[session] = (port, w)

            if not writer:
                logger.error(f"Consumer: no socket for session={session}, port={port}")
            else:
                try:
                    writer.write(payload)
                    await writer.drain()
                    logger.info(f"Sent {len(payload)}B to localhost:{port} (session={session})")
                except Exception as e:
                    logger.error(f"Consumer: write failed for session={session}, port={port}: {e}")
                    # drop this writer so next item reconnects
                    self.sockets.pop(session, None)

            self.queue.task_done()
