import asyncio
import json
import time
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
        self.sockets: dict[str, asyncio.StreamWriter] = {}
        self.port_map = port_map

        # semaphore to bound concurrent connections/writes
        self.conn_sem = asyncio.Semaphore(self.settings.max_concurrent_sockets)
        # track last send time per session for throttling
        self._last_sent: dict[str, float] = {}

    async def setup(self):
        await self.redis.connect()
        # start producer + consumer
        asyncio.create_task(self._producer_loop(), name="redis→queue")
        asyncio.create_task(self._consumer_loop(), name="queue→socket")
        logger.info("🔄 Producer & Consumer loops started")

    async def _producer_loop(self):
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

                for raw in data:
                    msg = json.loads(raw)
                    port = int(msg["port"])
                    payload = bytes.fromhex(msg["hex"])
                    await self.queue.put((session, port, payload))
                    logger.debug(f"Enqueued {len(payload)}B for session={session}→port={port}")

                await self.redis.delete(key)

            await asyncio.sleep(0.5)

    async def _consumer_loop(self):
        while True:
            session, port, payload = await self.queue.get()

            # Throttle per-session: wait if we sent too recently
            since = time.monotonic() - self._last_sent.get(session, 0)
            wait = self.settings.send_interval - since
            if wait > 0:
                await asyncio.sleep(wait)

            # Acquire semaphore before dialing/writing
            async with self.conn_sem:
                writer = self.sockets.get(session)
                if writer is None:
                    # open new connection
                    _r, w = await self.socket_svc.connect(port)
                    if w:
                        writer = w
                        self.sockets[session] = w

                if not writer:
                    logger.error(f"No socket for session={session}, port={port}")
                else:
                    try:
                        writer.write(payload)
                        await writer.drain()
                        logger.info(f"Sent {len(payload)}B to localhost:{port} (session={session})")
                    except Exception as e:
                        logger.error(f"Write failed for {session}@{port}: {e}")
                        # drop socket so we reconnect next time
                        self.sockets.pop(session, None)

            # record last send time
            self._last_sent[session] = time.monotonic()
            self.queue.task_done()
