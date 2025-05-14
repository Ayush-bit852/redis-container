import asyncio
import json
from asyncio import Queue

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
        self.queue: Queue[tuple[str, bytes]] = Queue()
        self.sockets: dict[str, asyncio.StreamWriter] = {}

    async def setup(self):
        # connect to Redis & then spawn two loops
        await self.redis.connect()
        asyncio.create_task(self._producer_loop(), name="redis->queue")
        asyncio.create_task(self._consumer_loop(), name="queue->socket")
        logger.info("Processor producer & consumer loops started")

    async def _producer_loop(self):
        """ Continuously fetch all Redis lists and enqueue raw HEX payloads. """
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

                # enqueue each message, then delete the Redis list
                for raw in data:
                    msg = json.loads(raw)
                    hexbytes = bytes.fromhex(msg["hex"])
                    await self.queue.put((session, hexbytes))
                    logger.debug(f"Producer: enqueued {len(hexbytes)} bytes for session={session}")

                await self.redis.delete(key)

            await asyncio.sleep(1)  # small delay to avoid busy‐loop

    async def _consumer_loop(self):
        """ Continuously dequeue payloads and write to the correct socket. """
        while True:
            session, payload = await self.queue.get()
            port = None
            # infer port & protocol mapping only if needed; else embed session→port in queue
            # for simplicity assume protocol map holds a single port per session:
            # you might extend producer to enqueue (session, port, payload)
            # here we just demo reconnect logic on socket write:
            try:
                # open socket if needed
                reader_writer = self.sockets.get(session)
                if reader_writer is None:
                    # default port based on session's first message?
                    # for demo reuse port 1234 (or store mapping in app.state.PORT_MAP)
                    raise RuntimeError("No socket yet")
                writer = reader_writer
                writer.write(payload)
                await writer.drain()
                logger.info(f"Consumer: sent {len(payload)} bytes for session={session}")
            except Exception:
                # try to open a fresh socket
                port = self.settings.some_default_port  # replace with actual logic
                reader, writer = await self.socket_svc.connect(port)
                if writer:
                    self.sockets[session] = writer
                    try:
                        writer.write(payload)
                        await writer.drain()
                        logger.info(f"Consumer: reconnected & sent {len(payload)} bytes for session={session}")
                    except Exception as e:
                        logger.error(f"Consumer: failed even after reconnect: {e}")
                else:
                    logger.error(f"Consumer: could not open socket to port={port}")
            finally:
                self.queue.task_done()
