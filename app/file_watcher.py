import asyncio
import logging
from pathlib import Path

from app.config import Settings
from app.parser import LogParser
from app.redis_service import RedisService

logger = logging.getLogger(__name__)

class FileWatcher:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.dir = Path(settings.watch_dir)
        self.poll_interval = settings.watch_poll_interval
        self.redis_svc = RedisService(settings)
        self.parser = LogParser()
        self.seen = set()

    async def setup(self):
        if not self.dir.exists():
            self.dir.mkdir(parents=True)
            logger.info(f"Created watch directory: {self.dir}")
        await self.redis_svc.connect()

    async def watch(self):
        logger.info(f"Starting file watcher on {self.dir}")
        while True:
            for file in self.dir.glob("*.log"):
                if file in self.seen:
                    continue
                self.seen.add(file)

                text = file.read_text()
                entries = self.parser.parse(text)

                for e in entries:
                    # each push will auto-reconnect if needed
                    await self.redis_svc.push(e.session, e)
                logger.info(f"Queued {len(entries)} entries from file {file.name}")

            await asyncio.sleep(self.poll_interval)
