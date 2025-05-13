import asyncio
import logging
from pathlib import Path

from config import Settings
from parser import LogParser
from redis_service import RedisService

logger = logging.getLogger(__name__)

class FileWatcher:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.dir = Path(settings.watch_dir)
        self.poll_interval = settings.watch_poll_interval
        self.redis_svc = RedisService(settings)
        self.parser = LogParser()
        # Keep track of processed files
        self.seen = set()

    async def setup(self):
        # Ensure watch directory exists
        if not self.dir.exists():
            self.dir.mkdir(parents=True)
        await self.redis_svc.connect()

    async def watch(self):
        print(self)
        while True:
            for file in self.dir.glob("*.log"):
                if file in self.seen:
                    continue
                self.seen.add(file)
                try:
                    text = file.read_text()
                    entries = self.parser.parse(text)
                    for e in entries:
                        await self.redis_svc.push(e.session, e)
                    logger.info(f"Queued {len(entries)} entries from {file.name}")
                except Exception as e:
                    logger.error(f"Error processing file {file}: {e}")
            await asyncio.sleep(self.poll_interval)