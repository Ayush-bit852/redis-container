make processor in a way where it pushes to redis as a diff thread and read from it to socket diff thread working async

### File: /Users/sagarthakkar/IdeaProjects/ensurity/redis-container/app/main.py

```py
import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, BackgroundTasks

from app.file_watcher import FileWatcher
from app.config import get_settings, Settings
from app.logger import configure_logging, logging
from app.parser import LogParser
from app.processor import LogProcessor
from app.redis_service import RedisService
from app.ftp_service import ftp_monitor

@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    logger = logging.getLogger(__name__)
    logger.info("Application startup…")

    # Load protocol map
    protocol_map = json.load(open("protocol_ports.json"))
    app.state.PORT_MAP = protocol_map
    logger.debug(f"Protocol map: {protocol_map}")

    # Processor + Redis
    settings: Settings = get_settings()
    processor = LogProcessor(settings)
    await processor.setup()
    app.state.processor = processor
    logger.info("LogProcessor ready")

    # FTP monitor
    # asyncio.create_task(ftp_monitor(settings))
    # logger.info("FTP monitor launched")

    # File watcher
    watcher = FileWatcher(settings)
    await watcher.setup()
    app.state.watcher = watcher
    asyncio.create_task(watcher.watch())
    logger.info(f"FileWatcher watching {settings.watch_dir}")

    yield
    logger.info("Application shutdown…")

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/upload")
async def upload(
    background: BackgroundTasks,
    file: UploadFile = File(...),
    parser: LogParser = Depends(LogParser),
    settings: Settings = Depends(get_settings),
):
    logger = logging.getLogger(__name__)
    try:
        raw = await file.read()
        text = raw.decode("utf-8")
        entries = parser.parse(text)

        redis_svc = RedisService(settings)
        await redis_svc.connect()
        for e in entries:
            logger.debug(f"Enqueueing session={e.session}")
            await redis_svc.push(e.session, e)

        background.add_task(app.state.processor.process, app.state.PORT_MAP)
        logger.info(f"Dispatched processor for {len(entries)} entries")

        return {
            "message": "Processed and queued logs successfully",
            "messages_processed": len(entries),
            "unique_sessions": len({e.session for e in entries}),
        }
    except Exception:
        logger.exception("Error handling /upload")
        raise HTTPException(status_code=500, detail="Internal error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )

```

### File: /Users/sagarthakkar/IdeaProjects/ensurity/redis-container/app/config.py

```py
from functools import lru_cache

try:
    # for projects still on the “pydantic.v1” API
    from pydantic.v1 import BaseSettings
except ImportError:
    # pydantic v2+ moves BaseSettings to its own package
    from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    server_ip: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    log_level: str = "DEBUG"
    min_sessions: int = 10

    # FTP settings
    ftp_host: str = ""
    ftp_port: int = 21
    ftp_user: str = ""
    ftp_password: str = ""
    ftp_remote_dir: str = "/logs"
    ftp_poll_interval: int = 60
    ftp_max_retries: int = 3
    ftp_backoff: float = 5.0

    # File watcher settings
    watch_dir: str = "./logs"
    watch_poll_interval: int = 5
    watcher_redis_retries: int = 5
    watcher_redis_backoff: float = 2.0

    # Redis retry settings
    redis_retries: int = 5
    redis_backoff: float = 2.0

    # Socket retry settings
    socket_retries: int = 3
    socket_backoff: float = 1.0

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings() -> Settings:
    return Settings()
```

### File: /Users/sagarthakkar/IdeaProjects/ensurity/redis-container/app/logger.py

```py
import logging.config
from app.config import get_settings

def configure_logging():
    cfg = get_settings()
    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,    # ← keep existing loggers alive
        "formatters": {
            "default": {
                "format": "%(asctime)s %(levelname)s %(name)s: %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": cfg.log_level,
            }
        },
        "root": {
            "handlers": ["console"],
            "level": cfg.log_level,
        },
        # (optional) explicitly configure uvicorn loggers so they propagate, e.g.:
        "loggers": {
            "uvicorn.error":  {"handlers": ["console"], "level": cfg.log_level, "propagate": False},
            "uvicorn.access": {"handlers": ["console"], "level": cfg.log_level, "propagate": False},
        }
    })

```

### File: /Users/sagarthakkar/IdeaProjects/ensurity/redis-container/app/parser.py

```py
import re
from datetime import datetime
from typing import List

from pydantic import BaseModel
from app.logger import configure_logging, logging

# ensure logging is configured
configure_logging()
logger = logging.getLogger(__name__)

class LogEntry(BaseModel):
    timestamp: datetime
    session: str
    port: int
    ip: str
    hex: str

class LogParser:
    _matcher = re.compile(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+INFO:\s+"
        r"\[(?P<session>[a-f0-9]+): (?P<port>\d+) < (?P<ip>[\d\.]+):\d+\] "
        r"\[TCP\] HEX: (?P<hex>[0-9A-Fa-f]+)"
    )

    def parse(self, text: str) -> List[LogEntry]:
        results: List[LogEntry] = []
        for line in text.splitlines():
            m = self._matcher.match(line.strip())
            if not m:
                continue
            data = m.groupdict()
            try:
                data["timestamp"] = datetime.strptime(data["timestamp"], "%Y-%m-%d %H:%M:%S")
                data["port"] = int(data["port"])
                results.append(LogEntry(**data))
            except Exception:
                logger.warning("Skipping invalid line: %r", line)
        logger.info(f"Parsed {len(results)} entries")
        return results

```

### File: /Users/sagarthakkar/IdeaProjects/ensurity/redis-container/app/**init**.py

```py
# from file_watcher import FileWatcher
# from config import get_settings, Settings
# from logger import configure_logging
# from parser import LogParser
# from processor import LogProcessor
# from redis_service import RedisService
# from ftp_service import ftp_monitor
from .file_watcher import FileWatcher
from .config import get_settings, Settings
from .logger import configure_logging
from .parser import LogParser
from .processor import LogProcessor
from .redis_service import RedisService
from .ftp_service import ftp_monitor

```

### File: /Users/sagarthakkar/IdeaProjects/ensurity/redis-container/app/processor.py

```py
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

```

### File: /Users/sagarthakkar/IdeaProjects/ensurity/redis-container/app/ftp\_service.py

```py
import asyncio
from ftplib import FTP
import tempfile
import os

from app.logger import configure_logging, logging
from app.parser import LogParser
from app.redis_service import RedisService
from app.config import Settings

configure_logging()
logger = logging.getLogger(__name__)

async def ftp_monitor(settings: Settings):
    seen = set()
    parser = LogParser()
    redis_svc = RedisService(settings)
    await redis_svc.connect()

    while True:
        for attempt in range(1, settings.ftp_max_retries + 1):
            try:
                with FTP() as ftp:
                    ftp.connect(settings.ftp_host, settings.ftp_port)
                    ftp.login(settings.ftp_user, settings.ftp_password)
                    ftp.cwd(settings.ftp_remote_dir)
                    files = ftp.nlst()
                    logger.debug(f"FTP listing: {files}")
                    break
            except Exception as e:
                logger.error(f"FTP listing attempt {attempt} failed: {e}")
                if attempt < settings.ftp_max_retries:
                    await asyncio.sleep(settings.ftp_backoff)
        else:
            logger.error("Failed to get FTP file list; retrying after interval")
            await asyncio.sleep(settings.ftp_poll_interval)
            continue

        for fname in files:
            if fname in seen:
                continue
            seen.add(fname)

            # download + parse
            for d_attempt in range(1, settings.ftp_max_retries + 1):
                try:
                    with tempfile.TemporaryDirectory() as tmp:
                        local_path = os.path.join(tmp, fname)
                        with open(local_path, 'wb') as f:
                            ftp.retrbinary(f"RETR {fname}", f.write)
                        logger.info(f"Downloaded FTP log: {fname}")
                        text = open(local_path).read()
                    break
                except Exception as e:
                    logger.error(f"FTP download attempt {d_attempt} for {fname} failed: {e}")
                    if d_attempt < settings.ftp_max_retries:
                        await asyncio.sleep(settings.ftp_backoff)
            else:
                logger.error(f"Skipping FTP file {fname} after {settings.ftp_max_retries} failed downloads")
                continue

            entries = parser.parse(text)
            for e in entries:
                await redis_svc.push(e.session, e)
            logger.info(f"Queued {len(entries)} entries from FTP file {fname}")

        await asyncio.sleep(settings.ftp_poll_interval)

```

### File: /Users/sagarthakkar/IdeaProjects/ensurity/redis-container/app/file\_watcher.py

```py
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

```

### File: /Users/sagarthakkar/IdeaProjects/ensurity/redis-container/app/redis\_service.py

```py
import asyncio
import logging
import aioredis
from fastapi import Depends

from app.config import Settings, get_settings
from app.logger import configure_logging

# ensure logging is configured
configure_logging()
logger = logging.getLogger(__name__)

class RedisService:
    def __init__(self, settings: Settings):
        self._settings = settings
        self.client = None

    async def connect(self):
        url = f"redis://{self._settings.server_ip}:{self._settings.redis_port}/{self._settings.redis_db}"
        for attempt in range(1, self._settings.redis_retries + 1):
            try:
                self.client = await aioredis.from_url(
                    url, encoding="utf-8", decode_responses=True
                )
                logger.info(f"Connected to Redis ({url}) on attempt {attempt}")
                return
            except Exception as e:
                logger.error(f"Redis connection attempt {attempt} failed: {e}")
                if attempt < self._settings.redis_retries:
                    await asyncio.sleep(self._settings.redis_backoff)
        raise ConnectionError(f"Could not connect to Redis after {self._settings.redis_retries} attempts")

    async def _ensure_connection(self):
        if self.client is None:
            await self.connect()

    async def push(self, session: str, message: dict):
        await self._ensure_connection()
        key = f"log_data:{session}"
        for attempt in range(1, self._settings.redis_retries + 1):
            try:
                await self.client.rpush(key, message.json())
                await self.client.expire(key, 3600)
                logger.debug(f"Pushed to Redis → key={key}, attempt={attempt}")
                return
            except Exception as e:
                logger.error(f"Redis push attempt {attempt} failed: {e}")
                await asyncio.sleep(self._settings.redis_backoff)
        raise RuntimeError(f"Failed to push to Redis key={key}")

    async def keys(self):
        await self._ensure_connection()
        return await self.client.keys("log_data:*")

    async def fetch_all(self, key: str):
        await self._ensure_connection()
        return await self.client.lrange(key, 0, -1)

    async def delete(self, key: str):
        await self._ensure_connection()
        return await self.client.delete(key)

def get_redis_service(
    settings: Settings = Depends(get_settings)
):
    return RedisService(settings)  # RedisService.__init__ takes no args

```

### File: /Users/sagarthakkar/IdeaProjects/ensurity/redis-container/app/socket\_service.py

```py
import asyncio
import logging

from app.config import get_settings
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
                logger.info(f"Socket connected → {self.host}:{port} on attempt {attempt}")
                return reader, writer
            except Exception as e:
                logger.error(f"Socket connect attempt {attempt} to {self.host}:{port} failed: {e}")
                if attempt < self._settings.socket_retries:
                    await asyncio.sleep(self._settings.socket_backoff)
        logger.error(f"Failed to connect socket to {self.host}:{port} after {self._settings.socket_retries} attempts")
        return None, None

```
