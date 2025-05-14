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