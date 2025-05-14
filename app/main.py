import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends

from app.file_watcher import FileWatcher
from app.config import get_settings, Settings
from app.logger import configure_logging, logging
from app.parser import LogParser
from app.processor import LogProcessor
from app.redis_service import RedisService
from app.ftp_service import ftp_monitor

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Configure logging early
    configure_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting application…")

    # Load protocol‐to‐port map
    protocol_map = json.load(open("protocol_ports.json"))
    app.state.PORT_MAP = protocol_map
    logger.debug(f"Protocol map loaded: {protocol_map}")

    # Initialize LogProcessor (spawns producer+consumer loops)
    settings: Settings = get_settings()
    processor = LogProcessor(settings)
    await processor.setup()
    app.state.processor = processor
    logger.info("LogProcessor setup complete")

    # Start FTP monitor
    # asyncio.create_task(ftp_monitor(settings))
    # logger.info("FTP monitor task launched")

    # Start file watcher
    watcher = FileWatcher(settings)
    await watcher.setup()
    app.state.watcher = watcher
    asyncio.create_task(watcher.watch())
    logger.info(f"FileWatcher watching directory: {settings.watch_dir}")

    yield
    logger.info("Shutting down application…")

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/upload")
async def upload(
    file: UploadFile = File(...),
    parser: LogParser = Depends(LogParser),
    settings: Settings = Depends(get_settings),
):
    logger = logging.getLogger(__name__)
    try:
        text = (await file.read()).decode()
        entries = parser.parse(text)

        # Push into Redis only; processor loops handle rest
        redis_svc = RedisService(settings)
        await redis_svc.connect()
        for e in entries:
            logger.debug(f"Enqueueing session={e.session}")
            await redis_svc.push(e.session, e)

        logger.info(f"Received and queued {len(entries)} entries")
        return {"queued": len(entries)}
    except Exception as exc:
        logger.exception("Error in /upload")
        raise HTTPException(status_code=500, detail=str(exc))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="debug",    # ensure DEBUG logs appear
    )