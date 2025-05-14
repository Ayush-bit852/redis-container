import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends

from app.file_watcher import FileWatcher
from app.config import get_settings, Settings
from app.logger import configure_logging, logging
from app.parser import LogParser
from app.processor import LogProcessor
from app.redis_service import RedisService
# ftp_monitor left in if you still need it
from app.ftp_service import ftp_monitor

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1) Logging
    configure_logging()
    logger = logging.getLogger(__name__)
    logger.info("🚀 Starting application…")

    # 2) Load & invert protocol_ports.json
    raw_map = json.loads(Path("protocol_ports.json").read_text())
    # raw_map: {"gps103":6001, ...} → port_map: {"6001": "gps103", ...}
    port_map = {str(v): int(v) for k, v in raw_map.items()}
    app.state.PORT_MAP = port_map
    logger.debug(f"Port map (inverted): {port_map}")

    # 3) Kick off LogProcessor
    settings: Settings = get_settings()
    processor = LogProcessor(settings, port_map)
    await processor.setup()
    app.state.processor = processor
    logger.info("✅ LogProcessor setup complete (producer+consumer loops)")

    # 4) Optional: start FTP monitor
    # asyncio.create_task(ftp_monitor(settings))
    # logger.info("📡 FTP monitor launched")

    # 5) Start FileWatcher
    watcher = FileWatcher(settings)
    await watcher.setup()
    app.state.watcher = watcher
    asyncio.create_task(watcher.watch())
    logger.info(f"👀 FileWatcher watching “{settings.watch_dir}”")

    yield
    logger.info("🛑 Shutting down application…")

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

        redis_svc = RedisService(settings)
        await redis_svc.connect()
        for e in entries:
            logger.debug(f"→ enqueue session={e.session}, port={e.port}")
            await redis_svc.push(e.session, e)

        logger.info(f"🗄️  Queued {len(entries)} entries into Redis")
        return {"queued": len(entries)}

    except Exception as exc:
        logger.exception("❌ Error in /upload")
        raise HTTPException(status_code=500, detail=str(exc))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="debug",
    )
