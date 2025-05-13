import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, BackgroundTasks

from app.file_watcher import FileWatcher
from app.config import get_settings, Settings
from app.logger import configure_logging
from app.parser import LogParser
from app.processor import LogProcessor
from app.redis_service import RedisService
from app.ftp_service import ftp_monitor

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Configure logging
    configure_logging()

    # Load protocol map
    protocol_map = json.load(open("protocol_ports.json"))
    app.state.PORT_MAP = protocol_map

    # Setup processor and Redis
    settings: Settings = get_settings()
    processor = LogProcessor(settings)
    await processor.setup()
    app.state.processor = processor

    # Launch FTP monitor
    asyncio.create_task(ftp_monitor(settings))

    # Launch local file watcher
    watcher = FileWatcher(settings)
    await watcher.setup()
    app.state.watcher = watcher
    asyncio.create_task(watcher.watch())

    yield  # app is up
    # (optional shutdown cleanup can go here)

# Create FastAPI app with lifespan
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
    try:
        data = (await file.read()).decode()
        entries = parser.parse(data)

        # push to Redis
        redis_svc = RedisService(settings)
        await redis_svc.connect()
        for e in entries:
            await redis_svc.push(e.session, e)

        # dispatch processing
        background.add_task(app.state.processor.process, app.state.PORT_MAP)

        return {
            "message": "Processed and queued logs successfully",
            "messages_processed": len(entries),
            "unique_sessions": len({e.session for e in entries}),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
