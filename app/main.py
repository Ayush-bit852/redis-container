import json

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, BackgroundTasks

from .config import get_settings, Settings
from .logger import configure_logging
from .parser import LogParser
from .processor import LogProcessor
from .redis_service import RedisService

app = FastAPI()
configure_logging()

# Load once at startup
PORT_MAP = {}

@app.on_event("startup")
async def startup_event():
    global PORT_MAP
    PORT_MAP = json.load(open("protocol_ports.json"))
    settings = get_settings()
    # Ensure Redis is connected
    processor = LogProcessor(settings)
    await processor.setup()
    app.state.processor = processor

@app.on_event("shutdown")
async def shutdown_event():
    # nothing extra needed; processor.process closes sockets
    pass

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
        text = (await file.read()).decode("utf-8")
        entries = parser.parse(text)

        # push to redis
        redis_svc = RedisService(settings)
        await redis_svc.connect()
        for e in entries:
            await redis_svc.push(e.session, e)

        # dispatch processing
        background.add_task(app.state.processor.process, PORT_MAP)

        return {
            "message": "Processed and queued logs successfully",
            "messages_processed": len(entries),
            "unique_sessions": len({e.session for e in entries})
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
