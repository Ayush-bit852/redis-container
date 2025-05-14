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
