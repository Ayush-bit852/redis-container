import asyncio
from ftplib import FTP, error_perm
import tempfile
import os
from logger import logging, configure_logging
from parser import LogParser
from redis_service import RedisService
from config import Settings

configure_logging()
logger = logging.getLogger(__name__)

async def ftp_monitor(settings: Settings):
    """
    Periodically poll the FTP server, download new log files,
    parse them and push entries to Redis.
    """
    seen = set()
    parser = LogParser()
    redis_svc = RedisService(settings)
    await redis_svc.connect()

    while True:
        try:
            with FTP() as ftp:
                ftp.connect(settings.ftp_host, settings.ftp_port)
                ftp.login(settings.ftp_user, settings.ftp_password)
                ftp.cwd(settings.ftp_remote_dir)
                files = ftp.nlst()

                for fname in files:
                    if fname in seen:
                        continue
                    seen.add(fname)

                    # download to temp file
                    with tempfile.TemporaryDirectory() as tmp:
                        local_path = os.path.join(tmp, fname)
                        with open(local_path, 'wb') as f:
                            ftp.retrbinary(f"RETR {fname}", f.write)
                        logger.info(f"Downloaded FTP log: {fname}")

                        # read and parse
                        with open(local_path, 'r') as f:
                            text = f.read()
                        entries = parser.parse(text)

                        # push to redis
                        for e in entries:
                            await redis_svc.push(e.session, e)
                        logger.info(f"Queued {len(entries)} entries from {fname}")
        except Exception as e:
            logger.error(f"FTP monitor error: {e}")

        await asyncio.sleep(settings.ftp_poll_interval)