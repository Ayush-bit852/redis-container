import os
import re
import json
import logging
import redis
import asyncio
import socket
import time
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
MIN_SESSIONS = 10
NUM_CORES = os.cpu_count()
HALF_CORES = max(1, NUM_CORES // 2)
MAX_WAIT_SECONDS = 60

redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# === LOGGER SETUP ===
logger = logging.getLogger("simulate_device_logger")
logger.setLevel(LOG_LEVEL)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        return obj.isoformat() if isinstance(obj, datetime) else super().default(obj)

# === PORT PROTOCOL MAP ===
def load_ports(file='protocol_ports.json'):
    try:
        with open(file, 'r') as f:
            return {str(v): k for k, v in json.load(f).items()}
    except Exception as e:
        logger.warning(f"Failed to load port mapping: {e}")
        return {}

PORT_PROTOCOL_MAP = load_ports()

# === REGEX MATCHER ===
matchers = {
    "tcp_hex": re.compile(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+INFO:\s+\[(?P<session>[a-f0-9]+): (?P<port>\d+) < (?P<ip>[\d\.]+):(?P<client_port>\d+)\] \[TCP\] HEX: (?P<hex>[0-9A-Fa-f]+)"
    )
}

def parse_line(line):
    for pattern in matchers.values():
        match = pattern.match(line.strip())
        if match:
            data = match.groupdict()
            try:
                data['timestamp'] = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S')
                data['type'] = 'tcp_hex'
                return data
            except ValueError:
                logger.warning(f"Invalid timestamp: {data.get('timestamp')}")
    return None

def parse_log_file(text: str):
    logger.info("Parsing log file content...")
    parsed = []
    sessions = set()
    for line in text.splitlines():
        parsed_line = parse_line(line)
        if parsed_line:
            parsed.append(parsed_line)
            sessions.add(parsed_line['session'])
    logger.info(f"Parsed {len(parsed)} lines, sessions found: {len(sessions)}")
    return parsed

def push_message_to_redis(msg):
    try:
        redis_client.rpush(f"log_data:{msg['session']}", json.dumps(msg, default=str))
    except redis.RedisError as e:
        logger.error(f"Redis push failed for {msg['session']}: {e}")

def connect_and_store_socket(port, protocol):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect(("127.0.0.1", port))
        logger.info(f"🔗 Connected to 127.0.0.1:{port} ({protocol})")
        return s
    except Exception as e:
        logger.error(f"❌ Connection failed to port {port} ({protocol}): {e}")
        return None

def process_redis_data_synchronously():
    session_sockets = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        keys = redis_client.keys("log_data:*")

        if len(keys) < MIN_SESSIONS:
            logger.warning("Insufficient sessions, but proceeding anyway.")

        for key in keys:
            session_id = key.decode().split(":")[1]
            try:
                messages = redis_client.lrange(key, 0, -1)
                if not messages:
                    continue

                msg = json.loads(messages[0])
                port = int(msg['port'])
                protocol = PORT_PROTOCOL_MAP.get(str(port), "unknown")

                if session_id not in session_sockets:
                    future = executor.submit(connect_and_store_socket, port, protocol)
                    sock = future.result()
                    if sock:
                        session_sockets[session_id] = sock
                    else:
                        continue

                sock = session_sockets.get(session_id)
                if sock:
                    for raw_msg in messages:
                        try:
                            msg = json.loads(raw_msg)
                            hex_data = msg['hex']
                            sock.sendall(bytes.fromhex(hex_data))
                            logger.info(f"📤 Sent HEX for session {session_id} on port {port} ({protocol})")
                        except Exception as e:
                            logger.error(f"Send failed for session {session_id}: {e}")
                            break

                    redis_client.delete(key)
                    logger.info(f"🗑️ Deleted Redis key log_data:{session_id} after sending.")

            except Exception as e:
                logger.error(f"Error processing Redis key {key}: {e}")

# === FASTAPI APP ===
app = FastAPI()

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    try:
        content = (await file.read()).decode("utf-8")
        parsed = parse_log_file(content)

        # Push messages to Redis using threadpool
        with ThreadPoolExecutor(max_workers=HALF_CORES) as executor:
            for msg in parsed:
                executor.submit(push_message_to_redis, msg)

        # Immediately process Redis data in main thread
        process_redis_data_synchronously()

        return {
            "message": "Processed and sent logs successfully.",
            "details": {
                "messages_processed": len(parsed),
                "unique_sessions": len(set(msg['session'] for msg in parsed))
            }
        }
    except Exception as e:
        logger.error(f"❌ Upload processing failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
