from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework import status
import socket
import time
import re
import argparse
import logging
import json
import redis
import os
import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
from dotenv import load_dotenv

# === CONFIGURATION ===
SERVER_IP = os.getenv('SERVER_IP')
# REDIS_HOST = os.getenv('REDIS_HOST', '10.1.17.225')
REDIS_PORT = int(os.getenv('REDIS_PORT'))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
redis_client = redis.StrictRedis(host=SERVER_IP, port=REDIS_PORT, db=REDIS_DB)

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
MIN_SESSIONS = 10  # Minimum number of sessions to start processing
NUM_CORES = cpu_count()
HALF_CORES = max(1, NUM_CORES // 2)  # 50% of cores for each task
MAX_WAIT_SECONDS = 60  # Timeout for session check

# === SET UP LOGGER ===
logger = logging.getLogger("simulate_device_logger")
logger.setLevel(LOG_LEVEL)
logger.propagate = False

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(LOG_LEVEL)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

# === DATE ENCODER ===
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# === REDIS CONNECTION ===
redis_client = redis.StrictRedis(host=SERVER_IP, port=REDIS_PORT, db=REDIS_DB)

def connect_and_store_socket(port, protocol):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect((f"{SERVER_IP}", port))
        logger.info(f"🔗 Connected to {SERVER_IP}:{port} ({protocol})")
        return port, s
    except Exception as e:
        logger.error(f"❌ Connection failed to {SERVER_IP}:{port} - {e}")
        return port, None

# === LOAD PORT PROTOCOL MAPPING ===
def load_ports(file='protocol_ports.json'):
    try:
        with open(file, 'r') as f:
            port_data = json.load(f)
            return {str(v): k for k, v in port_data.items()}
    except Exception as e:
        logger.warning(f"Could not load protocol_ports.json: {e}")
        return {}

PORT_PROTOCOL_MAP = load_ports()

# === MATCHERS ===
matchers = {
    "tcp_hex": re.compile(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+INFO:\s+\[(?P<session>[a-f0-9]+): (?P<port>\d+) < (?P<ip>[\d\.]+):(?P<client_port>\d+)\] \[TCP\] HEX: (?P<hex>[0-9A-Fa-f]+)"
    ),
}

def parse_line(line):
    for type_, pattern in matchers.items():
        match = pattern.match(line.strip())
        if match:
            data = match.groupdict()
            try:
                data['timestamp'] = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S')
                data['type'] = type_
                return data
            except ValueError:
                logger.warning(f"Invalid timestamp format: {data.get('timestamp')}")
    return None

# === PARSE LOG FILE ===
def parse_log_file(file_path):
    logger.info(f"Reading log file: {file_path}")
    parsed = []
    unique_sessions = set()
    with open(file_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            parsed_data = parse_line(line)
            if parsed_data:
                parsed.append(parsed_data)
                unique_sessions.add(parsed_data['session'])
            else:
                logger.debug(f"Skipping unmatched line {i}: {line.strip()}")
    logger.info(f"Parsed {len(parsed)} messages with {len(unique_sessions)} unique sessions: {unique_sessions}")
    return parsed

# === PUSH DATA TO REDIS ===
def push_message_to_redis(msg):
    session_id = msg['session']
    redis_key = f"log_data:{session_id}"
    try:
        redis_client.rpush(redis_key, json.dumps(msg, cls=DateTimeEncoder))
    except redis.RedisError as e:
        logger.error(f"Failed to push to Redis ({redis_key}): {e}")

async def push_to_redis(messages):
    loop = asyncio.get_running_loop()
    # Use ThreadPoolExecutor instead of ProcessPoolExecutor to avoid Django model issues
    with ThreadPoolExecutor(max_workers=HALF_CORES) as executor:
        tasks = [loop.run_in_executor(executor, push_message_to_redis, msg) for msg in messages]
        await asyncio.gather(*tasks)
    logger.info("All log data pushed to Redis.")

# === BACKGROUND TASKS ===
async def delayed_delete(redis_key, delay=10):
    await asyncio.sleep(delay)
    try:
        exists = redis_client.exists(redis_key)
        if not exists:
            logger.warning(f"⚠️ Redis key {redis_key} not found before deletion attempt.")
        deleted = redis_client.delete(redis_key)
        if deleted:
            logger.info(f"🗑️ [DELAYED] Deleted Redis key {redis_key} after {delay}s.")
        else:
            logger.warning(f"⚠️ Redis key {redis_key} deletion returned 0 (not found).")
    except redis.RedisError as e:
        logger.error(f"❌ Redis error during deletion of {redis_key}: {e}")

async def process_redis_data_per_session():
    session_sockets = {}  # session_id -> socket
    pending_sessions = set()
    deleted_positions_count = 0
    deletion_tasks = []  # Collect deletion tasks for await

    with ThreadPoolExecutor(max_workers=5) as socket_executor:
        start_time = time.time()
        while True:
            keys = redis_client.keys('log_data:*')
            session_count = len(keys)

            if session_count < MIN_SESSIONS and time.time() - start_time < MAX_WAIT_SECONDS:
                logger.debug(f"Waiting for {MIN_SESSIONS} sessions, currently {session_count}")
                await asyncio.sleep(1)
                continue
            if session_count < MIN_SESSIONS:
                logger.warning(f"Timeout after {MAX_WAIT_SECONDS}s, proceeding with {session_count} sessions")
            else:
                logger.info(f"Found {session_count} sessions, processing...")

            for key in keys:
                session_id = key.decode().split(':')[1]

                while redis_client.llen(key) > 0:
                    msg_json = redis_client.lindex(key, 0)
                    if not msg_json:
                        break

                    msg = json.loads(msg_json)
                    port = int(msg['port'])
                    hex_data = msg['hex']
                    protocol = PORT_PROTOCOL_MAP.get(str(port), "unknown")

                    if session_id not in session_sockets and session_id not in pending_sessions:
                        future = asyncio.get_running_loop().run_in_executor(
                            socket_executor, connect_and_store_socket, port, protocol
                        )
                        try:
                            port_, socket_obj = await future
                            if socket_obj:
                                session_sockets[session_id] = socket_obj
                            pending_sessions.discard(session_id)
                        except Exception as e:
                            logger.error(f"Socket setup failed for session {session_id}: {e}")
                            pending_sessions.discard(session_id)
                            break

                    if session_id in session_sockets:
                        try:
                            session_sockets[session_id].sendall(bytes.fromhex(hex_data))
                            redis_client.lpop(key)
                            logger.info(f"📤 Sent and removed from Redis {SERVER_IP}:{port} ({protocol}) for session {session_id}: {hex_data}")

                            # Decode and extract positions.<id>
                            try:
                                decoded_text = bytes.fromhex(hex_data).decode('utf-8', errors='ignore')
                                logger.debug(f"🧾 Decoded hex text: {decoded_text}")
                                match = re.search(r'positions\.(\d+)', decoded_text)
                                if match:
                                    key_to_delete = f"positions.{match.group(1)}"
                                    logger.info(f"🧬 Matched Redis key for delayed deletion: {key_to_delete}")
                                    task = asyncio.create_task(delayed_delete(key_to_delete))
                                    deletion_tasks.append(task)
                                    deleted_positions_count += 1
                                else:
                                    logger.debug("No positions.<id> key found in decoded text.")
                            except Exception as e:
                                logger.warning(f"Failed to decode hex or extract key: {e}")

                        except Exception as e:
                            logger.error(f"❌ Failed to send for session {session_id}: {e}")
                            session_sockets[session_id].close()
                            del session_sockets[session_id]
                            break

                if redis_client.llen(key) == 0:
                    try:
                        redis_client.delete(key)
                        logger.info(f"🧹 Deleted Redis session key {key.decode()} after processing.")
                    except redis.RedisError as e:
                        logger.error(f"Failed to delete session Redis key {key.decode()}: {e}")

            await asyncio.sleep(0.1)

        # Cleanup
        for s in session_sockets.values():
            s.close()
        logger.info("🛑 All session sockets closed.")

        # Wait for all deletions to finish
        if deletion_tasks:
            logger.info("⏳ Waiting for all delayed deletions to finish...")
            await asyncio.gather(*deletion_tasks)

        logger.info(f"✅ Total positions.* keys scheduled for deletion: {deleted_positions_count}")

# === FILE UPLOAD VIEW ===
class FileUploadView(APIView):
    parser_classes = (MultiPartParser, FormParser)

    def post(self, request, *args, **kwargs):
        uploaded_file = request.FILES.get('file')

        if not uploaded_file:
            return Response({"error": "No file uploaded."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            file_content = uploaded_file.read().decode('utf-8')

            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, mode='w+', encoding='utf-8') as tmp:
                tmp.write(file_content)
                tmp_path = tmp.name

            parsed_data = parse_log_file(tmp_path)

            # Start async processing in a separate thread to avoid blocking
            import threading
            def process_async():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(push_to_redis(parsed_data))
                    loop.run_until_complete(process_redis_data_per_session())
                finally:
                    loop.close()

            thread = threading.Thread(target=process_async)
            thread.start()

            return Response({
                "message": "File processing started successfully.",
                "details": {
                    "messages_processed": len(parsed_data),
                    "unique_sessions": len(set(msg['session'] for msg in parsed_data))
                }
            }, status=status.HTTP_202_ACCEPTED)

        except Exception as e:
            logger.error(f"Error in processing: {e}")
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)