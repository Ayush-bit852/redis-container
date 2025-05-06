from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework import status
from .models import FileUpload
from .serializers import FileUploadSerializer
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

# Logging setup
logger = logging.getLogger("simulate_device_logger")
logger.setLevel(os.getenv('LOG_LEVEL', 'DEBUG').upper())
logger.propagate = False

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(os.getenv('LOG_LEVEL', 'DEBUG').upper())
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# Constants
MIN_SESSIONS = 10  # Minimum number of sessions to start processing
NUM_CORES = cpu_count()
HALF_CORES = max(1, NUM_CORES // 2)  # 50% of cores for each task
MAX_WAIT_SECONDS = 60

# DateTimeEncoder for JSON serialization
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Socket connection function
def connect_and_store_socket(port, protocol):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect(("http://10.1.17.225", port))
        logger.info(f"🔗 Connected to http://10.1.17.225:{port} ({protocol})")
        return port, s
    except Exception as e:
        logger.error(f"❌ Connection failed to http://10.1.17.225:{port} - {e}")
        return port, None

# Load port-protocol mapping
def load_ports(file='protocol_ports.json'):
    try:
        with open(file, 'r') as f:
            port_data = json.load(f)
            return {str(v): k for k, v in port_data.items()}
    except Exception as e:
        logger.warning(f"Could not load protocol_ports.json: {e}")
        return {}

PORT_PROTOCOL_MAP = load_ports()

# Regex matchers for log line parsing
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

# Parse the log file
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

# Push data to Redis
def push_message_to_redis(msg):
    session_id = msg['session']
    redis_key = f"log_data:{session_id}"
    try:
        redis_client.rpush(redis_key, json.dumps(msg, cls=DateTimeEncoder))
    except redis.RedisError as e:
        logger.error(f"Failed to push to Redis ({redis_key}): {e}")

async def push_to_redis(messages):
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor(max_workers=HALF_CORES) as executor:
        tasks = [loop.run_in_executor(executor, push_message_to_redis, msg) for msg in messages]
        await asyncio.gather(*tasks)
    logger.info("All log data pushed to Redis.")

async def process_redis_data_per_session():
    session_sockets = {}
    pending_sessions = set()

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
                    msg_json = redis_client.lpop(key)
                    if not msg_json:
                        continue

                    msg = json.loads(msg_json)
                    port = int(msg['port'])
                    protocol = PORT_PROTOCOL_MAP.get(str(port), "unknown")
                    hex_data = msg['hex']

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

                    if session_id in session_sockets:
                        try:
                            session_sockets[session_id].sendall(bytes.fromhex(hex_data))
                            logger.info(f"📤 Sent to 127.0.0.1:{port} ({protocol}) for session {session_id}: {hex_data}")
                        except Exception as e:
                            logger.error(f"❌ Failed to send for session {session_id}: {e}")
                            session_sockets[session_id].close()
                            del session_sockets[session_id]

            await asyncio.sleep(0.1)

# File upload view handling
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

            # Start async processing (you can use asyncio.run or run in background task)
            asyncio.run(push_to_redis(parsed_data))
            asyncio.run(process_redis_data_per_session())

            return Response({"message": "File processed successfully."}, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error in processing: {e}")
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    










