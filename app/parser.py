import re
from datetime import datetime
from typing import List
from pydantic import BaseModel, Field
from .logger import configure_logging, logging

configure_logging()
logger = logging.getLogger(__name__)

class LogEntry(BaseModel):
    timestamp: datetime
    session: str
    port: int
    ip: str
    hex: str

class LogParser:
    _matcher = re.compile(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+INFO:\s+"
        r"\[(?P<session>[a-f0-9]+): (?P<port>\d+) < (?P<ip>[\d\.]+):\d+\] "
        r"\[TCP\] HEX: (?P<hex>[0-9A-Fa-f]+)"
    )

    def parse(self, text: str) -> List[LogEntry]:
        results = []
        for line in text.splitlines():
            m = self._matcher.match(line.strip())
            if not m:
                continue
            data = m.groupdict()
            try:
                data["timestamp"] = datetime.strptime(data["timestamp"], "%Y-%m-%d %H:%M:%S")
                data["port"] = int(data["port"])
                results.append(LogEntry(**data))
            except Exception:
                logger.warning("Skipping invalid line: %r", line)
        logger.info("Parsed %d entries", len(results))
        return results
