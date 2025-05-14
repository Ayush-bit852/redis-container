from functools import lru_cache

try:
    # for projects still on the “pydantic.v1” API
    from pydantic.v1 import BaseSettings
except ImportError:
    # pydantic v2+ moves BaseSettings to its own package
    from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    server_ip: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    log_level: str = "DEBUG"
    min_sessions: int = 10

    # FTP settings
    ftp_host: str = ""
    ftp_port: int = 21
    ftp_user: str = ""
    ftp_password: str = ""
    ftp_remote_dir: str = "/logs"
    ftp_poll_interval: int = 60
    ftp_max_retries: int = 3
    ftp_backoff: float = 5.0

    # File watcher settings
    watch_dir: str = "./logs"
    watch_poll_interval: int = 5
    watcher_redis_retries: int = 5
    watcher_redis_backoff: float = 2.0

    # Redis retry settings
    redis_retries: int = 5
    redis_backoff: float = 2.0

    # Socket retry settings
    socket_retries: int = 3
    socket_backoff: float = 1.0

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings() -> Settings:
    return Settings()