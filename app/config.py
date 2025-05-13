from functools import lru_cache
try:
    # for projects still on the “pydantic.v1” API
    from pydantic.v1 import BaseSettings
except ImportError:
    # pydantic v2moves BaseSettings to its own package
    from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    server_ip: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    log_level: str = "INFO"
    min_sessions: int = 10
    ftp_host: str = ""
    ftp_port: int = 21
    ftp_user: str = ""
    ftp_password: str = ""
    ftp_remote_dir: str = "/logs"
    ftp_poll_interval: int = 60  # in seconds

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings() -> Settings:
    return Settings()