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
    log_level: str = "INFO"
    min_sessions: int = 10

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings() -> Settings:
    return Settings()
