import logging.config
from app.config import get_settings

def configure_logging():
    cfg = get_settings()
    logging.config.dictConfig({
        "version": 1,
        "formatters": {
            "default": {
                "format": "%(asctime)s %(levelname)s %(name)s: %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": cfg.log_level,
            }
        },
        "root": {
            "handlers": ["console"],
            "level": cfg.log_level,
        },
    })
