from logging.config import dictConfig
from census_local import *

# Logging Configuration
dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "[%(asctime)s] — " + census_id + " — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s",
        }
    },
    "handlers": {
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "filename": log_file,
            "maxBytes": 100000,
            "backupCount": 10,
            "delay": "False",
       },
        "syslog": {
            "class": "logging.handlers.SysLogHandler",
            "formatter": "default",
            "address": (syslog_server, 514),
            "facility": "user"
        }
    },
    "loggers": {
        "gunicorn.error": {
            "handlers": ["file", "syslog"] ,
            "level": "INFO",
            "propagate": False,
        },
        "gunicorn.access": {
            "handlers": ["file", "syslog"] ,
            "level": "INFO",
            "propagate": False,
        }
    },
    "root": {
        "level": "WARN",
        "handlers": ["file", "syslog"]
    }
})
