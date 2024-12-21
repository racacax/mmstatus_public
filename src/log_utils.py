import json
import logging
import os
from logging.handlers import TimedRotatingFileHandler

import settings


class CustomJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        super(CustomJsonFormatter, self).format(record)
        output = {k: str(v) for k, v in record.__dict__.items()}
        return json.dumps(output)


def create_logger(name: str):
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    cf = CustomJsonFormatter()
    if settings.SHOW_LOGS:
        sh = logging.StreamHandler()
        sh.setFormatter(cf)
        logger.addHandler(sh)

    fh = TimedRotatingFileHandler(
        f"logs/{name}.log", when="d", interval=1, backupCount=5
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(cf)
    logger.addHandler(fh)
    return logger
