# -*- coding: utf-8 -*-

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from config import Config as Cfg


def get_logger(name):
    handler = RotatingFileHandler(Cfg.LOG_PATH + f"/{name}.log",
                                  mode='a',
                                  maxBytes=5 * 1024 * 1024,
                                  backupCount=2,
                                  encoding=None,
                                  delay=0)
    handler.setFormatter(logging.Formatter(Cfg.LOGGING_FORMATTER))

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger


path = Path(Cfg.LOG_PATH)
path.mkdir(parents=True, exist_ok=True)

