# -*- coding: utf-8 -*-

import logging
import sys
from logging.handlers import RotatingFileHandler
from logging.handlers import QueueHandler, QueueListener
from queue import Queue
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


class StdErrToLogger:
    def __init__(self, logger_: logging.Logger):
        self.logger = logger_

    def write(self, *args, **kwargs):
        self.logger.critical(str(args))

    def flush(self):
        pass


std_logger = get_logger('std_logger')
sys.stderr = StdErrToLogger(std_logger)

path = Path(Cfg.LOG_PATH)
path.mkdir(parents=True, exist_ok=True)
