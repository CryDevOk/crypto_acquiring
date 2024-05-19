import os
from enum import Enum


class StatCode(Enum):
    def __init__(self, *args):
        self.v = self.value
        self.n = self.name

    private = "private"

    USER = 10
    APPROVE = 11
    SADMIN = 12

    USDT_TRC20 = "USDT_TRC20"
    TRX = "TRX"


class Config(object):
    St = StatCode

    PATH = os.environ.get("APP_PATH", "/app").rstrip("/")
    WRITE_DSN = os.environ.get("PROC_API_WRITE_DSN")
    READ_DSN = os.environ.get("PROC_API_READ_DSN")
    DB_SECRET_KEY = os.environ.get("PROC_DB_SECRET_KEY").encode()
    PROC_API_KEY = os.environ.get("PROC_API_KEY")
    PROC_HANDLER_URLS = os.environ.get("PROC_HANDLER_URLS")

    WRITE_POOL_SIZE = 5
    READ_POOL_SIZE = 5

    LOG_PATH = PATH + "/logs"
    LOGGING_FORMATTER, TIME_FORMAT = '%(module)s#[LINE:%(lineno)d]# %(levelname)-3s [%(asctime)s] %(message)s', '%Y-%m-%d %H:%M:%S'
