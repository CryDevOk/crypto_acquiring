# -*- coding: utf-8 -*-
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
import asyncio
from decimal import Decimal
from typing import Dict

from config import Config as Cfg
from api import proc_api_client
from web3_client import utils as web3_utils, providers


def get_round_for_rate(rate: Decimal, quote_asset_precision=Decimal(0.01)) -> Decimal:
    """
    Calculate round for rate, for example you have rate 1 and quote_asset_precision 0.01
    ,so it doesn't seem effect to show amount as 1.000000000...something,
    """
    return 10 ** (quote_asset_precision / rate).log10().quantize(Decimal(1))


def amount_to_quote_amount(amount: int,
                           coin_rate: Decimal,
                           coin_decimal: int) -> Decimal:
    return Decimal(amount / (10 ** coin_decimal)) * coin_rate


def quote_amount_to_amount(quote_amount: Decimal,
                           coin_rate: Decimal,
                           coin_decimal: int) -> int:
    return int((10 ** coin_decimal * quote_amount) / (coin_rate * Cfg.quote_decimal_factor))


def amount_to_display(amount: int, coin_decimal: int, rounding: Decimal) -> str:
    return str(Decimal(amount / 10 ** coin_decimal).quantize(rounding))


def get_logger(name):
    handler = RotatingFileHandler(Cfg.LOG_PATH + f"/{name}.log",
                                  mode='a',
                                  maxBytes=5 * 1024 * 1024,
                                  backupCount=1,
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
        print(args)
        print(kwargs)
        self.logger.critical(str(args))

    def flush(self):
        pass


std_logger = get_logger('std_logger')
sys.stderr = StdErrToLogger(std_logger)

path = Path(Cfg.LOG_PATH)
path.mkdir(parents=True, exist_ok=True)


class AsyncPool(asyncio.Queue):
    def put_all(self, list_items):
        for item in list_items:
            self.put_nowait(item)


class SharedVariables:
    def __init__(self):
        self.last_handled_block: int = None
        self.trusted_block: int = None
        self.deposits_queue = asyncio.Queue()

        self.user_accounts: Dict[str, str] = {}  # {address: address_id}
        self.user_accounts_hex: Dict[str, str] = {}  # {address: address_id}
        self.handler_accounts_hex: Dict[str, str] = {}  # {address: address_id}

        self.user_accounts_event = asyncio.Event()

        self.api_keys_pool = AsyncPool()
        self.providers_request_explorer = web3_utils.TronRequestExplorer()

        trongrid_providers = [(providers.AsyncTronGridHTTPProvider,
                               {"endpoint_uri": Cfg.trongrid_server,
                                "api_key": api_key,
                                "request_explorer": self.providers_request_explorer}) for api_key in
                              Cfg.trongrid_api_keys]

        zan_providers = [(providers.AsyncZanHTTPProvider,
                          {"endpoint_uri": Cfg.zan_server,
                           "api_key": api_key,
                           "request_explorer": self.providers_request_explorer}) for api_key in Cfg.zan_api_key_keys]

        self.api_keys_pool.put_all([*trongrid_providers, *zan_providers] * 10)

        self.energy_price = 420
        self.coins_abi: Dict[str, Dict] = {}
        self.estimated_trc20_fee = 30_000_000  # TODO parse it using blocks
        self.estimated_native_fee = 3_000_000

        self.block_parser_interval = 2


proc_api_client = proc_api_client.Client(Cfg.PROC_URL, Cfg.PROC_API_KEY)
