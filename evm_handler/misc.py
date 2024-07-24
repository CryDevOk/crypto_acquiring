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

    def write(self, msg):
        msg = msg.replace("\n", "").replace("^", "").strip()
        if msg != "":
            print(msg)
            self.logger.critical(msg)

    def flush(self):
        pass


std_logger = get_logger('std_logger')
sys.stderr = StdErrToLogger(std_logger)

path = Path(Cfg.LOG_PATH)
path.mkdir(parents=True, exist_ok=True)


class SharedVariables:
    def __init__(self):
        self.last_handled_block: int = None
        self.trusted_block: int = None
        self.deposits_queue = asyncio.Queue()

        self.user_accounts: Dict[str, str] = {}  # {address: address_id}
        self.user_accounts_low_case: Dict[str, str] = {}  # {address.lower(): address_id}
        self.handler_accounts: Dict[str, str] = {}  # {address: address_id}
        self.handler_accounts_low_case: Dict[str, str] = {}

        self.user_accounts_event = asyncio.Event()
        providers_ = []
        if Cfg.getblock_api_keys:
            providers_.append(providers.ProviderCaller(providers.AsyncGetblockHTTPProvider,
                                             Cfg.getblock_server,
                                             Cfg.getblock_api_keys[0]))
        if Cfg.infura_api_key_keys:
            providers_.append(providers.ProviderCaller(providers.AsyncInfuraHTTPProvider,
                                             Cfg.infura_server,
                                             Cfg.infura_api_key_keys[0]))

        self.providers_pool = providers.ProvidersPool(providers_)

        self.coins_abi: Dict[str, Dict] = {}

        self.gas_price_event = asyncio.Event()
        self.gas_price = 0

        self.block_parser_interval = 2


proc_api_client = proc_api_client.Client(Cfg.PROC_URL, Cfg.PROC_API_KEY)
