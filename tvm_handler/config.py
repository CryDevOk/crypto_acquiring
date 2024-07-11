import os
from enum import Enum
import validators


class StatCode(Enum):
    def __init__(self, *args):
        self.v = self.value
        self.n = self.name

    private = "private"

    USER = 10
    APPROVE = 11
    SADMIN = 12

    API_READ = "API_READ"
    API_WRITE = "API_WRITE"

    user_id = "user_id"
    deposit_info = "deposit_info"

    web3 = "web3"
    fiat = "fiat"

    RU = "RU"
    ENG = "ENG"

    logged = "logged"
    session_token = "session_token"
    verify_message = "verify_message"
    keys = "keys"

    gameMobileUrl = "gameMobileUrl"
    gameDesktopUrl = "gameDesktopUrl"

    balance = "balance"
    currency = "currency"
    externalTransactionId = "externalTransactionId"
    amount = "amount"

    native = "native"


class Config(object):
    St = StatCode

    PATH = os.environ.get("APP_PATH", "/app").rstrip("/")
    LOG_PATH = PATH + "/logs"

    PROC_HANDLER_NAME = os.environ.get("PROC_HANDLER_NAME")
    PROC_HANDLER_DISPLAY = os.environ.get("PROC_HANDLER_DISPLAY")
    ADMIN_SEED = os.environ.get("PROC_HANDLER_ADMIN_SEED")

    WRITE_DSN = os.environ.get("PROC_HANDLER_WRITE_DSN")
    READ_DSN = os.environ.get("PROC_HANDLER_READ_DSN")
    DB_SECRET_KEY = os.environ.get("PROC_HANDLER_DB_SECRET_KEY").encode()

    trongrid_server = os.environ.get("PROC_HANDLER_PROVIDER_TRONGRID_URL")
    trongrid_api_keys = os.environ.get("PROC_HANDLER_TRONGRID_API_KEYS")
    if trongrid_api_keys:
        trongrid_api_keys = trongrid_api_keys.split(",")
        assert trongrid_server, "PROC_HANDLER_PROVIDER_TRONGRID_URL must be set if PROC_HANDLER_TRONGRID_API_KEYS has been set"
    else:
        trongrid_api_keys = []

    zan_server = os.environ.get("PROC_HANDLER_PROVIDER_ZAN_URL")
    zan_api_key_keys = os.environ.get("PROC_HANDLER_ZAN_API_KEYS")
    if zan_api_key_keys:
        zan_api_key_keys = zan_api_key_keys.split(",")
        assert zan_server, "PROC_HANDLER_PROVIDER_ZAN_URL must be set if PROC_HANDLER_ZAN_API_KEYS has been set"
    else:
        zan_api_key_keys = []

    scanner_url = os.environ.get("PROC_HANDLER_SCANNER_URL")
    config_coins = os.environ.get("PROC_HANDLER_COINS")

    network_name = os.environ.get("PROC_HANDLER_NETWORK_NAME")
    start_block = os.environ.get("PROC_HANDLER_START_BLOCK", "latest")

    PROC_HANDLER_API_KEY = os.environ.get("PROC_HANDLER_API_KEY")
    PROC_URL = os.environ.get("PROC_URL")
    PROC_API_KEY = os.environ.get("PROC_API_KEY")

    assert network_name is not None, "PROC_HANDLER_NETWORK_NAME must be set"
    assert validators.url(trongrid_server), "PROC_HANDLER_PROVIDER_URL must be a valid URL"
    assert validators.url(scanner_url), "PROC_HANDLER_SCANNER_URL must be a valid URL"

    if start_block != "latest":
        start_block = int(start_block)

    LOGGING_FORMATTER, TIME_FORMAT = '%(module)s#[LINE:%(lineno)d]# %(levelname)-3s [%(asctime)s] %(message)s', '%Y-%m-%d %H:%M:%S'

    admin_accounts = 1
    approve_accounts = 4

    allowed_slippage = 2
    block_offset = 18
    min_admin_address_native_balance = 50 * (10 ** 6)

    WRITE_POOL_SIZE = 10
    READ_POOL_SIZE = 10

    quote_coin = "USDT"
    quote_decimal_factor = 1

    native_warning_threshold = 10  # count of transaction that can wallet handle before it's balance will be low
    native_error_threshold = 2  # count of transaction that can wallet handle before it's balance will be critical
