import eth_utils
import eth_account
from eth_utils.exceptions import ValidationError
from typing import List, Tuple


def is_valid_address(address: str) -> bool:
    try:
        eth_utils.to_checksum_address(address)
        return True
    except ValidationError:
        return False


def create_pair() -> Tuple[str, str]:
    t = eth_account.Account.create()
    return t.address, str(eth_utils.to_hex(t.key))


def generate_keys(count):
    return [create_pair()[0] for x in range(count)]


def keys_from_mnemonic(mnemonic, amount, offset=0) -> List[Tuple[str, str]]:
    accs = []
    eth_account.Account.enable_unaudited_hdwallet_features()
    for i in range(amount):
        acc = eth_account.Account.from_mnemonic(mnemonic, account_path=f"m/44'/60'/0'/0/{i + offset}")
        accs.append([acc.address, acc.key.hex()])
        print(acc.address)
    return accs
