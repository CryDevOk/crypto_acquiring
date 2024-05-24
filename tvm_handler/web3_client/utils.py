import tronpy
from tronpy.keys import PrivateKey, to_base58check_address
import mnemonic
import base58

trc20_abi = {"abi": {"entrys": [
    {'outputs': [{'type': 'bool'}],
     'inputs': [{'name': '_to', 'type': 'address'},
                {'name': '_value', 'type': 'uint256'}], 'name': 'transfer',
     'stateMutability': 'Nonpayable', 'type': 'Function'},
    {'outputs': [{'type': 'bool'}], 'inputs': [
        {'name': '_spender', 'type': 'address'}, {'name': '_value', 'type': 'uint256'}], 'name': 'approve',
     'stateMutability': 'Nonpayable',
     'type': 'Function'},
    {'outputs': [{'type': 'uint256'}], 'constant': True, 'inputs': [{'name': 'who', 'type': 'address'}],
     'name': 'balanceOf', 'stateMutability': 'View', 'type': 'Function'},
    {'outputs': [{'type': 'bool'}],
     'inputs': [{'name': '_from', 'type': 'address'}, {'name': '_to', 'type': 'address'},
                {'name': '_value', 'type': 'uint256'}], 'name': 'transferFrom',
     'stateMutability': 'Nonpayable', 'type': 'Function'},
    {
        'inputs': [{'indexed': True, 'name': 'from', 'type': 'address'},
                   {'indexed': True, 'name': 'to', 'type': 'address'}, {'name': 'value', 'type': 'uint256'}],
        'name': 'Transfer', 'type': 'Event'},
    {'outputs': [{'name': 'remaining', 'type': 'uint256'}], 'constant': True,
     'inputs': [{'name': '_owner', 'type': 'address'}, {'name': '_spender', 'type': 'address'}],
     'name': 'allowance',
     'stateMutability': 'View', 'type': 'Function'}
]}}


def generate_mnemonic():
    return mnemonic.Mnemonic("english").generate(strength=128)


def is_valid_address(address):
    try:
        tronpy.keys.to_base58check_address(address)
        return True
    except ValueError:
        return False


def create_pair() -> tuple:
    key = PrivateKey.random()
    address = key.public_key.to_base58check_address()
    return address, key.hex()


def generate_keys(count):
    return [create_pair()[0] for x in range(count)]


def keys_from_mnemonic(mnemonic, amount, offset=0) -> list[PrivateKey]:
    client = tronpy.Tron()
    accounts = []
    for i in range(amount):
        acc = client.generate_address_from_mnemonic(mnemonic, account_path=f"m/44'/195'/0'/0/{i + offset}")
        accounts.append(PrivateKey(bytes.fromhex(acc['private_key'])))
    return accounts


def to_hex_address(raw_addr: str | bytes) -> str:
    addr = to_base58check_address(raw_addr)
    return base58.b58decode_check(addr).hex()
