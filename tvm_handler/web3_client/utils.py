import tronpy
from tronpy.keys import PrivateKey, to_base58check_address
import mnemonic
import base58
from datetime import datetime, timedelta
import pandas as pd
import asyncio

import hashlib
import base64
import binascii
import copy

from google.protobuf.json_format import ParseDict
from trontxsize.tron_pb2 import Transaction

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


class TronRequestExplorer:
    def __init__(self, provider_name, clean_time_frame: timedelta = timedelta(hours=24)):
        self.df = pd.DataFrame(columns=['timestamp', 'http_code'])
        self.clean_time_frame = clean_time_frame
        self.lock = asyncio.Lock()
        self.provider_name = provider_name

    async def add_request(self, request_http_code):
        async with self.lock:
            new_row = pd.DataFrame(
                {'timestamp': [datetime.now()], 'http_code': [request_http_code], 'provider': [self.provider_name]})
            if self.df.empty:
                self.df = new_row
            else:
                self.df = pd.concat(
                    [self.df, new_row],
                    ignore_index=True)

    async def clean_old_requests(self):
        async with self.lock:
            current_time = datetime.now()
            cutoff_time = current_time - self.clean_time_frame
            self.df = self.df[self.df['timestamp'] >= cutoff_time]

    async def count_requests(self, time_frame: timedelta):
        async with self.lock:
            cutoff_time = datetime.now() - time_frame
            request_count = self.df[self.df['timestamp'] >= cutoff_time].shape[0]
        return request_count

    async def share_unsuccessful_requests(self, time_frame: timedelta):
        async with self.lock:
            cutoff_time = datetime.now() - time_frame
            all_requests = self.df[self.df['timestamp'] >= cutoff_time].shape[0]
            unsuccessful_requests = \
                self.df[(self.df['timestamp'] >= cutoff_time) & (self.df['http_code'] != 200)].shape[0]
        return unsuccessful_requests / all_requests if all_requests > 0 else 0

    async def description_by_status_code(self, time_frame: timedelta):
        async with self.lock:
            cutoff_time = datetime.now() - time_frame
            all_requests = self.df[self.df['timestamp'] >= cutoff_time].shape[0]
            status_codes = self.df[self.df['timestamp'] >= cutoff_time]['http_code'].value_counts()
        message = f"Total requests: {all_requests}\n"
        for code, count in status_codes.items():
            message += f"Status code {code}: {count} ({count / all_requests:.2%})\n"
        message = message.rstrip('\n')
        return message

    async def rps(self, time_frame: timedelta):
        async with self.lock:
            total_seconds = time_frame.total_seconds()
            cutoff_time = datetime.now() - time_frame
            request_count = self.df[self.df['timestamp'] >= cutoff_time].shape[0]
            outgoing_requests_per_second = request_count / total_seconds if total_seconds > 0 else 0
        return outgoing_requests_per_second


BYTES_DIFF_PROTOBUF = 64  # difference we always get between java and python protobuf for some reason


def normalize_string(v):
    return base64.b64encode(v.encode())


def normalize_string2(v):
    return base64.b64encode(binascii.unhexlify(v))


def raw_data_to_pbf_tx(tx: dict) -> int:
    data = copy.deepcopy(tx)
    raw_data = data["raw_data"]
    raw_contracts = raw_data["contract"]
    contracts = []

    for contract in raw_contracts:
        parameter = contract["parameter"]
        for key, value in parameter["value"].items():
            if isinstance(value, str):
                if key.endswith("address") and value[0] == "T":
                    parameter["value"][key] = base58.b58decode_check(parameter["value"][key]).hex()
                if key != "asset_name":
                    parameter["value"][key] = normalize_string2(parameter["value"][key])
                else:
                    parameter["value"][key] = normalize_string(parameter["value"][key])
        parameter.update(parameter.pop("value", {}))
        parameter["@type"] = parameter.pop("type_url", None)
        contracts.append(contract)

    raw_data["contract"] = contracts

    for key in ("data", "ref_block_bytes", "ref_block_hash"):
        if key in data["raw_data"]:
            data["raw_data"][key] = normalize_string2(data["raw_data"][key])

    return ParseDict(data, Transaction())


def calculate_tx_id(raw_data: dict) -> str:
    """
    Get the transaction id of the raw data.
    :param raw_data:
    :return:
    """
    pbtx = raw_data_to_pbf_tx({"raw_data": raw_data, "signature": []})
    msg_bytes = pbtx.raw_data.SerializeToString()
    return hashlib.sha256(msg_bytes).hexdigest()


def get_tx_size(transaction) -> int:
    return transaction.ByteSize() + BYTES_DIFF_PROTOBUF
