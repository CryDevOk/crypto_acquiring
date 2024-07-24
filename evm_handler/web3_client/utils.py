import eth_utils
import eth_account
from eth_utils.exceptions import ValidationError
from typing import List, Tuple
import mnemonic
import pandas as pd
import asyncio
from datetime import datetime, timedelta


erc20_abi = [
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [
            {
                "name": "",
                "type": "string"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": False,
        "inputs": [
            {
                "name": "_spender",
                "type": "address"
            },
            {
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "approve",
        "outputs": [
            {
                "name": "",
                "type": "bool"
            }
        ],
        "payable": False,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "totalSupply",
        "outputs": [
            {
                "name": "",
                "type": "uint256"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": False,
        "inputs": [
            {
                "name": "_from",
                "type": "address"
            },
            {
                "name": "_to",
                "type": "address"
            },
            {
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "transferFrom",
        "outputs": [
            {
                "name": "",
                "type": "bool"
            }
        ],
        "payable": False,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [
            {
                "name": "",
                "type": "uint8"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [
            {
                "name": "_owner",
                "type": "address"
            }
        ],
        "name": "balanceOf",
        "outputs": [
            {
                "name": "balance",
                "type": "uint256"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [
            {
                "name": "",
                "type": "string"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": False,
        "inputs": [
            {
                "name": "_to",
                "type": "address"
            },
            {
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "transfer",
        "outputs": [
            {
                "name": "",
                "type": "bool"
            }
        ],
        "payable": False,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [
            {
                "name": "_owner",
                "type": "address"
            },
            {
                "name": "_spender",
                "type": "address"
            }
        ],
        "name": "allowance",
        "outputs": [
            {
                "name": "",
                "type": "uint256"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "payable": True,
        "stateMutability": "payable",
        "type": "fallback"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": True,
                "name": "owner",
                "type": "address"
            },
            {
                "indexed": True,
                "name": "spender",
                "type": "address"
            },
            {
                "indexed": False,
                "name": "value",
                "type": "uint256"
            }
        ],
        "name": "Approval",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": True,
                "name": "from",
                "type": "address"
            },
            {
                "indexed": True,
                "name": "to",
                "type": "address"
            },
            {
                "indexed": False,
                "name": "value",
                "type": "uint256"
            }
        ],
        "name": "Transfer",
        "type": "event"
    }
]


def generate_mnemonic():
    return mnemonic.Mnemonic("english").generate(strength=128)


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


def keys_from_mnemonic(mnemonic, amount, offset=0) -> List[eth_account.account.LocalAccount]:
    accounts = []
    eth_account.Account.enable_unaudited_hdwallet_features()
    for i in range(amount):
        acc = eth_account.Account.from_mnemonic(mnemonic, account_path=f"m/44'/60'/0'/0/{i + offset}")
        accounts.append(acc)
        print(acc.address, acc.key.hex())
    return accounts


class RequestExplorer:
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
