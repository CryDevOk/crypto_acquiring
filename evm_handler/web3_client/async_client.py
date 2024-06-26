# -*- coding: utf-8 -*-
import asyncio
import eth_utils
import eth_abi
import httpx
import time
from typing import Union, List, Tuple, Dict
from web3.eth import Eth
from web3.auto import w3

import eth_account
from eth_typing import ChecksumAddress

from web3_client.providers import AsyncHTTPProvider
from web3_client.exceptions import Web3Exception, \
    StuckTransaction, \
    TransactionNotFound, \
    AlreadyKnown, \
    UnderpricedTransaction, \
    InsufficientFundsForTx, \
    TransactionFailed
from web3_client.utils import generate_mnemonic, keys_from_mnemonic, erc20_abi


def hex_to_int(hex_str):
    return int(hex_str, 16)


class AsyncEth():
    def __init__(self, server, network_id):
        self.server = server
        self.client = None
        self.network_id = network_id
        self.provider = None

    async def __aenter__(self):
        self.provider = AsyncHTTPProvider(self.server)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.provider.client.aclose()

    async def result(self, tx_hash):
        await self.wait_for_mempool(tx_hash)
        await self.wait_for_mined(tx_hash)
        receipt = await self.wait_for_receipt(tx_hash)
        if receipt.get("status") == '0x1':
            return tx_hash
        else:
            raise TransactionFailed(tx_hash=tx_hash)

    async def broadcast_and_wait_result(self, txn):
        try:
            await self.send_raw_transaction(txn.rawTransaction.hex())
        except httpx.HTTPError:
            return await self.result(txn.hash.hex())
        else:
            return await self.result(txn.hash.hex())

    async def send_ether(self, to_: ChecksumAddress, amount: int, signer_key: str, gas_price: int, gas: int,
                         nonce: int = None):
        account = eth_account.Account.from_key(signer_key)

        if not nonce:
            nonce = await self.get_transaction_count(account.address)

        transaction = {'value': eth_utils.to_hex(amount),
                       'chainId': int(self.network_id),
                       'gas': gas,
                       'gasPrice': gas_price,
                       'from': account.address,
                       'nonce': eth_utils.to_hex(nonce),
                       'to': to_}
        txn = account.sign_transaction(transaction)
        return await self.broadcast_and_wait_result(txn)

    async def wait_for_mempool(self, tx_hash, timeout=120, interval=3):
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                return await self.is_transaction_in_mempool(tx_hash)
            except (Web3Exception, TransactionNotFound, httpx.HTTPError, httpx.HTTPStatusError):
                await asyncio.sleep(interval)
        return await self.is_transaction_in_mempool(tx_hash)

    async def wait_for_mined(self, tx_hash, timeout=60, interval=3) -> dict:
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                return await self.is_transaction_mined(tx_hash)
            except (Web3Exception, StuckTransaction, httpx.HTTPError, httpx.HTTPStatusError):
                await asyncio.sleep(interval)
        return await self.is_transaction_mined(tx_hash)

    async def wait_for_receipt(self, tx_hash, timeout=30, interval=3) -> dict:
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                return await self.get_transaction_receipt(tx_hash)
            except (httpx.HTTPError, httpx.HTTPStatusError):
                await asyncio.sleep(interval)
        return await self.get_transaction_receipt(tx_hash)

    async def get_transaction_receipt(self, tx_hash) -> dict:
        res = await self.provider.make_request("", {"jsonrpc": "2.0", "method": "eth_getTransactionReceipt",
                                                    "params": [tx_hash], "id": 1})
        return res["result"]

    async def get_transaction_by_hash(self, tx_hash) -> dict:
        res = await self.provider.make_request("", {"jsonrpc": "2.0", "method": "eth_getTransactionByHash",
                                                    "params": [tx_hash], "id": 1})
        return res["result"]

    async def is_transaction_mined(self, tx_hash):
        res = await self.get_transaction_by_hash(tx_hash)
        if res.get('transactionIndex') is None:
            raise StuckTransaction(res.get("nonce"), tx_hash)
        else:
            return res

    async def is_transaction_in_mempool(self, tx_hash):
        res = await self.get_transaction_by_hash(tx_hash)
        if res is None:
            raise TransactionNotFound(tx_hash)
        else:
            return res

    async def send_raw_transaction(self, data):
        res = await self.provider.make_request("",
                                               {"jsonrpc": "2.0", "method": "eth_sendRawTransaction", "params": [data],
                                                "id": 1})
        return res["result"]

    async def call(self, data):
        res = await self.provider.make_request("", {"jsonrpc": "2.0", "method": "eth_call", "params": [data, "latest"],
                                                    "id": 1})
        return res["result"]

    async def gas_price(self) -> int:
        res = await self.provider.make_request("", {"jsonrpc": "2.0", "method": "eth_gasPrice", "params": [], "id": 1})
        return hex_to_int(res["result"])

    async def get_account_balance(self, addr) -> int:
        res = await self.provider.make_request("", {"jsonrpc": "2.0", "method": "eth_getBalance",
                                                    "params": [addr, "latest"], "id": 1})
        return hex_to_int(res["result"])

    async def get_transaction_count(self, addr) -> int:
        res = await self.provider.make_request("", {"jsonrpc": "2.0", "method": "eth_getTransactionCount",
                                                    "params": [addr, "latest"], "id": 1})
        return hex_to_int(res["result"])

    async def get_chain_id(self) -> int:
        res = await self.provider.make_request("", {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1})
        return hex_to_int(res["result"])

    async def latest_block_number(self) -> int:
        res = await self.provider.make_request("",
                                               {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1})
        return hex_to_int(res["result"])

    async def get_logs(self, data) -> List[Dict]:
        res = await self.provider.make_request("",
                                               {"jsonrpc": "2.0", "method": "eth_getLogs", "params": [data], "id": 1})
        return res["result"]

    async def get_block_by_number(self, block_number) -> Dict:
        res = await self.provider.make_request("", {"jsonrpc": "2.0", "method": "eth_getBlockByNumber",
                                                    "params": [block_number, True], "id": 1})
        return res["result"]


class AsyncContract():
    def __init__(self, client: AsyncEth, contract_address: str, abi_info):
        self.client = client
        self.contract_address = eth_utils.to_checksum_address(contract_address)
        self.abi_info = abi_info
        self.contract_obj = Eth(w3).contract(self.contract_address, abi=abi_info, decode_tuples=True)

    async def send_transaction(self, method_name: str,
                               args: tuple,
                               signer_key: str,
                               gas_price: int,
                               gas: int,
                               nonce: int = None):
        account = eth_account.Account.from_key(signer_key)

        if not nonce:
            nonce = await self.client.get_transaction_count(account.address)

        tx_data = {'value': eth_utils.to_hex(0),
                   'chainId': int(self.client.network_id),
                   'gas': gas,
                   'gasPrice': gas_price,
                   'from': account.address,
                   'nonce': eth_utils.to_hex(nonce)}

        method = getattr(self.contract_obj.functions, method_name)
        txb = method(*args)
        txb = txb.build_transaction(tx_data)

        txn = account.sign_transaction(txb)
        return await self.client.broadcast_and_wait_result(txn)

    async def call_contract(self, method_name: str, args: tuple):
        tx_data = {"value": eth_utils.to_hex(0),
                   'gasPrice': None,
                   'gas': eth_utils.to_hex(100000),
                   "chainId": eth_utils.to_hex(self.client.network_id)}

        method = self.contract_obj.get_function_by_name(method_name)

        txb = method(*args)

        txb = txb.build_transaction(tx_data)
        resp = await self.client.call(txb)
        return eth_abi.decode([x.get('type') for x in method.abi.get("outputs")], bytes.fromhex(resp[2:]))


class ERC20(AsyncContract):
    async def transfer(self, to_, amount, signer_key, gas_price, gas=100000, nonce=None):
        return await self.send_transaction("transfer", (to_, amount), signer_key, gas_price, gas, nonce)

    async def approve(self, to_, amount, signer_key, gas_price, gas=100000, nonce=None):
        return await self.send_transaction("approve", (to_, amount), signer_key, gas_price, gas, nonce)

    async def transfer_from(self, from_, to_, amount, signer_key, gas_price, gas=100000, nonce=None):
        return await self.send_transaction("transferFrom", (from_, to_, amount), signer_key, gas_price, gas, nonce)

    async def allowance(self, owner, spender) -> int:
        resp = await self.call_contract("allowance", (owner, spender))
        return resp[0]

    async def balance_of(self, address) -> int:
        resp = await self.call_contract("balanceOf", (address,))
        return resp[0]


async def distribute_eth(client: AsyncEth, accounts: List[eth_account.account.LocalAccount], amount: int,
                         priv_key: str):
    gas_price = int((await client.gas_price()) * 1.5)
    for acc in accounts:
        resp = await client.send_ether(acc.address, amount, priv_key, gas_price=gas_price, gas=21000)
        print(resp)


async def distribute_erc20(client: AsyncEth,
                           accounts: List[eth_account.account.LocalAccount],
                           amount: int,
                           priv_key: str,
                           contract_address: str):
    contract = ERC20(client, contract_address, erc20_abi)
    gas_price = int((await client.gas_price()) * 1.5)
    for account in accounts:
        resp = await contract.transfer(account.address, amount, priv_key, gas_price)
        print(resp)


async def prepare_new_accounts():
    address_count: int = 4
    eth_amount = 0.1
    wei_amount = int(eth_amount * 10 ** 18)
    coin_amount = 20_000_000
    priv_key = ""
    contract_address = ""
    network_id = 11155111
    provider_url = ""

    mnem = generate_mnemonic()
    print(mnem)
    accounts = keys_from_mnemonic(mnem, address_count)
    async with AsyncEth(provider_url, network_id) as client:
        await distribute_eth(client, accounts, wei_amount, priv_key)
        await distribute_erc20(client, accounts, coin_amount, priv_key, contract_address)


if __name__ == "__main__":
    asyncio.run(prepare_new_accounts())
