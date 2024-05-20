# -*- coding: utf-8 -*-
import asyncio
import eth_utils
import eth_abi
import httpx
from eth_utils import to_checksum_address
import time
from typing import Union, List, Tuple, Dict
from web3.eth import Eth
from web3.auto import w3

from providers import AsyncHTTPProvider
import eth_account
from eth_typing import ChecksumAddress
from exceptions import Web3Exception, StuckTransaction, TransactionNotFound, AlreadyKnown, UnderpricedTransaction, \
    InsufficientFundsForTx, TransactionFailed


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
        self.contract_address = to_checksum_address(contract_address)
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

    async def transferFrom(self, from_, to_, amount, signer_key, gas_price, gas=100000, nonce=None):
        return await self.send_transaction("transferFrom", (from_, to_, amount), signer_key, gas_price, gas, nonce)

    async def allowance(self, owner, spender) -> int:
        return await self.call_contract("allowance", (owner, spender))[0]

    async def balanceOf(self, address) -> int:
        return await self.call_contract("balanceOf", (address,))[0]


async def test():
    from web3.auto import w3
    from web3.eth import Eth

    async with AsyncEth("https://sepolia.infura.io/v3/bfe1a422517541399a1ffa598756b45a", 11155111) as client:
        # contract = ERC20(client, "0x0f1a713859fB1d1afAc99Fe2D20CAf639560EC83", min_abi)
        data = {"fromBlock": "0x5aa3b9",
                "toBlock": "0x5aa3b9",
                "topics": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]}
        print(await client.get_logs(data))
        # data = await client.get_block_by_number("0x5aa3b9")
        # print(data)
        # print(await contract.balanceOf("0x66f39eb22fA3F0664Cb0AA2BaaC402fE23925c64"))


if __name__ == "__main__":
    print("This module is not for direct call!")
    asyncio.run(test())
