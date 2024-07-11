# -*- coding: utf-8 -*-
from tronpy.async_tron import AsyncTron, AsyncTrx, AsyncContract, DEFAULT_CONF, AsyncTransactionRet, AsyncTransaction, \
    AsyncTransactionBuilder, AsyncContractMethod
from tronpy.keys import PrivateKey, keccak256
from tronpy.abi import trx_abi
import httpx
import time
import asyncio
from tronpy.exceptions import (
    AddressNotFound,
    ApiError,
    AssetNotFound,
    BadHash,
    BadKey,
    BadSignature,
    BlockNotFound,
    BugInJavaTron,
    TaposError,
    TransactionError,
    TransactionNotFound,
    TvmError,
    UnknownError,
    ValidationError,
    BadAddress
)

from web3_client.utils import generate_mnemonic, keys_from_mnemonic, trc20_abi, TronRequestExplorer, calculate_tx_id


class BalanceError(Exception):
    pass


class EstimatedEnergyError(Exception):
    pass


class UnableToGetReceiptError(Exception):
    def __init__(self, original_error, tx_hash, message):
        self.original_error = original_error
        self.tx_hash = tx_hash
        self.message = message

    def __str__(self):
        return f"Unable to get receipt for transaction {self.tx_hash}: {self.message} {self.original_error}"


class BuildTransactionError(Exception):
    def __init__(self, original_error, message):
        self.original_error = original_error
        self.message = message

    def __str__(self):
        return f"Build transaction error: {self.message} {self.original_error}"


class Account(PrivateKey):
    def __init__(self, private_key_bytes: bytes):
        super().__init__(private_key_bytes)
        self.address = self.public_key.to_base58check_address()


def event_input_type(event_abi: dict) -> str:
    return "(" + (",".join(arg.get("type", "") for arg in event_abi["inputs"])) + ")"


def event_hash(event_abi: dict) -> str:
    return keccak256((event_abi["name"] + MyAsyncContract.event_input_type(event_abi)).encode()).hex()


class MyAsyncTransactionRet(AsyncTransactionRet):
    async def wait(self, timeout=30, interval=1.6, solid=False) -> dict:
        """Wait the transaction to be on chain.

        :returns: TransactionInfo
        """

        get_transaction_info = self._client.get_transaction_info
        if solid:
            get_transaction_info = self._client.get_solid_transaction_info

        end_time = time.time() + timeout
        exception = None
        while time.time() < end_time:
            try:
                return await get_transaction_info(self._txid)
            except (TransactionNotFound, httpx.HTTPError) as exc:
                exception = exc
                await asyncio.sleep(interval)

        if isinstance(exception, httpx.HTTPError):  # this can happen if the node is down
            raise UnableToGetReceiptError(exception, self._txid, "Failed to get transaction receipt")
        else:
            raise exception


class MyAsyncTransaction(AsyncTransaction):
    @classmethod
    async def create(cls, *args, **kwargs) -> "MyAsyncTransaction":
        return cls(*args, **kwargs)


class MyAsyncTransactionBuilder(AsyncTransactionBuilder):
    async def build(self, options=None, **kwargs) -> MyAsyncTransaction:
        """Build the transaction."""
        ref_block_id = await self._client.get_latest_solid_block_id()
        # last 2 byte of block number part
        self._raw_data["ref_block_bytes"] = ref_block_id[12:16]
        # last half part of block hash
        self._raw_data["ref_block_hash"] = ref_block_id[16:32]

        tx_id = calculate_tx_id(self._raw_data)

        if self._method:
            return await MyAsyncTransaction.create(self._raw_data, client=self._client, txid=tx_id, method=self._method)
        else:
            return await MyAsyncTransaction.create(self._raw_data, client=self._client, txid=tx_id)


class MyAsyncTrx(AsyncTrx):
    def _build_transaction(self, type_: str, obj: dict, *,
                           method: "AsyncContractMethod" = None) -> MyAsyncTransactionBuilder:
        inner = {
            "parameter": {"value": obj, "type_url": f"type.googleapis.com/protocol.{type_}"},
            "type": type_,
        }
        if method:
            return MyAsyncTransactionBuilder(inner, client=self.client, method=method)
        return MyAsyncTransactionBuilder(inner, client=self.client)


class MyAsyncTron(AsyncTron):
    def __init__(self, provider: callable, provider_args: dict = None, conf: dict = None):
        self.client = None

        self.conf = DEFAULT_CONF
        """The config dict."""

        if conf is not None:
            self.conf = dict(DEFAULT_CONF, **conf)

        self.provider = provider(**(provider_args or {}))
        self._trx = MyAsyncTrx(self)

        super(MyAsyncTron).__init__()

    async def broadcast_hex(self, hex_str: MyAsyncTransaction) -> dict:
        """Broadcast a hex string."""
        return await self.provider.make_request("wallet/broadcasthex", {"transaction": hex_str})

    async def broadcast_and_wait_result(self, txn) -> str:
        try:
            await self.broadcast(txn)
        except httpx.HTTPError:
            pass
        txn_ret = MyAsyncTransactionRet({"txid": txn.txid}, client=self, method=txn._method)
        if txn._method:
            await txn_ret.result()
            return txn_ret._txid
        else:
            await txn_ret.wait()
            return txn_ret._txid

    async def trx_transfer(self, to_, amount, signer_key, fee_limit=1000):
        try:
            signer_account = Account(bytes.fromhex(signer_key))
            txb = self.trx.transfer(signer_account.address, to_, amount).fee_limit(fee_limit)
            txn = await txb.build()
            txn = txn.sign(signer_account)
        except Exception as e:
            raise BuildTransactionError(e, f"Failed to build transaction: {e}")
        else:
            return await self.broadcast_and_wait_result(txn)

    async def get_account_balance(self, addr) -> int:
        try:
            info = await self.get_account(addr)
        except AddressNotFound:
            return 0
        else:
            return info.get("balance", 0)

    async def latest_block_number(self) -> int:
        data = await self.provider.http_api_request("post", "jsonrpc", json={
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_blockNumber"
        })
        return int(data["result"], 16)

    async def get_entire_block(self, block_num):
        return await self.provider.http_api_request("post", "wallet/getblockbynum",
                                                    json={"detail": True, "num": block_num})

    async def get_txs_of_block(self, block_num):
        return await self.provider.http_api_request("post", "wallet/gettransactioninfobyblocknum",
                                                    json={"num": block_num})


class MyAsyncContract(AsyncContract):
    def __init__(self, client, contract_address, abi_info, energy_price=420):
        self.energy_price = energy_price
        super().__init__(addr=contract_address,
                         bytecode=abi_info.get("bytecode", ""),
                         name=abi_info.get("name", ""),
                         abi=abi_info.get("abi", {}).get("entrys", []),
                         origin_energy_limit=abi_info.get("origin_energy_limit", 0),
                         user_resource_percent=abi_info.get("consume_user_resource_percent", 100),
                         origin_address=abi_info.get("origin_address", ""),
                         code_hash=abi_info.get("code_hash", ""),
                         client=client, )

    async def build_transaction(self, method_name: str, args, signer_account, fee_limit=100_000_000):
        method = getattr(self.functions, method_name)
        txb = await method(*args)
        txb = txb.with_owner(signer_account.address).fee_limit(fee_limit)
        txn = await txb.build()
        txn = txn.sign(signer_account)
        return txn

    async def estimate_energy(self, signer_address, encoded_data, function_signature, signature) -> int:
        bandwidth = len(encoded_data) + len(signature) + 69
        res = await self._client.provider.make_request("wallet/triggerconstantcontract",
                                                       {"owner_address": signer_address,
                                                        "contract_address": self.contract_address,
                                                        "function_selector": function_signature,
                                                        "parameter": encoded_data[8:],
                                                        "visible": True})
        if res.get("result", {}).get("result"):
            message = res.get("result", {}).get("message")
            if message is None:
                return int((res.get('energy_used', 0) * self.energy_price + bandwidth * 1000) * 1.1)
            else:
                try:
                    result = res.get("constant_result", [])
                    if result and len(result[0]) > (4 + 32) * 2:
                        error_msg = trx_abi.decode_single("string", bytes.fromhex(result[0])[4 + 32:])
                        message = f"triggerconstantcontract {message}: {error_msg}"
                except Exception:
                    pass
                raise EstimatedEnergyError(message)
        else:
            raise EstimatedEnergyError("No result in response")

    async def trigger_contract(self, method_name: str, args: tuple, signer_key: str):
        signer_account = Account(bytes.fromhex(signer_key))
        test_txn = await self.build_transaction(method_name, args, signer_account)
        parameters = test_txn._raw_data.get("contract", [{}])[0].get("parameter", {}).get("value", {}).get('data')

        sun_balance_need = await self.estimate_energy(signer_account.address, parameters,
                                                      test_txn._method.function_signature,
                                                      test_txn._signature[0])
        signer_balance = await self._client.get_account_balance(signer_account.address)

        if signer_balance < sun_balance_need:
            raise BalanceError(f"Insufficient balance for transaction: {signer_balance} < {sun_balance_need}")
        else:
            txn = await self.build_transaction(method_name, args, signer_account, fee_limit=sun_balance_need)
            return await self._client.broadcast_and_wait_result(txn)

    async def call_contract(self, method_name: str, args: tuple):
        method = getattr(self.functions, method_name)
        return await method(*args)


class TRC20(MyAsyncContract):
    async def transfer(self, to_, amount, signer_key):
        return await self.trigger_contract("transfer", (to_, amount), signer_key)

    async def approve(self, to_, amount, signer_key):
        return await self.trigger_contract("approve", (to_, amount), signer_key)

    async def transfer_from(self, from_, to_, amount, signer_key):
        return await self.trigger_contract("transferFrom", (from_, to_, amount), signer_key)

    async def allowance(self, owner, spender):
        return await self.call_contract("allowance", (owner, spender))

    async def balance_of(self, address):
        return await self.call_contract("balanceOf", (address,))


async def distribute_trx(client: MyAsyncTron,
                         accounts: list[PrivateKey],
                         amount: int,
                         priv_key: str):
    for account in accounts:
        resp = await client.trx_transfer(account.public_key.to_base58check_address(), amount, priv_key)
        print(resp)


async def distribute_trc20(client: MyAsyncTron,
                           accounts: list[PrivateKey],
                           amount: int,
                           priv_key: str,
                           contract_address: str):
    contract = TRC20(client, contract_address, trc20_abi)
    for account in accounts:
        resp = await contract.transfer(account.public_key.to_base58check_address(), amount, priv_key)
        print(resp)


async def prepare_new_accounts():
    import web3_client.providers as providers
    address_count: int = 4
    trx_amount = 1
    wei_amount = int(trx_amount * 10 ** 6)
    coin_amount = 20
    priv_key = ""
    contract_address = ""

    # conn_creds = (providers.AsyncZanHTTPProvider,
    #               {"endpoint_uri": "https://api.zan.top/node/v1/tron/nile",
    #                "api_key": ""})

    conn_creds = (providers.AsyncTronGridHTTPProvider,
                  {"endpoint_uri": "https://nile.trongrid.io",
                   "api_key": ""})

    mnem = generate_mnemonic()
    print(mnem)
    accounts = keys_from_mnemonic(mnem, address_count)
    async with MyAsyncTron(*conn_creds) as client:
        await distribute_trx(client, accounts, wei_amount, priv_key)
        await distribute_trc20(client, accounts, coin_amount, priv_key, contract_address)


if __name__ == "__main__":
    asyncio.run(prepare_new_accounts())
