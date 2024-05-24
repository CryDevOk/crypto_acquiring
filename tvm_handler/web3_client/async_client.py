from tronpy.async_tron import AsyncTron, AsyncTrx, AsyncContract, DEFAULT_CONF, AsyncTransactionRet
from tronpy.providers.async_http import AsyncHTTPProvider
from tronpy.keys import PrivateKey, keccak256
from tronpy.abi import trx_abi
from httpx import _exceptions as httpx_exc
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

from web3_client.utils import generate_mnemonic, keys_from_mnemonic, trc20_abi


class BalanceError(Exception):
    pass


class EstimatedEnergyError(Exception):
    pass


from urllib.parse import urljoin


class MyAsyncHTTPProvider(AsyncHTTPProvider):
    async def http_api_request(self, method, path, **kwargs):
        url = urljoin(self.endpoint_uri, path)
        resp = await self.client.request(method, url, **kwargs)
        resp.raise_for_status()
        return resp.json()


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
        while time.time() < end_time:
            try:
                return await get_transaction_info(self._txid)
            except (TransactionNotFound, httpx_exc.HTTPError):
                await asyncio.sleep(interval)

        raise TransactionNotFound("timeout and can not find the transaction")


class MyAsyncTron(AsyncTron):
    def __init__(self, server: str, api_key: str, conf: dict = None):
        self.server = server
        self.api_key = api_key
        self.client = None

        self.conf = DEFAULT_CONF
        """The config dict."""

        if conf is not None:
            self.conf = dict(DEFAULT_CONF, **conf)

        self.provider = None
        self._trx = AsyncTrx(self)

        super(MyAsyncTron).__init__()

    async def __aenter__(self):
        self.provider = MyAsyncHTTPProvider(self.server, api_key=self.api_key)
        return self

    async def broadcast_and_wait_result(self, txn):
        try:
            await self.broadcast(txn)
        except httpx_exc.HTTPError:
            pass
        txn_ret = MyAsyncTransactionRet({"txid": txn.txid}, client=self, method=txn._method)
        return await txn_ret.result()

    async def trx_transfer(self, to_, amount, signer_key, fee_limit=1000):
        signer_account = Account(bytes.fromhex(signer_key))
        txb = self.trx.transfer(signer_account.address, to_, amount).fee_limit(fee_limit)
        txn = await txb.build()
        txn = txn.sign(signer_account)
        return await self.broadcast_and_wait_result(txn)

    async def get_account_balance(self, addr) -> int:
        try:
            info = await self.get_account(addr)
        except AddressNotFound:
            return 0
        else:
            return info.get("balance", 0)

    async def latest_block_number(self) -> str:
        data = await self.provider.http_api_request("post", "walletsolidity/getblock")
        return data["block_header"]["raw_data"]["number"]

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

    async def balance_of(self, address) -> tuple:
        return await self.call_contract("balanceOf", (address,))


async def distribute_trx(client: MyAsyncTron, accounts: list[PrivateKey], amount: int, priv_key: str):
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
    address_count: int = 4
    trx_amount = 0.1
    wei_amount = int(trx_amount * 10 ** 6)
    coin_amount = 20_000_000
    priv_key = ""
    contract_address = ""
    provider_url = ""
    api_key = ""

    mnem = generate_mnemonic()
    print(mnem)
    accounts = keys_from_mnemonic(mnem, address_count)
    async with MyAsyncTron(provider_url, api_key) as client:
        await distribute_trx(client, accounts, wei_amount, priv_key)
        await distribute_trc20(client, accounts, coin_amount, priv_key, contract_address)


if __name__ == "__main__":
    asyncio.run(prepare_new_accounts())
