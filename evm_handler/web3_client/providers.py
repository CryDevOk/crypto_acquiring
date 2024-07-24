import httpx
from httpx import Timeout
import time
import random

from web3_client.utils import RequestExplorer

from web3_client.exceptions import Web3Exception, \
    TransactionNotFound, \
    StuckTransaction, \
    AlreadyKnown, \
    UnderpricedTransaction, \
    InsufficientFundsForTx

DEFAULT_TIMEOUT = 10.0


class AsyncHTTPProvider:
    def __init__(self,
                 endpoint_uri: str | dict = None,
                 timeout: float = DEFAULT_TIMEOUT,
                 api_key: str = None,
                 request_explorer: RequestExplorer = None
                 ):
        self.endpoint_uri = endpoint_uri.rstrip("/") + "/" + api_key

        headers = {"Content-Type": "application/json"}

        self.client = httpx.AsyncClient(headers=headers, timeout=Timeout(timeout))
        self.request_explorer = request_explorer

        self.timeout = timeout
        """Request timeout in second."""

    async def make_request(self, _: str, params: type = None) -> dict:
        if params is None:
            params = {}

        resp = await self.client.post(self.endpoint_uri, json=params)
        if self.request_explorer:
            await self.request_explorer.add_request(resp.status_code)
        resp.raise_for_status()

        res = resp.json()
        if res.get("error"):
            if res["error"].get("code") == -32000:
                if res["error"].get("message", "").startswith("insufficient funds for gas"):
                    raise InsufficientFundsForTx(params)
                elif res["error"].get("message", "").startswith("replacement transaction underpriced"):
                    raise UnderpricedTransaction(params)
                elif res["error"].get("message", "").startswith("already known"):
                    raise AlreadyKnown(params)
                else:
                    raise Web3Exception(res["error"])
            else:
                raise Web3Exception(res["error"])
        else:
            return res


class AsyncGetblockHTTPProvider(AsyncHTTPProvider):
    name = "getblock"


class AsyncInfuraHTTPProvider(AsyncHTTPProvider):
    name = "infura"


class ProviderCaller:
    def __init__(self, provider,
                 url,
                 api_key):
        self.provider = provider
        self.url = url
        self.api_key = api_key
        self.request_explorer = RequestExplorer(self.provider.name)
        self.is_enabled = True
        self.pause = 0

    def __call__(self) -> AsyncGetblockHTTPProvider | AsyncInfuraHTTPProvider:
        self.last_used = time.time()
        return self.provider(endpoint_uri=self.url, api_key=self.api_key, request_explorer=self.request_explorer)


class ProvidersPool:
    def __init__(self, providers: list[ProviderCaller]):
        self.providers = providers

    async def get(self) -> AsyncGetblockHTTPProvider | AsyncInfuraHTTPProvider:
        enabled_providers = [provider for provider in self.providers if provider.is_enabled]
        return random.choice(enabled_providers)()

    async def refresh(self):
        disabled_providers = [provider for provider in self.providers if not provider.is_enabled]
        pass
