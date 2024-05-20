from typing import Any, Union
from urllib.parse import urljoin
import httpx
from httpx import Timeout

from exceptions import Web3Exception, \
    TransactionNotFound, \
    StuckTransaction, \
    AlreadyKnown, \
    UnderpricedTransaction, \
    InsufficientFundsForTx

DEFAULT_TIMEOUT = 10.0


class AsyncHTTPProvider:
    def __init__(self, endpoint_uri: str = None, jw_token: str = None, timeout=DEFAULT_TIMEOUT):
        self.endpoint_uri = endpoint_uri

        headers = {"Content-Type": "application/json"}

        self.client = httpx.AsyncClient(headers=headers, timeout=Timeout(timeout))

        self.timeout = timeout
        """Request timeout in second."""

    async def make_request(self, method: str, params: Any = None) -> dict:
        if params is None:
            params = {}
        url = urljoin(self.endpoint_uri, method)

        resp = await self.client.post(url, json=params)
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
