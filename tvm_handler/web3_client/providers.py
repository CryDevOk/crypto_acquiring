# -*- coding: utf-8 -*-
from tronpy.providers import async_http
from urllib.parse import urljoin
import httpx

from web3_client.utils import TronRequestExplorer


class AsyncTronGridHTTPProvider(async_http.AsyncHTTPProvider):
    def __init__(
            self,
            endpoint_uri: str | dict = None,
            timeout: float = async_http.DEFAULT_TIMEOUT,
            api_key: str = async_http.DEFAULT_API_KEY,
            request_explorer: TronRequestExplorer = None
    ):
        super().__init__()

        self.endpoint_uri = endpoint_uri
        self.client = httpx.AsyncClient(
            headers={"User-Agent": f"Tronpy/{async_http.VERSION}", "Tron-Pro-Api-Key": api_key},
            timeout=async_http.Timeout(timeout))

        self.request_explorer = request_explorer
        """Request timeout in second."""

    async def http_api_request(self, method, path, **kwargs):
        url = urljoin(self.endpoint_uri, path)
        resp = await self.client.request(method, url, **kwargs)
        if self.request_explorer:
            await self.request_explorer.add_request(resp.status_code)
        resp.raise_for_status()
        return resp.json()

    async def make_request(self, method: str, params: type = None) -> dict:
        if params is None:
            params = {}
        url = urljoin(self.endpoint_uri, method)
        resp = await self.client.post(url, json=params)
        if self.request_explorer:
            await self.request_explorer.add_request(resp.status_code)
        resp.raise_for_status()
        return resp.json()


class AsyncZanHTTPProvider(async_http.AsyncHTTPProvider):
    def __init__(
            self,
            endpoint_uri: str = None,
            timeout: float = async_http.DEFAULT_TIMEOUT,
            api_key: str = async_http.DEFAULT_API_KEY,
            request_explorer: TronRequestExplorer = None
    ):
        super().__init__()

        self.endpoint_uri = endpoint_uri.rstrip("/") + "/" + api_key + "/"
        self.client = httpx.AsyncClient(
            headers={"accept": "application/json",
                     "content-type": "application/json"},
            timeout=async_http.Timeout(timeout))

        self.request_explorer = request_explorer
        """Request timeout in second."""

    async def http_api_request(self, method, path, **kwargs):
        url = urljoin(self.endpoint_uri, path)
        resp = await self.client.request(method, url, **kwargs)
        if self.request_explorer:
            await self.request_explorer.add_request(resp.status_code)
        resp.raise_for_status()
        return resp.json()

    async def make_request(self, method: str, params: type = None) -> dict:
        if params is None:
            params = {}
        url = urljoin(self.endpoint_uri, method)
        resp = await self.client.post(url, json=params)
        if self.request_explorer:
            await self.request_explorer.add_request(resp.status_code)
        resp.raise_for_status()
        return resp.json()
