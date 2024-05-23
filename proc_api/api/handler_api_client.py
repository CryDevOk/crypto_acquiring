# -*- coding: utf-8 -*-
import httpx
import json
from typing import Union


class ClientException(Exception):
    def __init__(self, response: Union[str, dict], http_code: int = None):
        self.http_code = http_code
        self.code = None
        self.message = 'Unknown Error'
        self.response = response
        if isinstance(response, str):
            self.message = response
        elif isinstance(response, dict):
            if 'errorCode' in response:
                self.code = response['errorCode']
            if 'error' in response:
                self.message = response['error']
        message = f"Error http_code: {self.http_code}, service_code: {self.code}, message: {self.message}"
        super(ClientException, self).__init__(message)


class Client(object):
    def __init__(self, server, api_key=None):
        self.server = server
        self.API_KEY = api_key
        self.headers = {"accept": "application/json", "Api-Key": self.API_KEY}

    async def _api_request(self, method, url, **kwargs):
        timeout = httpx.Timeout(timeout=10)
        async with httpx.AsyncClient(timeout=timeout) as session:
            resp = await session.request(method, self.server + url, headers=self.headers, **kwargs)
            try:
                data = resp.json()
            except json.JSONDecodeError:
                raise ClientException(resp.text, resp.status_code)
            else:
                if resp.status_code != 200:
                    raise ClientException(data, resp.status_code)
                else:
                    return data

    async def get_handler_info(self):
        return await self._api_request('GET', '/get_handler_info')

    async def readiness(self):
        return await self._api_request('GET', '/readiness')

    async def add_account(self, user_id):
        return await self._api_request('POST', '/add_account', json={"user_id": user_id})

    async def get_deposit_info(self, **data):
        return await self._api_request('GET', '/get_deposit_info', params=data)

    async def get_withdraw_info(self, **data):
        return await self._api_request('GET', '/get_withdraw_info', params=data)

    async def create_withdrawal(self, **data):
        return await self._api_request('POST', '/create_withdrawal', json=data)

    async def get_handled_blocks(self, **data):
        return await self._api_request('GET', '/get_handled_blocks', params=data)