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
    def __init__(self, server: str, api_key=None):
        self.server = server.rstrip('/')
        self.API_KEY = api_key
        self.headers = {"accept": "application/json", "Api-Key": self.API_KEY}

    async def _api_request(self, method, path, **kwargs):
        timeout = httpx.Timeout(timeout=10)
        async with httpx.AsyncClient(timeout=timeout) as session:
            resp = await session.request(method, self.server + path, headers=self.headers, **kwargs)
            try:
                data = resp.json()
            except json.JSONDecodeError:
                raise ClientException(resp.text, resp.status_code)
            else:
                if resp.status_code != 200:
                    raise ClientException(data, resp.status_code)
                else:
                    return data

    async def readiness(self):
        return await self._api_request('GET', '/readiness')

    async def add_callback(self, callback_id, user_id, path, json_data):
        return await self._api_request('POST', '/v1/api/private/callback', json={"callback_id": callback_id,
                                                                                 "user_id": user_id,
                                                                                 "path": path,
                                                                                 "json_data": json_data})
