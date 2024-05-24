#!/usr/bin/python3
# -*- coding: utf-8 -*-
import httpx
import asyncio
from typing import Union, Tuple


async def binance_get_tickers() -> Tuple[Union[dict, None], Union[Exception, None]]:
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"https://api.binance.com/api/v3/ticker/price")
            resp.raise_for_status()
            data = resp.json()
            return {item['symbol']: float(item['price']) for item in data}, None
    except Exception as e:
        return None, e


async def bybit_get_tickers() -> Tuple[Union[dict, None], Union[Exception, None]]:
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"https://api.bybit.com/v5/market/tickers?category=spot")
            data = response.json()
            return {item['symbol']: float(item['lastPrice']) for item in data['result']["list"]}, None
    except Exception as e:
        return None, e


async def get_coin_rates() -> Tuple[dict, list]:
    tasks = [asyncio.create_task(binance_get_tickers()), asyncio.create_task(bybit_get_tickers())]
    results = await asyncio.gather(*tasks)
    resp = {}
    exceptions = []
    for prices, exception in results:
        if exception:
            exceptions.append(exception)
        else:
            resp.update(prices)
    return resp, exceptions
