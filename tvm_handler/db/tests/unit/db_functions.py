#! /usr/bin/python3
# -*- coding: utf-8 -*-
from dotenv import dotenv_values
import os
import asyncio

config_ = dotenv_values("../../../../.env_proc_tron_nile")

for k, v in config_.items():
    os.environ[k] = v

from db.database import DB, read_async_session, write_async_session


async def get_withdrawals():
    async with read_async_session() as session:
        db = DB(session, None)
        withdrawals = await db.get_and_lock_pending_withdrawals()
        print(withdrawals)


async def get_deposits():
    async with read_async_session() as session:
        db = DB(session, None)
        deposits = await db.get_and_lock_pending_deposits_coin()
        print(deposits)

if __name__ == "__main__":
    asyncio.run(get_withdrawals())
