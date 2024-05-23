# -*- coding: utf-8 -*-
# Description: This module is responsible for handling the callback.

from misc import get_logger
from db.database import DB, write_async_session
from db.models import Callbacks
import api

import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta, timezone


async def execute_callback(
        callback_id: int,
        callback_period: int,
        callback_url: str,
        callback_api_key: str,
        path: str,
        json_data: dict[str, type]

) -> tuple[None, Exception, tuple[int, int]] | tuple[type, None, tuple[int, int]]:
    """
    This function is responsible for executing the callback.
    :return:
    """
    client = api.callback_api_client.Client(callback_url, callback_api_key)
    try:
        resp = await client.callback(path, json_data)
    except Exception as exc:
        return None, exc, (callback_id, callback_period)
    else:
        return resp, None, (callback_id, callback_period)


async def callback_handler(logger: logging.Logger) -> None:
    """
    This function is responsible for handling the callback.
    :param logger:
    :return:
    """
    reqs = []
    async with write_async_session() as session:
        db = DB(session, logger)

        callbacks = await db.get_and_lock_callbacks(20)

        if callbacks:
            for data in callbacks:
                reqs.append(asyncio.create_task(execute_callback(**data)))
            results = await asyncio.gather(*reqs)

            for data, exception, req_ident in results:
                callback_id, callback_period = req_ident
                if not exception:
                    await db.update_callback_by_id(callback_id, {Callbacks.is_notified.key: True,
                                                                 Callbacks.locked_by_callback.key: False},
                                                   commit=True)

                else:
                    if isinstance(exception, api.callback_api_client.ClientException) and exception.http_code == 409:
                        logger.warning(f"callback_id {callback_id} already notified")
                        await db.update_callback_by_id(callback_id,
                                                       {Callbacks.locked_by_callback.key: False,
                                                        Callbacks.is_notified.key: True}, commit=True)
                    else:
                        time_to_callback = datetime.now(timezone.utc) + timedelta(seconds=callback_period)
                        callback_period += 60
                        await db.update_callback_by_id(callback_id,
                                                       {Callbacks.locked_by_callback.key: False,
                                                        Callbacks.time_to_callback.key: time_to_callback,
                                                        Callbacks.callback_period.key: callback_period},
                                                       commit=True)
                        logger.error(f"callback {callback_id} {exception}")


async def main():
    scheduler = AsyncIOScheduler()
    scheduler._logger.setLevel(logging.ERROR)  # to avoid apscheduler noise warning logs
    scheduler.add_job(callback_handler, "interval", seconds=1, max_instances=1,
                      args=(get_logger("callback_handler"),))

    scheduler.start()
    while True:
        await asyncio.sleep(1000)


if __name__ == '__main__':
    asyncio.run(main())
