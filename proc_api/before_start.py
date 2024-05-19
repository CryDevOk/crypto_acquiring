#! /usr/bin/python3
# -*- coding: utf-8 -*-
import asyncio

from db.database import DB, write_async_session, engine
from db.models import Base
from config import Config as Cfg
from handler_api_client import Client
from misc import startup_logger
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text


async def create_models():
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"))
        await conn.run_sync(Base.metadata.create_all)


def get_handlers_creds():
    handlers = Cfg.PROC_HANDLER_URLS.split(",")
    for handler in handlers:
        handler_creds = handler.split("|")
        if len(handler_creds) != 2:
            raise ValueError(f"Invalid handler format: {handler_creds}")
        yield handler_creds[0], handler_creds[1]


async def startup():
    try:
        await create_models()

        async with write_async_session() as session:
            db = DB(session)
            for handler_url, handler_api_key in get_handlers_creds():
                client = Client(handler_url, handler_api_key)
                resp: dict = await client.get_handler_info()

                try:
                    await db.add_handler(resp["name"], resp["display_name"], handler_url, handler_api_key)
                except IntegrityError as e:
                    if "unique" in str(e.orig):
                        pass
                    else:
                        print(f"Error: {e}")
                        startup_logger.error(f"Error: {e}")
                        exit(1)
                except Exception as e:
                    print(f"Error: {e}")
                    startup_logger.error(f"Error: {e}")
                    exit(1)

    except Exception as e:
        print(f"Error: {e}")
        startup_logger.error(f"Error: {e}")
        exit(1)
    else:
        print("First run completed successfully")
        startup_logger.info("First run completed successfully")


if __name__ == "__main__":
    asyncio.run(startup())
