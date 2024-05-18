# -*- coding: utf-8 -*-
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import and_
from sqlalchemy.sql import select

from .models import User, NetworkHandlers, Customer
from config import Config as Cfg


def dsn2alchemy_conn_string(dsn_string):
    r = dict(s.split("=") for s in dsn_string.split())
    return f'postgresql+asyncpg://' + f"{r['user']}:{r['password']}@{r['host']}:{r['port']}/{r['dbname']}"


engine = create_async_engine(dsn2alchemy_conn_string(Cfg.WRITE_DSN), pool_timeout=10, pool_recycle=3600,
                             pool_size=Cfg.WRITE_POOL_SIZE, max_overflow=0, future=True)
read_engine = create_async_engine(dsn2alchemy_conn_string(Cfg.READ_DSN), pool_timeout=10, pool_recycle=3600,
                                  pool_size=Cfg.READ_POOL_SIZE, max_overflow=0, future=True)

write_async_session = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
read_async_session = async_sessionmaker(read_engine, expire_on_commit=False, autoflush=False)


class DB(object):
    def __init__(self, session, logger=None):
        self.session = session
        self.logger = logger

    async def insert_customer(self, customer_id, callback_url, callback_api_key, api_key):
        stmt = postgresql.insert(Customer).values({Customer.id.key: customer_id,
                                                   Customer.callback_url.key: callback_url,
                                                   Customer.callback_api_key.key: callback_api_key,
                                                   Customer.api_key.key: api_key})
        try:
            await self.session.execute(stmt)
            await self.session.commit()
        except Exception as e:
            await self.session.rollback()
            raise e

    async def verify_customer(self, customer_id, api_key, user_id=None):
        if user_id:
            stmt = select(User.id).join(Customer, User.customer_id == Customer.id).where(
                and_(Customer.id == customer_id,
                     Customer.api_key == api_key,
                     User.id == user_id))
        else:
            stmt = select(Customer.id).where(and_(Customer.id == customer_id, Customer.api_key == api_key))
        resp = await self.session.execute(stmt)
        return resp.rowcount == 1

    async def insert_user(self, user_id, customer_id, role):
        stmt = postgresql.insert(User).values({User.id.key: user_id,
                                               User.customer_id.key: customer_id,
                                               User.role.key: role})
        await self.session.execute(stmt)

    async def get_handlers(self, columns: list):
        stmt = select(*columns).where(NetworkHandlers.is_active == True)
        return await self.session.execute(stmt)

    async def get_tx_handler(self, tx_handler: str, columns: list):
        stmt = select(*columns)
        stmt = stmt.where(and_(NetworkHandlers.name == tx_handler, NetworkHandlers.is_active == True))
        resp = await self.session.execute(stmt)
        return resp.fetchone()

    async def add_handler(self, name, display_name, server_url, api_key):
        stmt = postgresql.insert(NetworkHandlers).values({
            NetworkHandlers.name.key: name,
            NetworkHandlers.display_name.key: display_name,
            NetworkHandlers.server_url.key: server_url,
            NetworkHandlers.api_key.key: api_key})
        try:
            await self.session.execute(stmt)
            await self.session.commit()
        except Exception as e:
            await self.session.rollback()
            raise e
