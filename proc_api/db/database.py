# -*- coding: utf-8 -*-
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import and_, func, select, update, Column, Row, literal_column

from db.models import User, NetworkHandlers, Customer, Callbacks
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


def array_to_dict(columns: list[Column], values: Row[tuple[type]]):
    resp = {}
    if values:
        for index, col in enumerate(columns):
            resp[col.key] = values[index]
    return resp


class DB(object):
    def __init__(self, session: AsyncSession, logger=None):
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

    async def update_customer_by_callback_url(self, callback_url, data: dict):
        stmt = update(Customer).where(Customer.callback_url == callback_url).values(data).returning(Customer.id)
        resp = await self.session.execute(stmt)
        data = resp.fetchone()
        await self.session.commit()
        return array_to_dict([Customer.id], data)

    async def verify_customer(self, customer_id, api_key) -> bool:
        stmt = select(Customer.id).where(and_(Customer.id == customer_id, Customer.api_key == api_key))
        resp = await self.session.execute(stmt)
        data = resp.fetchone()
        return bool(data)

    async def verify_customer_and_user(self, customer_id, api_key, user_id) -> tuple[bool, bool]:
        stmt = select(Customer.id, User.customer_id).outerjoin(
            User,
            and_(User.customer_id == Customer.id, User.id == user_id)
        ).where(and_(Customer.id == customer_id, Customer.api_key == api_key))

        resp = await self.session.execute(stmt)
        data = resp.fetchone()
        return bool(data), bool(data[1])

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

    async def insert_callback(self, callback_id, user_id, path, json_data):
        stmt = postgresql.insert(Callbacks).values({Callbacks.id.key: callback_id,
                                                    Callbacks.user_id.key: user_id,
                                                    Callbacks.path.key: path,
                                                    Callbacks.json_data.key: json_data})
        try:
            await self.session.execute(stmt)
            await self.session.commit()
        except Exception as e:
            await self.session.rollback()
            raise e

    async def get_and_lock_callbacks(self, limit):
        subquery = (select(Callbacks.id.label('callback_id'),
                           Callbacks.callback_period,
                           Customer.callback_url,
                           Customer.callback_api_key,
                           Callbacks.path,
                           Callbacks.json_data
                           ).where(and_(Callbacks.is_notified == False,
                                        Callbacks.locked_by_callback == False,
                                        Callbacks.time_to_callback < func.NOW())
                                   )
                    .limit(limit).with_for_update(skip_locked=True)
                    .join(User, User.id == Callbacks.user_id)
                    .join(Customer, Customer.id == User.customer_id)
                    )

        columns = [subquery.c.callback_id,
                   subquery.c.callback_period,
                   subquery.c.callback_url,
                   subquery.c.callback_api_key,
                   subquery.c.path,
                   subquery.c.json_data]

        stmt = (
            update(Callbacks)
            .values(locked_by_callback=True)
            .where(and_(
                Callbacks.id == subquery.c.callback_id
            ))
            .returning(
                *columns
            )
        )

        try:
            resp = await self.session.execute(stmt)
            data = resp.fetchall()
            await self.session.commit()
            return [array_to_dict(columns, row) for row in data]
        except Exception as exc:
            await self.session.rollback()
            raise exc

    async def update_callback_by_id(self, callback_id, data, commit=False):
        stmt = update(Callbacks).where(Callbacks.id == callback_id).values(data)
        try:
            await self.session.execute(stmt)
            if commit:
                await self.session.commit()
        except Exception as exc:
            await self.session.rollback()
            raise exc
