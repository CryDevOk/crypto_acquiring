#! /bin/usr/python3
# -*- coding: utf-8 -*-
from sqlalchemy import Column, and_, func, select, update
from sqlalchemy.dialects import postgresql
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from decimal import Decimal
from typing import List, Tuple, Any, Union
from sqlalchemy.orm import aliased

from .models import Users, UserAddress, Deposits, Withdrawals, Blocks, Coins, Balances
from config import Config as Cfg, StatCode as St


def dsn2alchemy_conn_string(dsn_string):
    r = dict(s.split("=") for s in dsn_string.split())
    return f'postgresql+asyncpg://' + f"{r['user']}:{r['password']}@{r['host']}:{r['port']}/{r['dbname']}"


engine = create_async_engine(dsn2alchemy_conn_string(Cfg.WRITE_DSN), pool_timeout=10, pool_recycle=3600,
                             pool_size=Cfg.WRITE_POOL_SIZE, max_overflow=0, future=True, query_cache_size=0)
engine.execution_options(compiled_cache=None)
read_engine = create_async_engine(dsn2alchemy_conn_string(Cfg.READ_DSN), pool_timeout=10, pool_recycle=3600,
                                  pool_size=Cfg.WRITE_POOL_SIZE, max_overflow=0, future=True, query_cache_size=0)
engine.execution_options(compiled_cache=None)

write_async_session = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
read_async_session = async_sessionmaker(read_engine, expire_on_commit=False, autoflush=False)


def array_prepare_to_json(columns, values):
    resp = {}
    for index, col in enumerate(columns):
        if isinstance(values[index], Decimal):
            resp[col.key] = str(values[index])
        elif isinstance(values[index], datetime):
            resp[col.key] = values[index].strftime(Cfg.TIME_FORMAT)
        elif isinstance(values[index], set):
            resp[col.key] = list(values[index])
        else:
            resp[col.key] = values[index]
    return resp


def array_to_dict(columns: List[Column], values: List[Any]):
    resp = {}
    if values:
        for index, col in enumerate(columns):
            resp[col.key] = values[index]
    return resp


def unpack_column_to_array(values):
    return [x[0] for x in values]


class DB(object):
    def __init__(self, session, logger=None):
        self.session = session
        self.logger = logger

    async def _insert_user(self, user_id, role):
        stmt = postgresql.insert(Users).values({Users.id.key: user_id, Users.role.key: role})
        await self.session.execute(stmt)

    async def get_user_by_id(self, user_id: str, columns: List[Column]):
        stmt = select(*columns).where(Users.id == user_id)
        resp = await self.session.execute(stmt)
        data = resp.fetchone()
        return array_to_dict(columns, data)

    async def get_last_handled_block(self) -> int:
        stmt = select(func.max(Blocks.id))
        resp = await self.session.execute(stmt)
        return resp.scalar_one_or_none()

    async def insert_last_handled_block(self, block_id: int, commit: bool = False) -> None:
        stmt = postgresql.insert(Blocks).values({Blocks.id.key: block_id})
        await self.session.execute(stmt)
        if commit:
            try:
                await self.session.commit()
            except Exception as exc:
                await self.session.rollback()
                raise exc

    async def get_user_deposit_info(self, user_id: str):
        stmt = select(UserAddress.public)
        stmt = stmt.where(UserAddress.user_id == user_id)
        resp = await self.session.execute(stmt)
        return resp.scalar_one_or_none()

    async def users_addresses(self, role: Union[int, list] = None, limit: int = None) -> List[Tuple[str, str]]:
        stmt = select(UserAddress.id, UserAddress.public)
        if role:
            stmt = stmt.join(Users, Users.id == UserAddress.user_id)
            if isinstance(role, int):
                stmt = stmt.where(Users.role == role)
            elif isinstance(role, list):
                stmt = stmt.where(Users.role.in_(role))
        if limit:
            stmt = stmt.limit(limit)
        resp = await self.session.execute(stmt)
        return resp.fetchall()

    async def all_accounts(self) -> List[Tuple[str, str]]:
        stmt = select(UserAddress.id, UserAddress.public, Users.role)
        stmt = stmt.join(Users, Users.id == UserAddress.user_id)
        resp = await self.session.execute(stmt)
        return resp.fetchall()

    async def get_random_user_id(self, role: Union[int, None] = None):
        stmt = select(Users.id)
        if role:
            stmt = stmt.where(Users.role == role)
        stmt = stmt.order_by(func.random()).limit(1)
        resp = await self.session.execute(stmt)
        return resp.scalar_one_or_none()

    async def _create_user_address(self, user_id: str, admin_id: str, approve_id: str, public: str, private: str):
        stmt = postgresql.insert(UserAddress).values({
            UserAddress.user_id.key: user_id,
            UserAddress.admin_id.key: admin_id,
            UserAddress.approve_id.key: approve_id,
            UserAddress.public.key: public,
            UserAddress.private.key: private
        })
        try:
            await self.session.execute(stmt)
            await self.session.commit()
        except Exception as exc:  # TODO: change exception handling
            await self.session.rollback()
            raise exc

    async def add_deposits(self, deposits: List[dict], commit: bool = False):
        """[{
            Deposits.address_id.key: address_id,
            Deposits.tx_hash_in.key: tx_hash_in,
            Deposits.amount.key: amount,
            Deposits.contract_address.key: contract_address,
        },...]"""
        stmt = postgresql.insert(Deposits).values(deposits)

        try:
            await self.session.execute(stmt)
            if commit:
                await self.session.commit()
        except Exception as exc:
            await self.session.rollback()
            raise exc

    async def add_withdrawal(self, user_id, address, amount, quote_amount, contract_address, user_currency):
        stmt = postgresql.insert(Withdrawals).values({
            Withdrawals.user_id.key: user_id,
            Withdrawals.withdrawal_address.key: address,
            Withdrawals.amount.key: amount,
            Withdrawals.quote_amount.key: quote_amount,
            Withdrawals.contract_address.key: contract_address,
            Withdrawals.user_currency.key: user_currency
        })
        stmt = stmt.returning(Withdrawals.id)
        try:
            resp = await self.session.execute(stmt)
            await self.session.commit()
        except Exception as exc:
            await self.session.rollback()
            raise exc
        else:
            return resp.scalar()

    async def add_account(self, user_id, admin_id, approve_id, addr, key, role=St.USER.v):
        try:
            await self._insert_user(user_id, role)
            await self._create_user_address(user_id, admin_id, approve_id, addr, key)
            await self.session.commit()
        except Exception as exc:
            await self.session.rollback()
            raise exc
        else:
            return True

    async def update_deposit_by_id(self, dep_id: str, data: dict, commit: bool = False):
        stmt = update(Deposits).where(Deposits.id == dep_id)
        stmt = stmt.values(data)
        resp = await self.session.execute(stmt, execution_options={"synchronize_session": False})
        if commit:
            await self.session.commit()
        return resp

    async def update_withdrawal_by_id(self, withdrawal_id: str, data: dict, commit: bool = False):
        stmt = update(Withdrawals).where(Withdrawals.id == withdrawal_id)
        stmt = stmt.values(data)
        resp = await self.session.execute(stmt, execution_options={"synchronize_session": False})
        if commit:
            await self.session.commit()
        return resp

    async def update_user_address_by_id(self, address_id: str, data: dict, commit: bool = False):
        stmt = update(UserAddress).where(UserAddress.id == address_id)
        stmt = stmt.values(data)
        resp = await self.session.execute(stmt)
        if commit:
            await self.session.commit()
        return resp

    async def upsert_balance(self, address_id: str, coin_id: str, balance: Decimal, commit: bool = False):
        stmt = postgresql.insert(Balances).values({
            Balances.address_id.key: address_id,
            Balances.coin_id.key: coin_id,
            Balances.balance.key: balance
        })
        stmt = stmt.on_conflict_do_update(index_elements=[Balances.address_id.key, Balances.coin_id.key], set_={
            Balances.balance.key: stmt.excluded.balance
        })
        try:
            await self.session.execute(stmt)
            if commit:
                await self.session.commit()
        except Exception as exc:
            await self.session.rollback()
            raise exc

    async def get_and_lock_unnotified_deposits(self, limit):
        subquery = (
            select(Deposits.id, Deposits.address_id, Users.id.label("user_id"), Deposits.contract_address,
                   Coins.name.label("coin_name"), Coins.current_rate, Coins.decimal)
            .where(and_(
                Deposits.locked_by_callback == False,  # Assuming 'locked_by_callback' is a Boolean column
                Deposits.is_notified == False,
                Deposits.time_to_callback < func.NOW()
            ))
            .join(UserAddress, UserAddress.id == Deposits.address_id)
            .join(Users, Users.id == UserAddress.user_id)
            .join(Coins, Coins.contract_address == Deposits.contract_address)
            .limit(limit)
            .with_for_update()
        )

        columns = [Deposits.id.label("deposit_id"),
                   Deposits.amount,
                   Deposits.tx_hash_in,
                   Deposits.callback_period,
                   Deposits.quote_amount,
                   subquery.c.user_id,
                   subquery.c.contract_address,
                   subquery.c.coin_name,
                   subquery.c.current_rate,
                   subquery.c.decimal
                   ]

        # Define the main query
        stmt = (
            update(Deposits)
            .values(locked_by_callback=True)
            .where(Deposits.id == subquery.c.id)
            .returning(*columns)
        )
        try:
            resp = await self.session.execute(stmt)
            data = resp.fetchall()
            await self.session.commit()
            return [array_to_dict(columns, row) for row in data]
        except Exception as exc:
            await self.session.rollback()
            raise exc

    async def get_and_lock_unnotified_withdrawals(self, limit) -> List[dict]:
        """
        :param limit:
        :return: List[dict]
        """
        subquery = (
            select(Withdrawals.id, Coins.name, Coins.current_rate, Coins.decimal)
            .join(Coins, Coins.contract_address == Withdrawals.contract_address)
            .where(and_(
                Withdrawals.tx_hash_out != None,  # Assuming 'tx_hash_out' is not nullable
                Withdrawals.locked_by_callback == False,  # Assuming 'locked_by_callback' is a Boolean column
                Withdrawals.is_notified == False,
                Withdrawals.time_to_callback < func.NOW()
            ))
            .limit(limit)
            .with_for_update()
        )

        columns = [Withdrawals.id.label("withdrawal_id"),
                   Withdrawals.amount,
                   Withdrawals.quote_amount,
                   Withdrawals.tx_hash_out,
                   Withdrawals.user_id,
                   Withdrawals.callback_period,
                   Withdrawals.user_currency,
                   Withdrawals.withdrawal_address,
                   subquery.c.name.label("coin_name"),
                   subquery.c.current_rate,
                   subquery.c.decimal
                   ]

        stmt = (
            update(Withdrawals)
            .values(locked_by_callback=True)
            .where(Withdrawals.id == subquery.c.id)
            .returning(*columns)
        )

        try:
            resp = await self.session.execute(stmt)
            data = resp.fetchall()
            await self.session.commit()
            return [array_to_dict(columns, row) for row in data]
        except Exception as exc:
            await self.session.rollback()
            raise exc

    async def get_and_lock_pending_deposits_native(self, limit=5):
        user = aliased(UserAddress)
        admin = aliased(UserAddress)

        subquery = (select(
            Deposits.id,
            user.private.label('user_private'),
            admin.public.label('admin_public'),
        )
                    .distinct(Deposits.address_id)
                    .where(and_(
            Deposits.contract_address == St.native.v,
            Deposits.tx_hash_out.is_(None),
            Deposits.locked_by_tx_handler == False,
            Deposits.time_to_tx_handler < func.NOW(),
            user.locked_by_tx == False
        )
        )
                    .join(user, user.id == Deposits.address_id)
                    .join(admin, user.admin_id == admin.user_id)
                    .limit(limit)
                    )

        columns = [
            Deposits.id.label("deposit_id"),
            Deposits.amount,
            subquery.c.user_private,
            subquery.c.admin_public,
            Deposits.tx_handler_period,
            Deposits.address_id
        ]
        stmt = (
            update(Deposits)
            .values(locked_by_tx_handler=True)
            .where(and_(
                Deposits.id == subquery.c.id
            ))
            .returning(
                *columns
            )
        )

        try:
            resp = await self.session.execute(stmt)
            data = [array_to_dict(columns, row) for row in resp.fetchall()]

            user_addresses = [row['address_id'] for row in data]
            user_addresses_to_lock_stmt = update(UserAddress).values(locked_by_tx=True).where(
                UserAddress.id.in_(user_addresses))

            await self.session.execute(user_addresses_to_lock_stmt)

            await self.session.commit()

        except Exception as exc:
            await self.session.rollback()
            raise exc
        else:
            return data

    async def get_and_lock_pending_deposits_coin(self, limit=5):
        user = aliased(UserAddress)
        approve = aliased(UserAddress)
        admin = aliased(UserAddress)

        subquery = (select(
            Deposits.id,
            Deposits.address_id,
            Deposits.contract_address,
            user.public.label('user_public'),
            user.private.label('user_private'),
            admin.public.label('admin_public'),
            approve.id.label('approve_id'),
            approve.public.label('approve_public'),
            approve.private.label('approve_private')
        )
                    .distinct(Deposits.address_id)
                    .where(and_(
            Deposits.contract_address != St.native.v,
            Deposits.tx_hash_out.is_(None),
            Deposits.locked_by_tx_handler == False,
            Deposits.time_to_tx_handler < func.NOW(),
            approve.locked_by_tx == False
        )
        )
                    .join(user, user.id == Deposits.address_id)
                    .join(approve, user.approve_id == approve.user_id)
                    .join(admin, user.admin_id == admin.user_id)
                    .limit(limit)
                    )

        columns = [
            subquery.c.contract_address,
            subquery.c.user_public,
            subquery.c.user_private,
            subquery.c.admin_public,
            subquery.c.approve_id,
            subquery.c.approve_public,
            subquery.c.approve_private,
            Deposits.id.label("deposit_id"),
            Deposits.amount,
            Deposits.tx_handler_period,
            Deposits.address_id
        ]

        stmt = (
            update(Deposits)
            .values(locked_by_tx_handler=True)
            .where(Deposits.id == subquery.c.id)
            .returning(
                *columns
            )
        )

        try:
            resp = await self.session.execute(stmt)
            data = [array_to_dict(columns, row) for row in resp.fetchall()]

            approve_addresses = [row['approve_id'] for row in data]
            approve_addr_to_lock_stmt = update(UserAddress).values(locked_by_tx=True).where(
                UserAddress.id.in_(approve_addresses))

            user_addresses = [row['address_id'] for row in data]
            user_addresses_to_lock_stmt = update(UserAddress).values(locked_by_tx=True).where(
                UserAddress.id.in_(user_addresses))

            await self.session.execute(approve_addr_to_lock_stmt)
            await self.session.execute(user_addresses_to_lock_stmt)

            await self.session.commit()

        except Exception as exc:
            await self.session.rollback()
            raise exc

        else:
            return data

    async def count_free_admins(self):
        stmt = (
            select(func.count())
            .select_from(Users)
            .join(UserAddress, UserAddress.user_id == Users.id)
            .where(and_(Users.role == St.SADMIN.v, UserAddress.locked_by_tx == False))
        )

        resp = await self.session.execute(stmt)
        return resp.scalar()

    async def get_and_lock_pending_withdrawals(self):
        count_admins = await self.count_free_admins()

        subquery = (
            select(Withdrawals.id, UserAddress.id.label("admin_addr_id"), UserAddress.private.label("admin_private"))
            .join(Balances, Balances.coin_id == Withdrawals.contract_address)
            .join(UserAddress, UserAddress.id == Balances.address_id)
            .join(Users, Users.id == UserAddress.user_id)
            .where(and_(Withdrawals.tx_hash_out.is_(None),
                        Withdrawals.admin_addr_id.is_(None),
                        Withdrawals.contract_address == Balances.coin_id,
                        Withdrawals.amount < Balances.balance,
                        Users.role == St.SADMIN.v,
                        UserAddress.locked_by_tx.is_(False)
                        )
                   ).limit(count_admins).with_for_update()
        )

        columns = [Withdrawals.contract_address,
                   Withdrawals.id.label("withdrawal_id"),
                   Withdrawals.withdrawal_address,
                   Withdrawals.amount,
                   Withdrawals.tx_handler_period,
                   subquery.c.admin_addr_id,
                   subquery.c.admin_private
                   ]

        stmt = (
            update(Withdrawals)
            .values(admin_addr_id=subquery.c.admin_addr_id)
            .where(Withdrawals.id == subquery.c.id)
            .returning(*columns)
        )

        try:
            resp = await self.session.execute(stmt)
            data = resp.fetchall()
            await self.session.commit()
            return [array_to_dict(columns, row) for row in data]
        except Exception as exc:
            await self.session.rollback()
            raise exc

    async def get_free_admin_acc(self):
        stmt = select(UserAddress.id, UserAddress.public, UserAddress.private)
        stmt = stmt.join(Users, Users.id == UserAddress.user_id)
        stmt = stmt.where(and_(Users.role == St.SADMIN.v, UserAddress.locked_by_tx == False))
        resp = await self.session.execute(stmt)
        return resp.scalar_one_or_none()

    async def upsert_coins(self, coins: List[dict]) -> None:
        """
        :param coins: [{"contract_address": "address", "name": "coin_name", "decimal": 6, "fee_amount": 5000000, "min_amount": 1000000}, ...]
        :return: None
        """
        stmt = postgresql.insert(Coins).values(coins)
        stmt = stmt.on_conflict_do_update(index_elements=[Coins.contract_address.key], set_={
            Coins.name.key: stmt.excluded.name,
            Coins.decimal.key: stmt.excluded.decimal,
            Coins.min_amount.key: stmt.excluded.min_amount,
            Coins.fee_amount.key: stmt.excluded.fee_amount
        })
        try:
            await self.session.execute(stmt)
            await self.session.commit()
        except Exception as exc:
            await self.session.rollback()
            raise exc

    async def get_coins(self, columns: List[Column], for_json=False) -> List[dict]:
        stmt = select(*columns)
        resp = await self.session.execute(stmt)
        data = resp.fetchall()
        if for_json:
            return [array_prepare_to_json(columns, row) for row in data]
        else:
            return [array_to_dict(columns, row) for row in data]

    async def update_coin(self, contract_address: str, data: dict, commit: bool = False):
        stmt = update(Coins).where(Coins.contract_address == contract_address)
        stmt = stmt.values(data)
        resp = await self.session.execute(stmt, execution_options={"synchronize_session": False})
        if commit:
            await self.session.commit()
        assert resp.rowcount == 1, f"Coin with id {contract_address} not found"

    async def get_coin(self, contract_address, columns: List[Column]):
        stmt = select(*columns).where(Coins.contract_address == contract_address)
        resp = await self.session.execute(stmt)
        data = resp.fetchone()
        return array_to_dict(columns, data)

    async def get_handled_blocks(self, limit, offset, for_json=False) -> List[dict]:
        columns = [Blocks.id, Blocks.deposit_count, Blocks.withdrawal_count]
        stmt = select(*columns)
        stmt = stmt.order_by(Blocks.id.desc()).limit(limit).offset(offset)
        resp = await self.session.execute(stmt)
        data = resp.fetchall()
        if for_json:
            return [array_prepare_to_json(columns, row) for row in data]
        else:
            return [array_to_dict(columns, row) for row in data]

    async def get_admin_balances(self) -> List[dict]:
        columns = [Balances.balance, Balances.coin_id, Balances.address_id, Users.role]
        stmt = select()
        stmt = stmt.join(UserAddress, UserAddress.id == Balances.address_id)
        stmt = stmt.join(Users, Users.id == UserAddress.user_id)
        stmt = stmt.where(Users.role.in_(St.SADMIN.v, St.APPROVE.v))
        resp = await self.session.execute(stmt)
        data = resp.fetchall()
        return [array_to_dict(columns, row) for row in data]

    async def get_pending_deposits(self, for_json=False) -> List[dict]:
        columns = [Deposits.id, Deposits.address_id, Deposits.contract_address, Deposits.amount, Deposits.tx_hash_in]
        stmt = select(*columns).where(Deposits.tx_hash_out.is_(None))
        resp = await self.session.execute(stmt)
        data = resp.fetchall()
        if for_json:
            return [array_prepare_to_json(columns, row) for row in data]
        else:
            return [array_to_dict(columns, row) for row in data]

    async def get_pending_withdrawals(self, for_json=False) -> List[dict]:
        columns = [Withdrawals.id, Withdrawals.user_id, Withdrawals.contract_address, Withdrawals.amount, Withdrawals.tx_hash_out]
        stmt = select(*columns).where(Withdrawals.tx_hash_out.is_(None))
        resp = await self.session.execute(stmt)
        data = resp.fetchall()
        if for_json:
            return [array_prepare_to_json(columns, row) for row in data]
        else:
            return [array_to_dict(columns, row) for row in data]
