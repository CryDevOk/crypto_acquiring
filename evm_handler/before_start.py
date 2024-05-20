#! /usr/bin/python3
# -*- coding: utf-8 -*-
import asyncio
import uuid
from sqlalchemy import text, exc as sqlalchemy_exc

from misc import startup_logger, SharedVariables
from config import Config as Cfg, StatCode as St
from db.database import DB, write_async_session, engine
from db.models import Base, Coins
from web3_client import utils


async def create_models():
    async with engine.begin() as conn:
        # if not engine.dialect.has_table(engine, "users"):
        # await conn.run_sync(Base.metadata.drop_all)
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"))
        await conn.run_sync(Base.metadata.create_all)


async def insert_users(mnemonic, count_users, role, offset):
    async with write_async_session() as session:
        db = DB(session, None)
        keys = utils.keys_from_mnemonic(mnemonic, count_users, offset)
        ret = []
        for i in range(count_users):
            print(St(role), keys[i][0])
            try:
                user_id = uuid.uuid4().hex
                await db.add_account(user_id, None, None, *keys[i], role)
            except sqlalchemy_exc.IntegrityError as exc:
                if "unique" in str(exc.orig):
                    pass
                else:
                    raise exc
            except Exception as exc:
                raise exc
            else:
                ret.append(user_id)
    return ret


async def upsert_coins(coins):
    async with write_async_session() as session:
        db = DB(session, None)
        await db.upsert_coins(coins)


def parse_coins(coins):
    # USDC|6|1000000|3000000|0x0f1a713859fB1d1afAc99Fe2D20CAf639560EC83,...
    ret_data = []
    coins = coins.split(",")
    addresses = set()
    for coin in coins:
        coin = coin.split("|")
        name = coin[0]
        decimal = int(coin[1])
        min_amount = int(coin[2])
        fee_amount = int(coin[3])
        contract_address = coin[4]
        assert contract_address, f"Empty address"
        assert contract_address not in addresses, f"Coin {contract_address} already exists"
        assert decimal > 0, f"Wrong decimal: {decimal}"
        assert min_amount > 0, f"Wrong min_amount: {min_amount}"
        addresses.add(contract_address)

        ret_data.append({Coins.contract_address.key: contract_address,
                         Coins.name.key: name,
                         Coins.decimal.key: decimal,
                         Coins.min_amount.key: min_amount,
                         Coins.fee_amount.key: fee_amount
                         })
    return ret_data


async def startup():
    try:
        await create_models()
        await insert_users(Cfg.ADMIN_SEED, Cfg.admin_accounts, St.SADMIN.v, 0)
        await insert_users(Cfg.ADMIN_SEED, Cfg.approve_accounts, St.APPROVE.v, Cfg.admin_accounts)
        coins = parse_coins(Cfg.config_coins)
        await upsert_coins(coins)
    except Exception as exc:
        print(f"Error: {exc}")
        startup_logger.error(f"Error: {exc}")
        exit(1)
    else:
        print(f"First run successfully")
        startup_logger.info(f"First run successfully")
        exit(0)


if __name__ == "__main__":
    variables = SharedVariables()
    asyncio.run(startup())
