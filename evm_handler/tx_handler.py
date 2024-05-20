# -*- coding: utf-8 -*-
# Description: This module contains the main logic of the ERC20 parser and the native coin.

from misc import get_logger, MyScheduler, SharedVariables, amount_to_quote_amount, \
    get_round_for_rate, amount_to_display
import asyncio
import logging
from typing import List, Tuple, Dict
from decimal import Decimal
import eth_utils
import eth_abi

from web3_client import async_client
from db.database import DB, write_async_session, read_async_session
from db.models import Deposits, Coins
from config import Config as Cfg, StatCode as St
import api


async def trx_balance(conn_creds, addr_id, address):
    try:
        async with async_client.AsyncEth(*conn_creds) as client:
            res: int = await client.get_account_balance(address)
    except Exception as exc:
        return None, exc, (addr_id, conn_creds)
    else:
        return res, None, (addr_id, conn_creds)


async def trc20_balance(conn_creds, addr_id, contract_address, address: str):
    try:
        async with async_client.AsyncEth(*conn_creds) as client:
            contract = async_client.ERC20(client, contract_address, abi_info=Cfg.erc20_abi)
            res = await contract.balanceOf(address)
    except Exception as exc:
        return None, exc, (addr_id, contract_address, conn_creds)
    else:
        return res, None, (addr_id, contract_address, conn_creds)


async def admin_approve_native_bal(logger):
    reqs = []
    async with write_async_session() as session:
        db = DB(session, logger)
        users: List[Tuple[str, str]] = await db.users_addresses([St.SADMIN.v, St.APPROVE.v])

        if users:
            for addr_id, address in users:
                conn_creds: List[Tuple[str, str]] = await variables.api_keys_pool.get()
                reqs.append(asyncio.create_task(trx_balance(conn_creds, addr_id, address)))

            results = await asyncio.gather(*reqs)

            for balance, err, req_ident in results:
                addr_id, conn_creds = req_ident
                await variables.api_keys_pool.put(conn_creds)
                if not err:
                    if balance <= variables.estimated_native_fee * Cfg.native_warning_threshold:
                        logger.warning(f"{addr_id}: has balance {amount_to_display(balance, 6, Decimal('0.01'))} "
                                       f"and can handle less then {Cfg.native_warning_threshold} transactions")
                    await db.upsert_balance(addr_id, St.native.v, balance, commit=True)
                else:
                    logger.error(f"{addr_id}: {err}")


async def admin_coins_bal(logger):
    reqs = []
    async with write_async_session() as session:
        db = DB(session, logger)
        users: List[Tuple[str, str]] = await db.users_addresses([St.SADMIN.v])

        if users:
            coins = await db.get_coins([Coins.contract_address, Coins.name])
            for addr_id, address in users:
                for coin in coins:
                    contract_address: str = coin[Coins.contract_address.key]
                    if contract_address != St.native.v:
                        conn_creds: List[Tuple[str, str]] = await variables.api_keys_pool.get()
                        reqs.append(asyncio.create_task(trc20_balance(conn_creds,
                                                                      addr_id,
                                                                      contract_address,
                                                                      address)))

            results = await asyncio.gather(*reqs)

            for balance, err, req_ident in results:
                addr_id, contract_address, conn_creds = req_ident
                await variables.api_keys_pool.put(conn_creds)
                if not err:
                    await db.upsert_balance(addr_id, contract_address, balance, commit=True)
                else:
                    logger.error(f"{addr_id}: {err}")


async def update_in_memory_last_handled_block(logger: logging.Logger):
    try:
        async with read_async_session() as session:
            db = DB(session, logger)
            block = await db.get_last_handled_block()
            if not block:
                if Cfg.start_block == "latest":
                    conn_creds: List[Tuple[str, str]] = await variables.api_keys_pool.get()
                    async with async_client.AsyncEth(*conn_creds) as client:
                        block = await client.latest_block_number() - Cfg.block_offset
                        await db.insert_last_handled_block(block, commit=True)
                    await variables.api_keys_pool.put(conn_creds)
                else:
                    block = Cfg.start_block
                    await db.insert_last_handled_block(Cfg.start_block, commit=True)
    except Exception as exc:
        logger.error(exc)
        raise
    else:
        print(f"last handled block: {block}")
        variables.last_handled_block = block


async def update_coin_rates(logger: logging.Logger):
    rates, exceptions = await api.coin_rate_client.get_coin_rates()
    if exceptions:
        for exc in exceptions:
            logger.error(exc)

    async with write_async_session() as session:
        db = DB(session, logger)
        coins = await db.get_coins([Coins.contract_address, Coins.name, Coins.current_rate])

        for coin in coins:
            if coin[Coins.name.key] != Cfg.quote_coin:
                symbol = f"{coin[Coins.name.key]}{Cfg.quote_coin}"
                if rates.get(symbol):
                    await db.update_coin(coin[Coins.contract_address.key], {Coins.current_rate.key: rates[symbol]},
                                         commit=True)
                else:
                    logger.error(f"No rate for {symbol}")
            else:
                if coin[Coins.current_rate.key] != 1:
                    await db.update_coin(coin[Coins.contract_address.key], {Coins.current_rate.key: 1}, commit=True)


async def update_in_memory_accounts(logger: logging.Logger):
    """
    This function updates the user_accounts dictionary in the variables module.
    :param logger: logging.Logger
    :return:
    """
    try:
        async with write_async_session() as session:
            db = DB(session, logger)
            users = await db.all_accounts()
    except Exception as exc:
        logger.error(exc)
        raise
    else:
        variables.user_accounts_event.clear()
        for address_id, user_address, role in users:
            if role == St.USER.v:
                variables.user_accounts[user_address] = address_id
            else:
                variables.handler_accounts[user_address] = address_id
    finally:
        variables.user_accounts_event.set()


async def coins_txs_parser(transactions: List[dict],
                           coins: Dict[str, dict],
                           logger: logging.Logger):
    """
    This function parses the ERC20 events and creates the deposit records.
    :param transactions: List[dict]
    :param coins: key - contract_address in hex format, value - dict with keys: name, min_amount
    :param logger:
    :return:
    """

    deposits = []
    for unit in transactions:
        if unit.get("address") in coins and not unit.get("removed"):
            address_received: str = eth_abi.decode(["address"], eth_utils.decode_hex(unit["topics"][2]))[0]
            await variables.user_accounts_event.wait()
            address_id = variables.user_accounts.get(address_received)
            if address_id:
                amount: int = eth_abi.decode(["uint256"], eth_utils.decode_hex(unit["data"]))[0]
                coin = coins[unit["address"]]
                if amount >= coin[Coins.min_amount.key]:
                    quote_amount: Decimal = amount_to_quote_amount(amount,
                                                                   coin[Coins.current_rate.key],
                                                                   coin[Coins.decimal.key])
                    deposits.append({
                        Deposits.address_id.key: address_id,
                        Deposits.amount.key: amount,
                        Deposits.quote_amount.key: quote_amount,
                        Deposits.contract_address.key: unit["address"],
                        Deposits.tx_hash_in.key: unit['id']
                    })
                else:
                    logger.info(f"deposit less then minimum amount {unit['id']}")
    return deposits


async def native_txs_parser(block: Dict, native_coin: dict, client: async_client.AsyncEth, logger: logging.Logger):
    deposits = []
    for unit in block.get("transactions", []):
        if unit["input"] == "0x":
            sender = unit["from"]
            recipient = unit["to"]
            amount = int(unit["value"], 16)
            await variables.user_accounts_event.wait()
            if recipient in variables.user_accounts and sender not in variables.handler_accounts:
                receipt = await client.get_transaction_receipt(unit["hash"])

                if receipt["status"] == "0x1":
                    if amount >= native_coin[Coins.min_amount.key]:
                        address_id = variables.user_accounts[recipient]
                        quote_amount: Decimal = amount_to_quote_amount(amount,
                                                                       native_coin[Coins.current_rate.key],
                                                                       native_coin[Coins.decimal.key])
                        deposits.append({
                            Deposits.address_id.key: address_id,
                            Deposits.amount.key: amount,
                            Deposits.contract_address.key: St.native.v,
                            Deposits.tx_hash_in.key: unit['hash'],
                            Deposits.quote_amount.key: quote_amount
                        })
                    else:
                        logger.info(f"deposit less then minimum amount {unit['hash']}")
                await asyncio.sleep(0.5)  # to avoid rate limit, TODO: upgrade rps plan
    return deposits


async def block_parser(conn_creds_1, conn_creds_2, logger: logging.Logger):
    async with async_client.AsyncEth(*conn_creds_1) as client1, async_client.AsyncEth(*conn_creds_2) as client2:
        try:
            latest_trust_block = await client1.latest_block_number() - Cfg.block_offset
        except Exception as exc:
            logger.error(exc)
            return
        else:
            current_block = variables.last_handled_block + 1

            if latest_trust_block > current_block:
                if latest_trust_block - current_block > Cfg.block_offset * Cfg.allowed_slippage:
                    logger.warning(
                        f"Slippage for the block pasring more then {Cfg.block_offset} in {Cfg.allowed_slippage} times")

                tasks = [asyncio.create_task(client1.get_logs(current_block)),
                         asyncio.create_task(client2.get_block_by_number(current_block))]

                try:
                    transactions, block = await asyncio.gather(*tasks)
                except Exception as exc:
                    logger.error(exc)
                else:
                    async with write_async_session() as session:
                        db = DB(session, logger)
                        resp = await db.get_coins(
                            [Coins.contract_address, Coins.name, Coins.current_rate, Coins.min_amount, Coins.decimal])

                        coins = {coin[Coins.contract_address.key]: coin for coin in resp if
                                 coin[Coins.contract_address.key] != St.native.v}

                        coin_txs = await coins_txs_parser(transactions, coins, logger)

                        native_coin = [coin for coin in resp if coin[Coins.contract_address.key] == St.native.v][0]
                        native_txs = await native_txs_parser(block, native_coin, client1, logger)

                        deposits = coin_txs + native_txs

                        if deposits:
                            await db.add_deposits(deposits)
                        await db.insert_last_handled_block(current_block, commit=True)
                        variables.last_handled_block = current_block
                        logger.info(f"handled block number {current_block}")


async def main():
    startup_logger = get_logger("startup_logger")
    scheduler = MyScheduler()
    reserved_conn_creds1 = await variables.api_keys_pool.get()
    reserved_conn_creds2 = await variables.api_keys_pool.get()
    try:
        await update_in_memory_last_handled_block(startup_logger)
        await update_in_memory_accounts(startup_logger)
        await update_coin_rates(startup_logger)
    except Exception as exc:
        startup_logger.error(f"launch failed {exc}")
        raise Exception(f"launch failed {exc}")
    else:
        startup_logger.info("launch success")

        scheduler.every(10).seconds.do(update_coin_rates, get_logger("update_coin_rates"))
        scheduler.every(10).seconds.do(update_in_memory_accounts, get_logger("update_in_memory_accounts"))
        scheduler.every(30).seconds.do(admin_coins_bal, get_logger("admin_coins_bal"))
        scheduler.every(30).seconds.do(admin_approve_native_bal, get_logger("admin_approve_native_bal"))
        scheduler.every(1).seconds.do(block_parser, reserved_conn_creds1, reserved_conn_creds2, get_logger("block_parser"))

        while True:
            await scheduler.run_pending()
            await asyncio.sleep(1)


if __name__ == '__main__':
    variables = SharedVariables()
    asyncio.run(main())
