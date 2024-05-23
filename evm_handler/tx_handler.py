# -*- coding: utf-8 -*-
# Description: This module contains the main logic of the ERC20 parser and the native coin.

from misc import get_logger, SharedVariables, amount_to_quote_amount, \
    get_round_for_rate, amount_to_display
import asyncio
import logging
from typing import List, Tuple, Dict, Any
from decimal import Decimal
import eth_utils
import eth_abi
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta, timezone

from web3_client import async_client, utils as web3_utils
from db.database import DB, write_async_session, read_async_session, UserAddress, Withdrawals
from db.models import Deposits, Coins
from config import Config as Cfg, StatCode as St
import api


async def coin_transfer_to_admin(conn_creds,
                                 contract_address,
                                 user_public,
                                 user_private,
                                 admin_public,
                                 approve_id,
                                 approve_public,
                                 approve_private,
                                 deposit_id,
                                 amount,
                                 tx_handler_period) -> tuple[None, Exception, tuple[Any, Any, Any], Any] | tuple[
    Any, None, tuple[Any, Any, Any], Any]:
    amount = int(amount)
    async with async_client.AsyncEth(*conn_creds) as client:
        contract = async_client.ERC20(client, contract_address, abi_info=web3_utils.erc20_abi)
        try:
            allowance = await contract.allowance(user_public, approve_public)
        except Exception as exc:
            return None, exc, (deposit_id, tx_handler_period, approve_id), conn_creds
        else:
            if allowance < amount:
                try:
                    await variables.gas_price_event.wait()
                    await client.send_ether(user_public,
                                            int(100000 * variables.gas_price * 1.3),
                                            approve_private,
                                            gas_price=variables.gas_price,
                                            gas=21000)

                except Exception as exc:
                    return None, exc, (deposit_id, tx_handler_period, approve_id), conn_creds
                else:
                    try:
                        await variables.gas_price_event.wait()
                        await contract.approve(approve_public, 9_999_999_999_999_999, user_private,
                                               gas_price=variables.gas_price)
                    except Exception as exc:
                        return None, exc, (deposit_id, tx_handler_period, approve_id), conn_creds
                    else:
                        try:
                            await variables.gas_price_event.wait()
                            res = await contract.transfer_from(user_public,
                                                               admin_public,
                                                               amount,
                                                               approve_private,
                                                               gas_price=variables.gas_price)
                        except Exception as exc:
                            return None, exc, (deposit_id, tx_handler_period, approve_id), conn_creds
                        else:
                            return res, None, (deposit_id, tx_handler_period, approve_id), conn_creds
            else:
                try:
                    await variables.gas_price_event.wait()
                    res = await contract.transfer_from(user_public,
                                                       admin_public,
                                                       amount,
                                                       approve_private,
                                                       gas_price=variables.gas_price)
                except Exception as exc:
                    return None, exc, (deposit_id, tx_handler_period, approve_id), conn_creds
                else:
                    return res, None, (deposit_id, tx_handler_period, approve_id), conn_creds


async def withdraw_coin(conn_creds,
                        contract_address,
                        withdrawal_address,
                        amount,
                        admin_private,
                        withdrawal_id,
                        tx_handler_period,
                        admin_addr_id,
                        **_
                        ):
    amount = int(amount)
    try:
        async with async_client.AsyncEth(*conn_creds) as client:
            contract = async_client.ERC20(client, contract_address, abi_info=web3_utils.erc20_abi)
            await variables.gas_price_event.wait()
            res = await contract.transfer(withdrawal_address, amount, admin_private, gas_price=variables.gas_price)
    except Exception as exc:
        return None, exc, (withdrawal_id, tx_handler_period, admin_addr_id), conn_creds
    else:
        return res, None, (withdrawal_id, tx_handler_period, admin_addr_id), conn_creds


async def withdraw_native(conn_creds,
                          withdrawal_address,
                          amount,
                          admin_private,
                          withdrawal_id,
                          tx_handler_period,
                          admin_addr_id,
                          **_
                          ):
    amount = int(amount)
    try:
        async with async_client.AsyncEth(*conn_creds) as client:
            await variables.gas_price_event.wait()
            res = await client.send_ether(withdrawal_address,
                                          amount,
                                          admin_private,
                                          gas_price=variables.gas_price,
                                          gas=21000)
    except Exception as exc:
        return None, exc, (withdrawal_id, tx_handler_period, admin_addr_id), conn_creds
    else:
        return res, None, (withdrawal_id, tx_handler_period, admin_addr_id), conn_creds


async def native_balance(conn_creds, addr_id, address):
    try:
        async with async_client.AsyncEth(*conn_creds) as client:
            res: int = await client.get_account_balance(address)
    except Exception as exc:
        return None, exc, (addr_id, address, conn_creds)
    else:
        return res, None, (addr_id, address, conn_creds)


async def trc20_balance(conn_creds, addr_id, contract_address, address: str):
    try:
        async with async_client.AsyncEth(*conn_creds) as client:
            contract = async_client.ERC20(client, contract_address, abi_info=web3_utils.erc20_abi)
            res = await contract.balance_of(address)
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
                reqs.append(asyncio.create_task(native_balance(conn_creds, addr_id, address)))

            results = await asyncio.gather(*reqs)

            for balance, err, req_ident in results:
                addr_id, address, conn_creds = req_ident
                await variables.api_keys_pool.put(conn_creds)
                if not err:
                    if balance <= (variables.gas_price * 100000) * Cfg.native_warning_threshold:
                        logger.warning(
                            f"{addr_id} {address}: has balance {amount_to_display(balance, 18, Decimal('0.00001'))} "
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


async def update_gas_price(logger: logging.Logger):
    conn_creds: List[Tuple[str, str]] = await variables.api_keys_pool.get()
    try:
        async with async_client.AsyncEth(*conn_creds) as client:
            gas_price = int((await client.gas_price()) * 1.5)
    except Exception as exc:
        logger.error(exc)
    else:
        variables.gas_price_event.clear()
        variables.gas_price = gas_price
    finally:
        variables.gas_price_event.set()
        await variables.api_keys_pool.put(conn_creds)


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
                variables.user_accounts_low_case[user_address.lower()] = address_id
            else:
                variables.handler_accounts[user_address] = address_id
                variables.handler_accounts_low_case[user_address.lower()] = address_id
    finally:
        variables.user_accounts_event.set()


async def coins_txs_parser(transactions: List[dict],
                           coins: Dict[str, dict],
                           logger: logging.Logger):
    """
    This function parses the ERC20 events and creates the deposit records.
    :param transactions: List[dict]
    :param coins: key - contract_address string low, value - dict with keys: name, min_amount
    :param logger:
    :return:
    """
    deposits = []
    for unit in transactions:
        if unit.get("address") in coins and not unit.get("removed"):
            address_received: str = eth_abi.decode(["address"], eth_utils.decode_hex(unit["topics"][2]))[0]  # low case
            await variables.user_accounts_event.wait()
            address_id = variables.user_accounts_low_case.get(address_received)
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
                        Deposits.contract_address.key: coin[Coins.contract_address.key],
                        Deposits.tx_hash_in.key: unit['transactionHash']
                    })
                else:
                    logger.info(f"deposit less then minimum amount {unit['transactionHash']}")
    return deposits


async def native_txs_parser(block: Dict, native_coin: dict, client: async_client.AsyncEth, logger: logging.Logger):
    deposits = []
    for unit in block.get("transactions", []):
        if unit["input"] == "0x":
            sender = unit["from"]
            recipient = unit["to"]
            amount = int(unit["value"], 16)
            await variables.user_accounts_event.wait()

            if recipient in variables.user_accounts_low_case and sender not in variables.handler_accounts_low_case:

                receipt = await client.get_transaction_receipt(unit["hash"])

                if receipt["status"] == "0x1":
                    if amount >= native_coin[Coins.min_amount.key]:
                        address_id = variables.user_accounts_low_case[recipient]
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

                data = {"fromBlock": eth_utils.to_hex(current_block),
                        "toBlock": eth_utils.to_hex(current_block),
                        "topics": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]}

                tasks = [asyncio.create_task(client1.get_logs(data)),
                         asyncio.create_task(client2.get_block_by_number(eth_utils.to_hex(current_block)))]

                try:
                    transactions, block = await asyncio.gather(*tasks)
                except Exception as exc:
                    logger.error(exc)
                else:
                    async with write_async_session() as session:
                        db = DB(session, logger)
                        resp = await db.get_coins(
                            [Coins.contract_address, Coins.name, Coins.current_rate, Coins.min_amount, Coins.decimal])

                        coins = {coin[Coins.contract_address.key].lower(): coin for coin in resp if
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


async def tx_conductor_native(logger: logging.Logger):
    reqs = []
    async with write_async_session() as session:
        db = DB(session, logger)
        deposits = await db.get_and_lock_pending_deposits_native(7)

        if deposits:
            for deposit in deposits:
                conn_creds: List[Tuple[str, str]] = await variables.api_keys_pool.get()
                reqs.append(asyncio.create_task(native_transfer_to_admin(conn_creds=conn_creds, **deposit)))

            results = await asyncio.gather(*reqs)

            for tx_hash, err, req_ident, conn_creds in results:
                await variables.api_keys_pool.put(conn_creds)
                deposit_id, tx_handler_period = req_ident
                if not err:
                    await db.update_deposit_by_id(deposit_id, {Deposits.tx_hash_out.key: tx_hash,
                                                               Deposits.locked_by_tx_handler.key: False}, commit=True)
                elif tx_hash:
                    logger.critical(f"{err} deposit_id: {deposit_id}, {tx_hash}")
                    await db.update_deposit_by_id(deposit_id, {Deposits.tx_hash_out.key: tx_hash}, commit=True)
                else:
                    logger.error(f"{err} deposit_id: {deposit_id}")
                    time_to_tx_handler = datetime.now(timezone.utc) + timedelta(seconds=tx_handler_period)
                    tx_handler_period += 30
                    await db.update_deposit_by_id(deposit_id, {Deposits.locked_by_tx_handler.key: False,
                                                               Deposits.time_to_tx_handler.key: time_to_tx_handler,
                                                               Deposits.tx_handler_period.key: tx_handler_period},
                                                  commit=True)


async def native_transfer_to_admin(conn_creds,
                                   deposit_id,
                                   amount,
                                   user_private,
                                   admin_public,
                                   tx_handler_period):
    amount = int(amount)
    try:
        async with async_client.AsyncEth(*conn_creds) as client:
            await variables.gas_price_event.wait()
            amount_with_fee: int = int(amount - variables.gas_price * 21000)
            res = await client.send_ether(admin_public, amount_with_fee, user_private, gas_price=variables.gas_price,
                                          gas=21000)
    except Exception as exc:
        return None, exc, (deposit_id, tx_handler_period), conn_creds
    else:
        return res, None, (deposit_id, tx_handler_period), conn_creds


async def tx_conductor_coin(logger: logging.Logger):
    reqs = []
    async with write_async_session() as session:
        db = DB(session, logger)
        deposits = await db.get_and_lock_pending_deposits_coin(5)

        if deposits:
            for deposit in deposits:
                conn_creds: List[Tuple[str, str]] = await variables.api_keys_pool.get()
                reqs.append(asyncio.create_task(coin_transfer_to_admin(conn_creds=conn_creds, **deposit)))

            results = await asyncio.gather(*reqs)

            for tx_hash, err, req_ident, conn_creds in results:
                await variables.api_keys_pool.put(conn_creds)
                deposit_id, tx_handler_period, approve_id = req_ident
                if not err:
                    await db.update_deposit_by_id(deposit_id, {Deposits.tx_hash_out.key: tx_hash,
                                                               Deposits.locked_by_tx_handler.key: False}, commit=True)
                elif tx_hash and err:
                    logger.critical(f"{err} deposit_id: {deposit_id}, {tx_hash}")
                    await db.update_deposit_by_id(deposit_id, {Deposits.tx_hash_out.key: tx_hash})
                    await db.update_user_address_by_id(approve_id, {UserAddress.locked_by_tx.key: False},
                                                       commit=True)
                else:
                    logger.error(f"{err} deposit_id: {deposit_id}")
                    time_to_tx_handler = datetime.now(timezone.utc) + timedelta(seconds=tx_handler_period)
                    tx_handler_period += 30
                    await db.update_deposit_by_id(deposit_id, {Deposits.locked_by_tx_handler.key: False,
                                                               Deposits.time_to_tx_handler.key: time_to_tx_handler,
                                                               Deposits.tx_handler_period.key: tx_handler_period},
                                                  commit=True)


async def withdraw_handler(logger: logging.Logger):
    reqs = []
    async with write_async_session() as session:
        db = DB(session, logger)
        withdrawals = await db.get_and_lock_pending_withdrawals(Cfg.admin_accounts)

        if withdrawals:
            for withdrawal in withdrawals:
                contract_address = withdrawal[Withdrawals.contract_address.key]
                conn_creds: List[Tuple[str, str]] = await variables.api_keys_pool.get()
                if contract_address == St.native.v:
                    reqs.append(asyncio.create_task(withdraw_native(conn_creds, **withdrawal)))
                else:
                    reqs.append(asyncio.create_task(withdraw_coin(conn_creds, **withdrawal)))

            results = await asyncio.gather(*reqs)

            for tx_hash, err, req_ident, conn_creds in results:
                await variables.api_keys_pool.put(conn_creds)
                withdrawal_id, tx_handler_period, adm_address_id = req_ident
                if not err:
                    await db.update_withdrawal_by_id(withdrawal_id, {Withdrawals.tx_hash_out.key: tx_hash}, commit=True)
                elif tx_hash:
                    logger.critical(f"{err} withdrawal_id: {withdrawal_id}")
                    await db.update_withdrawal_by_id(withdrawal_id, {Withdrawals.tx_hash_out.key: tx_hash})
                    await db.update_user_address_by_id(adm_address_id, {UserAddress.locked_by_tx.key: False},
                                                       commit=True)
                else:
                    time_to_tx_handler = datetime.now(timezone.utc) + timedelta(tx_handler_period)
                    tx_handler_period += 15
                    await db.update_withdrawal_by_id(
                        withdrawal_id, {Withdrawals.admin_addr_id.key: None,
                                        Withdrawals.time_to_tx_handler.key: time_to_tx_handler,
                                        Withdrawals.tx_handler_period.key: tx_handler_period}, commit=True)
                    logger.error(f"{err} withdrawal_id: {withdrawal_id}")


async def main():
    startup_logger = get_logger("startup_logger")
    scheduler = AsyncIOScheduler()
    scheduler._logger.setLevel(logging.ERROR)  # to avoid apscheduler noise warning logs

    reserved_conn_creds1 = await variables.api_keys_pool.get()
    reserved_conn_creds2 = await variables.api_keys_pool.get()
    try:
        await update_in_memory_last_handled_block(startup_logger)
        await update_in_memory_accounts(startup_logger)
        await update_coin_rates(startup_logger)
        await update_gas_price(startup_logger)
    except Exception as exc:
        startup_logger.error(f"launch failed {exc}")
        raise Exception(f"launch failed {exc}")
    else:
        startup_logger.info("launch success")
        scheduler.add_job(update_gas_price, "interval", seconds=60,
                          args=(get_logger("update_gas_price"),))
        scheduler.add_job(update_coin_rates, "interval", seconds=10,
                          args=(get_logger("update_coin_rates"),))
        scheduler.add_job(update_in_memory_accounts, "interval", seconds=10,
                          args=(get_logger("update_in_memory_accounts"),))
        scheduler.add_job(admin_coins_bal, "interval", seconds=30,
                          args=(get_logger("admin_coins_bal"),))
        scheduler.add_job(admin_approve_native_bal, "interval", seconds=30,
                          args=(get_logger("admin_approve_native_bal"),))
        scheduler.add_job(block_parser, "interval", seconds=3, max_instances=1,
                          args=(reserved_conn_creds1, reserved_conn_creds2, get_logger("block_parser")))
        scheduler.add_job(tx_conductor_coin, "interval", seconds=1, max_instances=1,
                          args=(get_logger("tx_conductor_coin"),))
        scheduler.add_job(tx_conductor_native, "interval", seconds=1, max_instances=1,
                          args=(get_logger("tx_conductor_native"),))
        scheduler.add_job(withdraw_handler, "interval", seconds=1, max_instances=1,
                          args=(get_logger("withdraw_handler"),))

        scheduler.start()
        while True:
            await asyncio.sleep(1000)


if __name__ == '__main__':
    variables = SharedVariables()
    asyncio.run(main())
