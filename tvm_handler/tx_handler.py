# -*- coding: utf-8 -*-
# Description: This module contains the main logic of the TRC20 parser and the native coin.
from misc import get_logger, SharedVariables, amount_to_quote_amount, \
    get_round_for_rate, amount_to_display, proc_api_client
from web3_client import utils as web3_utils
from web3_client.async_client import MyAsyncTron, TRC20, BuildTransactionError, TransactionNotFound, TvmError, \
    UnableToGetReceiptError, ApiError, BadSignature, TaposError, TransactionError, ValidationError
from db.database import DB, write_async_session, read_async_session, Withdrawals
from db.models import Deposits, Coins
from config import Config as Cfg, StatCode as St
import api

import asyncio
import logging
from decimal import Decimal
import eth_utils
from tronpy.abi import trx_abi
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.job import Job
from datetime import datetime, timedelta, timezone
import httpx


class PreparingTransactionError(Exception):
    def __init__(self, original_error, message):
        self.original_error = original_error
        self.message = message

    def __str__(self):
        return f"Build transaction error: {self.message} {self.original_error}"


async def explorer(interval: int, block_parser_job: Job):
    timeframe = timedelta(seconds=interval)
    rps = await variables.providers_request_explorer.rps(timeframe)
    description = await variables.providers_request_explorer.description_by_status_code(timeframe)
    # share_unsuccessful_requests = await variables.providers_request_explorer.share_unsuccessful_requests(timeframe)
    providers_api_logger.info(
        f"{description}"
        f"\nRequests per second for {interval} seconds: {rps}"
        f"\nBlock parser interval: {variables.block_parser_interval}")

    block_parser_job.reschedule(
        trigger='interval',
        seconds=variables.block_parser_interval
    )

    slippage = variables.trusted_block - variables.last_handled_block > Cfg.block_offset * Cfg.allowed_slippage
    if slippage:
        variables.block_parser_interval = 0
        try:
            last_trusted_block = await get_trusted_block()
        except Exception as exc:
            common_logger.error(f"Unable to get trusted block {exc}")
        else:
            common_logger.warning(
                f"Last trusted block is {last_trusted_block}\n"
                f"Last handled block is {variables.last_handled_block}\n"
                f"Slippage is {last_trusted_block - variables.last_handled_block} blocks")

    else:
        variables.block_parser_interval = 2
        common_logger.info(f"Last handled block: {variables.last_handled_block}")


async def coin_transfer_to_admin(
        conn_creds,
        contract_address,
        user_public,
        user_private,
        admin_public,
        approve_id,
        approve_public,
        approve_private,
        deposit_id,
        amount,
        tx_handler_period) -> tuple[None, Exception, tuple[type, type, type, type]] | \
                              tuple[type, None, tuple[type, type, type, type]]:
    amount = int(amount)
    try:
        async with MyAsyncTron(*conn_creds) as client:
            contract = TRC20(client, contract_address, abi_info=web3_utils.trc20_abi)
            try:
                allowance: int = await contract.allowance(user_public, approve_public)
                if allowance < amount:
                    balance = await contract._client.get_account_balance(user_public)
                    if balance < variables.estimated_trc20_fee:
                        await client.trx_transfer(user_public, variables.estimated_trc20_fee, approve_private)
                    await contract.approve(approve_public, 9_999_999_999_999_999, user_private)
            except Exception as exc:
                raise PreparingTransactionError(exc, "Unable to prepare transaction")
            else:
                res = await contract.transfer_from(user_public, admin_public, amount, approve_private)
    except Exception as exc:
        return None, exc, (conn_creds, deposit_id, tx_handler_period, approve_id)
    else:
        return res, None, (conn_creds, deposit_id, tx_handler_period, approve_id)


async def withdraw_coin(conn_creds,
                        contract_address,
                        withdrawal_address,
                        amount,
                        admin_private,
                        withdrawal_id,
                        tx_handler_period,
                        admin_addr_id,
                        **_
                        ) -> tuple[None, Exception, tuple[type, type, type, type]] | \
                             tuple[type, None, tuple[type, type, type, type]]:
    amount = int(amount)
    try:
        async with MyAsyncTron(*conn_creds) as client:
            contract = TRC20(client, contract_address, abi_info=web3_utils.trc20_abi)
            res = await contract.transfer(withdrawal_address, amount, admin_private)
    except Exception as exc:
        return None, exc, (conn_creds, withdrawal_id, tx_handler_period, admin_addr_id)
    else:
        return res, None, (conn_creds, withdrawal_id, tx_handler_period, admin_addr_id)


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
        async with MyAsyncTron(*conn_creds) as client:
            res = await client.trx_transfer(withdrawal_address, amount, admin_private)
    except Exception as exc:
        return None, exc, (conn_creds, withdrawal_id, tx_handler_period, admin_addr_id)
    else:
        return res, None, (conn_creds, withdrawal_id, tx_handler_period, admin_addr_id)


async def notify_deposit(display_amount: str,
                         deposit_id,
                         callback_period: int,
                         quote_amount: Decimal,
                         tx_hash_in: str,
                         user_id,
                         coin_name,
                         **_) -> tuple[Exception, tuple[type, int, type]] | tuple[None, tuple[type, int, type]]:
    callback_id = f"deposit_{deposit_id}"
    path = "/v1/api/private/user/deposit"
    json_data = {"user_id": user_id,
                 "coin_name": coin_name,
                 "display_amount": display_amount,
                 "quote_amount": str(quote_amount),
                 "description": f"deposit {deposit_id}",
                 "tx_scaner_url": Cfg.scanner_url + tx_hash_in}
    try:
        await proc_api_client.add_callback(callback_id, user_id, path, json_data)
    except Exception as exc:
        return exc, (deposit_id, callback_period, user_id)
    else:
        return None, (deposit_id, callback_period, user_id)


async def notify_withdrawal(display_amount: str,
                            quote_amount: Decimal,
                            withdrawal_id,
                            tx_hash_out: str,
                            user_id,
                            callback_period,
                            coin_name: str,
                            user_currency: str,
                            current_rate: Decimal,
                            withdrawal_address: str,
                            **_) -> tuple[None, Exception, tuple[type, type]] | \
                                    tuple[type, None, tuple[type, type]]:
    callback_id = f"withdrawal_{withdrawal_id}"
    path = "/v1/api/private/user/notify_withdrawal"
    json_data = {"user_id": user_id,
                 "coin_name": coin_name,
                 "display_amount": display_amount,
                 "quote_amount": str(quote_amount),
                 "user_currency": user_currency,
                 "tx_scaner_url": Cfg.scanner_url + tx_hash_out,
                 "withdrawal_address": withdrawal_address,
                 "network_display": Cfg.PROC_HANDLER_DISPLAY,
                 "rate_display": current_rate}

    try:
        resp = await proc_api_client.add_callback(callback_id, user_id, path, json_data)
    except Exception as exc:
        return None, exc, (withdrawal_id, callback_period)
    else:
        return resp, None, (withdrawal_id, callback_period)


async def native_balance(conn_creds, addr_id, address) -> tuple[int, None, tuple[type, type, type]] | \
                                                          tuple[None, Exception, tuple[type, type, type]]:
    try:
        async with MyAsyncTron(*conn_creds) as client:
            res: int = await client.get_account_balance(address)
    except Exception as exc:
        return None, exc, (conn_creds, addr_id, address)
    else:
        return res, None, (conn_creds, addr_id, address)


async def trc20_balance(conn_creds, addr_id, contract_address, address: str) \
        -> tuple[int, None, tuple[type, type, type]] | \
           tuple[None, Exception, tuple[type, type, type]]:
    try:
        async with MyAsyncTron(*conn_creds) as client:
            contract = TRC20(client, contract_address, abi_info=web3_utils.trc20_abi)
            res: int = await contract.balance_of(address)
    except Exception as exc:
        return None, exc, (conn_creds, addr_id, contract_address)
    else:
        return res, None, (conn_creds, addr_id, contract_address)


async def admin_approve_native_bal():
    reqs = []
    async with write_async_session() as session:
        db = DB(session)
        users: list[tuple[str, str]] = await db.users_addresses([St.SADMIN.v, St.APPROVE.v])

        if users:
            for addr_id, address in users:
                conn_creds: list[tuple[str, str]] = await variables.api_keys_pool.get()
                reqs.append(asyncio.create_task(native_balance(conn_creds, addr_id, address)))

            results = await asyncio.gather(*reqs)

            for balance, err, req_ident in results:
                conn_creds, addr_id, address = req_ident
                await variables.api_keys_pool.put(conn_creds)
                if not err:
                    if balance <= variables.estimated_trc20_fee * Cfg.native_error_threshold:
                        common_logger.error(
                            f"{addr_id} {address}: has balance {amount_to_display(balance, 6, Decimal('0.01'))} TRX "
                            f"and can handle less then {Cfg.native_error_threshold} transactions")
                    await db.upsert_balance(addr_id, St.native.v, balance, commit=True)
                elif not isinstance(err, httpx.HTTPStatusError):
                    log_params = {"addr_id": addr_id, "address": address, "balance": balance, "error": err}
                    common_logger.error(f"admin_approve_native_bal {log_params}")


async def admin_coins_bal():
    reqs = []
    async with write_async_session() as session:
        db = DB(session)
        users: list[tuple[str, str]] = await db.users_addresses([St.SADMIN.v])

        if users:
            coins = await db.get_coins([Coins.contract_address, Coins.name])
            for addr_id, address in users:
                for coin in coins:
                    contract_address: str = coin[Coins.contract_address.key]
                    if contract_address != St.native.v:
                        conn_creds: list[tuple[str, str]] = await variables.api_keys_pool.get()
                        reqs.append(asyncio.create_task(trc20_balance(conn_creds,
                                                                      addr_id,
                                                                      contract_address,
                                                                      address)))

            results = await asyncio.gather(*reqs)

            for balance, err, req_ident in results:
                conn_creds, addr_id, contract_address = req_ident
                await variables.api_keys_pool.put(conn_creds)
                if not err:
                    await db.upsert_balance(addr_id, contract_address, balance, commit=True)
                elif not isinstance(err, httpx.HTTPStatusError):
                    log_params = {"addr_id": addr_id, "contract_address": contract_address, "balance": balance,
                                  "error": err}
                    common_logger.error(f"admin_coins_bal {log_params}")


async def update_in_memory_last_handled_block():
    async with read_async_session() as session:
        db = DB(session)
        block = await db.get_last_handled_block()
        if not block:
            if Cfg.start_block == "latest":
                conn_creds: list[tuple[str, str]] = await variables.api_keys_pool.get()
                async with MyAsyncTron(*conn_creds) as client:
                    block = await client.latest_block_number() - Cfg.block_offset
                    await db.insert_last_handled_block(block, commit=True)
                await variables.api_keys_pool.put(conn_creds)
            else:
                block = Cfg.start_block
                await db.insert_last_handled_block(Cfg.start_block, commit=True)
    common_logger.info(f"Last handled block: {block}")
    variables.last_handled_block = block


async def get_trusted_block():
    conn_creds = await variables.api_keys_pool.get()
    try:
        async with MyAsyncTron(*conn_creds) as client:
            block = await client.latest_block_number() - Cfg.block_offset
    finally:
        await variables.api_keys_pool.put(conn_creds)
    return block


async def update_in_memory_trusted_block():
    variables.trusted_block = await get_trusted_block()
    common_logger.info(f"Trusted block: {variables.trusted_block}")


async def update_coin_rates():
    rates, exceptions = await api.coin_rate_client.get_coin_rates()
    if exceptions:
        for exc in exceptions:
            common_logger.error(exc)

    async with write_async_session() as session:
        db = DB(session)
        coins = await db.get_coins([Coins.contract_address, Coins.name, Coins.current_rate])

        for coin in coins:
            if coin[Coins.name.key] != Cfg.quote_coin:
                symbol = f"{coin[Coins.name.key]}{Cfg.quote_coin}"
                if rates.get(symbol):
                    await db.update_coin(coin[Coins.contract_address.key], {Coins.current_rate.key: rates[symbol]},
                                         commit=True)
                else:
                    common_logger.error(f"No exchange rate for {symbol}")
            else:
                if coin[Coins.current_rate.key] != 1:
                    await db.update_coin(coin[Coins.contract_address.key], {Coins.current_rate.key: 1}, commit=True)


async def update_in_memory_accounts():
    """
    This function updates the user_accounts dictionary in the variables module.
    :param
    :return:
    """
    try:
        async with write_async_session() as session:
            db = DB(session)
            users = await db.all_accounts()
    except Exception as exc:
        log_params = {"error": exc}
        common_logger.error(f"update_in_memory_accounts {log_params}")
        raise
    else:
        variables.user_accounts_event.clear()
        for address_id, user_address, role in users:
            if role == St.USER.v:
                variables.user_accounts[user_address] = address_id
                variables.user_accounts_hex[web3_utils.to_hex_address(user_address)] = address_id
            else:
                variables.handler_accounts_hex[web3_utils.to_hex_address(user_address)] = address_id
    finally:
        variables.user_accounts_event.set()


async def coins_txs_parser(transactions: list[dict], coins: dict[str, dict]):
    """
    This function parses the TRC20 transactions and creates the deposit records.
    :param transactions: List[dict]
    :param coins: key - contract_address in hex format, value - dict with keys: name, min_amount
    :return:
    """
    event_name = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    deposits = []
    for unit in transactions:
        if "log" in unit:
            if unit["log"][0]["topics"][0] == event_name:
                if unit.get("contract_address") in coins:
                    address_received: str = trx_abi.decode_single("address",
                                                                  eth_utils.decode_hex(unit["log"][0]["topics"][2]))
                    await variables.user_accounts_event.wait()
                    address_id = variables.user_accounts.get(address_received)
                    if address_id:
                        amount: int = trx_abi.decode_single("uint256", eth_utils.decode_hex(unit["log"][0]["data"]))
                        coin = coins[unit["contract_address"]]
                        if amount >= coin[Coins.min_amount.key]:
                            quote_amount: Decimal = amount_to_quote_amount(amount,
                                                                           coin[Coins.current_rate.key],
                                                                           coin[Coins.decimal.key])
                            deposits.append({
                                Deposits.address_id.key: address_id,
                                Deposits.amount.key: amount,
                                Deposits.quote_amount.key: quote_amount,
                                Deposits.contract_address.key: web3_utils.to_base58check_address(
                                    unit["contract_address"]),
                                Deposits.tx_hash_in.key: unit['id']
                            })
                        else:
                            log_params = {"amount": amount,
                                          "contract_address": unit["contract_address"],
                                          "id": unit['id'],
                                          "min_amount": coin[Coins.min_amount.key]}
                            common_logger.warning(f"deposit less then minimum amount {log_params}")
    return deposits


async def native_txs_parser(block: dict, native_coin: dict):
    deposits = []
    for unit in block.get("transactions", []):
        if unit["ret"][0]["contractRet"] == "SUCCESS":
            tx_info = unit["raw_data"]["contract"][0]
            if tx_info["type"] == "TransferContract":
                sender = tx_info["parameter"]["value"]["owner_address"]
                recipient = tx_info["parameter"]["value"]["to_address"]
                amount = tx_info["parameter"]["value"]["amount"]
                await variables.user_accounts_event.wait()
                if recipient in variables.user_accounts_hex and sender not in variables.handler_accounts_hex:
                    if amount >= native_coin[Coins.min_amount.key]:
                        address_id = variables.user_accounts_hex[recipient]
                        quote_amount: Decimal = amount_to_quote_amount(amount,
                                                                       native_coin[Coins.current_rate.key],
                                                                       native_coin[Coins.decimal.key])
                        deposits.append({
                            Deposits.address_id.key: address_id,
                            Deposits.amount.key: amount,
                            Deposits.contract_address.key: St.native.v,
                            Deposits.tx_hash_in.key: unit['txID'],
                            Deposits.quote_amount.key: quote_amount
                        })
                    else:
                        log_params = {"amount": amount,
                                      "id": unit['txID'],
                                      "min_amount": native_coin[Coins.min_amount.key]}
                        common_logger.info(f"deposit less then minimum amount {log_params}")
    return deposits


async def block_parser():
    conn_creds_1 = await variables.api_keys_pool.get()
    conn_creds_2 = await variables.api_keys_pool.get()
    try:
        async with MyAsyncTron(*conn_creds_1) as client1, MyAsyncTron(*conn_creds_2) as client2:
            current_block = variables.last_handled_block + 1

            if variables.trusted_block - current_block < Cfg.block_offset * Cfg.allowed_slippage:
                try:
                    variables.trusted_block = await client1.latest_block_number() - Cfg.block_offset
                except Exception as exc:
                    if not isinstance(exc, httpx.HTTPStatusError):
                        log_params = {"error": exc}
                        common_logger.error(f"block_parser {log_params}")
                    return

            if variables.trusted_block > current_block:

                tasks = [asyncio.create_task(client1.get_txs_of_block(current_block)),
                         asyncio.create_task(client2.get_entire_block(current_block))]

                try:
                    transactions, block = await asyncio.gather(*tasks)
                except Exception as exc:
                    if not isinstance(exc, httpx.HTTPStatusError):
                        log_params = {"error": exc}
                        common_logger.error(f"block_parser {log_params}")
                else:
                    async with write_async_session() as session:
                        db = DB(session)
                        resp = await db.get_coins(
                            [Coins.contract_address, Coins.name, Coins.current_rate, Coins.min_amount,
                             Coins.decimal])

                        coins = {web3_utils.to_hex_address(coin[Coins.contract_address.key]): coin for coin in resp
                                 if
                                 coin[Coins.contract_address.key] != St.native.v}
                        coin_txs = await coins_txs_parser(transactions, coins)

                        native_coin = [coin for coin in resp if coin[Coins.contract_address.key] == St.native.v][0]
                        native_txs = await native_txs_parser(block, native_coin)

                        deposits = coin_txs + native_txs

                        if deposits:
                            await db.add_deposits(deposits)
                        await db.insert_last_handled_block(current_block, commit=True)
                        variables.last_handled_block = current_block
    finally:
        await variables.api_keys_pool.put(conn_creds_1)
        await variables.api_keys_pool.put(conn_creds_2)


async def postpone_deposit_handling(db, deposit_id, tx_handler_period):
    time_to_tx_handler = datetime.now(timezone.utc) + timedelta(seconds=tx_handler_period)
    tx_handler_period += 30
    await db.update_deposit_by_id(deposit_id, {Deposits.locked_by_tx_handler.key: False,
                                               Deposits.time_to_tx_handler.key: time_to_tx_handler,
                                               Deposits.tx_handler_period.key: tx_handler_period},
                                  commit=True)


async def tx_conductor_native():
    reqs = []
    async with write_async_session() as session:
        db = DB(session)
        deposits = await db.get_and_lock_pending_deposits_native()

        if deposits:
            for deposit in deposits:
                conn_creds: list[tuple[str, str]] = await variables.api_keys_pool.get()
                reqs.append(asyncio.create_task(native_transfer_to_admin(conn_creds=conn_creds, **deposit)))

            results = await asyncio.gather(*reqs)

            for tx_hash, err, req_ident in results:
                conn_creds, deposit_id, tx_handler_period = req_ident
                await variables.api_keys_pool.put(conn_creds)
                if not err:
                    await db.update_deposit_by_id(deposit_id, {Deposits.tx_hash_out.key: tx_hash,
                                                               Deposits.locked_by_tx_handler.key: False}, commit=True)
                else:  # exception handling
                    log_params = {"deposit_id": deposit_id, "tx_handler_period": tx_handler_period, "error": err}
                    if isinstance(err, (BuildTransactionError, TransactionNotFound, TvmError, ApiError,
                                        BadSignature, TaposError, TransactionError, ValidationError)):
                        common_logger.error(f"tx_conductor_native error {log_params}")
                        await postpone_deposit_handling(db, deposit_id, tx_handler_period)
                    else:
                        if isinstance(err, UnableToGetReceiptError):
                            common_logger.critical(f"tx_conductor_native error {log_params}")
                        elif not isinstance(err, httpx.HTTPStatusError):
                            common_logger.critical(f"tx_conductor_native unexpected error {log_params}")


async def native_transfer_to_admin(conn_creds,
                                   deposit_id,
                                   amount,
                                   user_private,
                                   admin_public,
                                   tx_handler_period) -> tuple[None, Exception, tuple[type, type, type]] | \
                                                         tuple[type, None, tuple[type, type, type]]:
    amount = int(amount)
    amount_with_fee: int = amount - variables.estimated_native_fee
    try:
        async with MyAsyncTron(*conn_creds) as client:
            res = await client.trx_transfer(admin_public, amount_with_fee, user_private)
    except Exception as exc:
        return None, exc, (conn_creds, deposit_id, tx_handler_period)
    else:
        return res, None, (conn_creds, deposit_id, tx_handler_period)


async def tx_conductor_coin():
    reqs = []
    async with write_async_session() as session:
        db = DB(session)
        deposits = await db.get_and_lock_pending_deposits_coin(5, variables.estimated_trc20_fee)

        if deposits:
            for deposit in deposits:
                conn_creds: list[tuple[str, str]] = await variables.api_keys_pool.get()
                reqs.append(asyncio.create_task(coin_transfer_to_admin(conn_creds=conn_creds, **deposit)))

            results = await asyncio.gather(*reqs)

            for tx_hash, err, req_ident in results:
                conn_creds, deposit_id, tx_handler_period, approve_id = req_ident
                await variables.api_keys_pool.put(conn_creds)
                if not err:
                    await db.update_deposit_by_id(deposit_id, {Deposits.tx_hash_out.key: tx_hash,
                                                               Deposits.locked_by_tx_handler.key: False}, commit=True)
                else:  # exception handling
                    log_params = {"deposit_id": deposit_id, "tx_handler_period": tx_handler_period, "error": err}
                    if isinstance(err, (PreparingTransactionError, BuildTransactionError,
                                        TransactionNotFound, TvmError, ApiError,
                                        BadSignature, TaposError, TransactionError, ValidationError)):
                        common_logger.error(f"tx_conductor_coin error {log_params}")
                        await postpone_deposit_handling(db, deposit_id, tx_handler_period)
                    else:
                        if isinstance(err, UnableToGetReceiptError):
                            common_logger.critical(f"tx_conductor_coin error {log_params}")
                        elif not isinstance(err, httpx.HTTPStatusError):
                            common_logger.critical(f"tx_conductor_coin unexpected error {log_params}")


async def withdraw_handler():
    reqs = []
    async with write_async_session() as session:
        db = DB(session)
        withdrawals = await db.get_and_lock_pending_withdrawals()
        if withdrawals:
            for withdrawal in withdrawals:
                contract_address = withdrawal[Withdrawals.contract_address.key]
                conn_creds: list[tuple[str, str]] = await variables.api_keys_pool.get()
                if contract_address == St.native.v:
                    reqs.append(asyncio.create_task(withdraw_native(conn_creds, **withdrawal)))
                else:
                    reqs.append(asyncio.create_task(withdraw_coin(conn_creds, **withdrawal)))

            results = await asyncio.gather(*reqs)

            for tx_hash, err, req_ident in results:
                conn_creds, withdrawal_id, tx_handler_period, adm_address_id = req_ident
                await variables.api_keys_pool.put(conn_creds)
                if not err:
                    await db.update_withdrawal_by_id(withdrawal_id, {Withdrawals.tx_hash_out.key: tx_hash}, commit=True)
                else:
                    log_params = {"withdrawal_id": withdrawal_id, "adm_address_id": adm_address_id,
                                  "tx_handler_period": tx_handler_period, "error": err}
                    if isinstance(err, (BuildTransactionError, TransactionNotFound,
                                        TvmError, ApiError, BadSignature, TaposError, TransactionError,
                                        ValidationError)):
                        common_logger.error(f"withdraw_handler error {log_params}")
                        time_to_tx_handler = datetime.now(timezone.utc) + timedelta(tx_handler_period)
                        tx_handler_period += 15
                        await db.update_withdrawal_by_id(
                            withdrawal_id, {Withdrawals.admin_addr_id.key: None,
                                            Withdrawals.time_to_tx_handler.key: time_to_tx_handler,
                                            Withdrawals.tx_handler_period.key: tx_handler_period}, commit=True)
                    else:
                        if isinstance(err, UnableToGetReceiptError):
                            common_logger.critical(f"withdraw_handler error {log_params}")
                        elif not isinstance(err, httpx.HTTPStatusError):
                            common_logger.critical(f"withdraw_handler unexpected error {log_params}")


async def deposit_callback_handler():
    reqs = []
    async with write_async_session() as session:
        db = DB(session)
        deposits = await db.get_and_lock_unnotified_deposits(100)

        if deposits:
            amount: Decimal
            for data in deposits:
                rounding: Decimal = get_round_for_rate(data[Coins.current_rate.key])
                display_amount: str = amount_to_display(data[Deposits.amount.key],
                                                        data[Coins.decimal.key],
                                                        rounding
                                                        )
                reqs.append(asyncio.create_task(notify_deposit(display_amount=display_amount, **data)))

            results = await asyncio.gather(*reqs)

            for exception, req_ident in results:
                deposit_id, callback_period, user_id = req_ident
                if not exception:
                    await db.update_deposit_by_id(deposit_id, {Deposits.is_notified.key: True,
                                                               Deposits.locked_by_callback.key: False}, commit=True)
                else:
                    log_params = {"deposit_id": deposit_id, "callback_period": callback_period, "user_id": user_id,
                                  "error": exception}
                    if isinstance(exception, api.proc_api_client.ClientException) and exception.http_code == 409:
                        callback_logger.warning(f"Deposit already notified {log_params}")
                        await db.update_deposit_by_id(deposit_id, {Deposits.locked_by_callback.key: False,
                                                                   Deposits.is_notified.key: True}, commit=True)
                    else:
                        time_to_callback = datetime.now(timezone.utc) + timedelta(seconds=callback_period)
                        callback_period += 60
                        await db.update_deposit_by_id(deposit_id, {Deposits.locked_by_callback.key: False,
                                                                   Deposits.time_to_callback.key: time_to_callback,
                                                                   Deposits.callback_period.key: callback_period},
                                                      commit=True)
                        callback_logger.error(f"deposit_callback_handler {log_params}")


async def withdrawal_callback_handler() -> None:
    """
    This function sends the withdrawal notifications to the users.
    :return:
    """
    reqs = []
    async with write_async_session() as session:
        db = DB(session)

        withdrawals = await db.get_and_lock_unnotified_withdrawals(100)

        if withdrawals:
            for data in withdrawals:
                rounding: Decimal = get_round_for_rate(data[Coins.current_rate.key])
                data[Coins.current_rate.key] = str(data[Coins.current_rate.key].quantize(rounding + 2))
                display_amount: str = amount_to_display(data[Withdrawals.amount.key],
                                                        data[Coins.decimal.key],
                                                        rounding
                                                        )
                reqs.append(asyncio.create_task(notify_withdrawal(display_amount=display_amount, **data)))
            results = await asyncio.gather(*reqs)

            for data, exception, req_ident in results:
                withdrawal_id, callback_period = req_ident
                if not exception:
                    await db.update_withdrawal_by_id(withdrawal_id, {Withdrawals.is_notified.key: True,
                                                                     Withdrawals.locked_by_callback.key: False},
                                                     commit=True)

                else:
                    log_params = {"withdrawal_id": withdrawal_id, "callback_period": callback_period,
                                  "error": exception}
                    if isinstance(exception, api.proc_api_client.ClientException) and exception.http_code == 409:
                        callback_logger.warning(f"Withdrawal already notified {log_params}")
                        await db.update_withdrawal_by_id(withdrawal_id,
                                                         {Withdrawals.locked_by_callback.key: False,
                                                          Withdrawals.is_notified.key: True}, commit=True)
                    else:
                        time_to_callback = datetime.now(timezone.utc) + timedelta(seconds=callback_period)
                        callback_period += 60
                        await db.update_withdrawal_by_id(withdrawal_id,
                                                         {Withdrawals.locked_by_callback.key: False,
                                                          Withdrawals.time_to_callback.key: time_to_callback,
                                                          Withdrawals.callback_period.key: callback_period},
                                                         commit=True)
                        callback_logger.error(f"withdrawal_callback_handler {log_params}")


async def main():
    scheduler = AsyncIOScheduler()
    scheduler._logger.setLevel(logging.ERROR)  # to avoid apscheduler noise warning logs

    try:
        await update_in_memory_trusted_block()
        await update_in_memory_last_handled_block()
        await update_in_memory_accounts()
        await update_coin_rates()
    except Exception as exc:
        startup_logger.error(f"launch failed {exc}")
        raise Exception(f"launch failed {exc}")
    else:
        startup_logger.info("launch success")
        scheduler.add_job(update_in_memory_accounts, "interval", seconds=10, max_instances=1)
        scheduler.add_job(update_coin_rates, "interval", seconds=10, max_instances=1)
        scheduler.add_job(admin_coins_bal, "interval", seconds=30, max_instances=1)
        scheduler.add_job(admin_approve_native_bal, "interval", seconds=30, max_instances=1)
        block_parser_job = scheduler.add_job(block_parser, "interval", seconds=variables.block_parser_interval,
                                             max_instances=1)
        scheduler.add_job(tx_conductor_coin, "interval", seconds=1, max_instances=1)
        scheduler.add_job(tx_conductor_native, "interval", seconds=1, max_instances=1)
        scheduler.add_job(withdraw_handler, "interval", seconds=1, max_instances=1)
        scheduler.add_job(deposit_callback_handler, "interval", seconds=1, max_instances=1)
        scheduler.add_job(withdrawal_callback_handler, "interval", seconds=1, max_instances=1)

        explorer_interval = 120
        scheduler.add_job(explorer,
                          "interval",
                          seconds=explorer_interval,
                          max_instances=1,
                          args=(explorer_interval, block_parser_job))

        scheduler.start()
        while True:
            await asyncio.sleep(1000)


if __name__ == '__main__':
    variables = SharedVariables()
    startup_logger = get_logger("startup_logger")
    providers_api_logger = get_logger("providers_api_logger")
    callback_logger = get_logger("callback_logger")
    common_logger = get_logger("common_logger")
    asyncio.run(main())
