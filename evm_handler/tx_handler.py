# -*- coding: utf-8 -*-
# Description: This module contains the main logic of the ERC20 parser and the native coin.
from misc import get_logger, SharedVariables, amount_to_quote_amount, \
    get_round_for_rate, amount_to_display, proc_api_client
from web3_client import utils as web3_utils
from web3_client.async_client import MyAsyncEth, ERC20, TransactionNotFound, \
    AlreadyKnown, UnderpricedTransaction, InsufficientFundsForTx, TransactionFailed, StuckTransaction, \
    ProviderConnectionErrorOnTx
from db.database import DB, write_async_session, read_async_session, Withdrawals
from db.models import Deposits, Coins, UserAddress
from config import Config as Cfg, StatCode as St
import api

import asyncio
import logging
from decimal import Decimal
import eth_utils
import eth_abi
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.job import Job
from datetime import datetime, timedelta, timezone
import httpx
import traceback

variables = SharedVariables()
startup_logger = get_logger("startup_logger")
providers_api_logger = get_logger("providers_api_logger")
callback_logger = get_logger("callback_logger")
common_logger = get_logger("common_logger")


class PreparingTransactionError(Exception):
    def __init__(self, original_error, message):
        self.original_error = original_error
        self.message = message

    def __str__(self):
        return f"Build transaction error: {self.message} {self.original_error}"


async def explorer(interval: int, block_parser_job: Job):
    timeframe = timedelta(seconds=interval)

    # share_unsuccessful_requests = await variables.providers_request_explorer.share_unsuccessful_requests(timeframe)
    message = ""
    for provider in variables.providers_pool.providers:
        rps = await provider.request_explorer.rps(timeframe)
        description = await provider.request_explorer.description_by_status_code(timeframe)
        message += f"{provider.provider.name}:\n{description}\nRequests per second: {rps:.2}\n\n"

    providers_api_logger.info(f"Requests statistics for the last {interval} seconds:\n{message}")

    block_parser_job.reschedule(
        trigger='interval',
        seconds=variables.block_parser_interval
    )

    common_logger.info(f"Last handled block: {variables.last_handled_block}")

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


async def coin_transfer_to_admin(
        provider,
        contract_address,
        user_public,
        user_private,
        admin_public,
        approve_id,
        approve_public,
        approve_private,
        deposit_id,
        amount,
        tx_handler_period,
        address_id,
        **_) -> tuple[None, Exception, tuple[type, type, type, type, str]] | \
                tuple[type, None, tuple[type, type, type, type, None]]:
    amount = int(amount)
    try:
        async with MyAsyncEth(provider, Cfg.network_id) as client:
            contract = ERC20(client, contract_address, abi_info=web3_utils.erc20_abi)
            try:
                allowance = await contract.allowance(user_public, approve_public)
                if allowance < amount:
                    balance = await client.get_account_balance(user_public)
                    if balance < int(100000 * variables.gas_price * 1.3):
                        await variables.gas_price_event.wait()
                        await client.send_ether(user_public,
                                                int(100000 * variables.gas_price * 1.3),
                                                approve_private,
                                                gas_price=variables.gas_price,
                                                gas=21000)
                    await variables.gas_price_event.wait()
                    await contract.approve(approve_public, 9_999_999_999_999_999, user_private,
                                           gas_price=variables.gas_price)
            except Exception as exc:
                raise PreparingTransactionError(exc, "Unable to prepare transaction")
            else:
                await variables.gas_price_event.wait()
                res = await contract.transfer_from(user_public,
                                                   admin_public,
                                                   amount,
                                                   approve_private,
                                                   gas_price=variables.gas_price)
    except Exception as exc:
        return None, exc, (deposit_id, tx_handler_period, approve_id, address_id, traceback.format_exc())
    else:
        return res, None, (deposit_id, tx_handler_period, approve_id, address_id, None)


async def withdraw_coin(provider,
                        contract_address,
                        withdrawal_address,
                        amount,
                        admin_private,
                        withdrawal_id,
                        tx_handler_period,
                        admin_addr_id,
                        **_
                        ) -> tuple[None, Exception, tuple[type, type, type]] | \
                             tuple[type, None, tuple[type, type, type]]:
    amount = int(amount)
    try:
        async with MyAsyncEth(provider, Cfg.network_id) as client:
            contract = ERC20(client, contract_address, abi_info=web3_utils.erc20_abi)
            await variables.gas_price_event.wait()
            res = await contract.transfer(withdrawal_address, amount, admin_private, gas_price=variables.gas_price)
    except Exception as exc:
        return None, exc, (withdrawal_id, tx_handler_period, admin_addr_id)
    else:
        return res, None, (withdrawal_id, tx_handler_period, admin_addr_id)


async def withdraw_native(provider,
                          withdrawal_address,
                          amount,
                          admin_private,
                          withdrawal_id,
                          tx_handler_period,
                          admin_addr_id,
                          **_
                          ) -> tuple[None, Exception, tuple[type, type, type]] | \
                               tuple[type, None, tuple[type, type, type]]:
    amount = int(amount)
    try:
        async with MyAsyncEth(provider, Cfg.network_id) as client:
            await variables.gas_price_event.wait()
            res = await client.send_ether(withdrawal_address,
                                          amount,
                                          admin_private,
                                          gas_price=variables.gas_price,
                                          gas=21000)
    except Exception as exc:
        return None, exc, (withdrawal_id, tx_handler_period, admin_addr_id)
    else:
        return res, None, (withdrawal_id, tx_handler_period, admin_addr_id)


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


async def native_balance(provider, addr_id, address) -> tuple[int, None, tuple[type, type, None]] | \
                                                        tuple[None, Exception, tuple[type, type, str]]:
    try:
        async with MyAsyncEth(provider, Cfg.network_id) as client:
            res: int = await client.get_account_balance(address)
    except Exception as exc:
        return None, exc, (addr_id, address, traceback.format_exc())
    else:
        return res, None, (addr_id, address, None)


async def trc20_balance(provider, addr_id, contract_address, address: str) \
        -> tuple[int, None, tuple[type, type]] | tuple[None, Exception, tuple[type, type]]:
    try:
        async with MyAsyncEth(provider, Cfg.network_id) as client:
            contract = ERC20(client, contract_address, abi_info=web3_utils.erc20_abi)
            res: int = await contract.balance_of(address)
    except Exception as exc:
        return None, exc, (addr_id, contract_address)
    else:
        return res, None, (addr_id, contract_address)


async def admin_approve_native_bal():
    reqs = []
    async with write_async_session() as session:
        db = DB(session)
        users: list[tuple[str, str]] = await db.users_addresses([St.SADMIN.v, St.APPROVE.v])

        if users:
            for addr_id, address in users:
                provider = await variables.providers_pool.get()
                reqs.append(asyncio.create_task(native_balance(provider, addr_id, address)))

            results = await asyncio.gather(*reqs)

            for balance, err, req_ident in results:
                addr_id, address, tb = req_ident
                if not err:
                    if balance <= (variables.gas_price * 100000) * Cfg.native_warning_threshold:
                        common_logger.error(
                            f"{addr_id} {address}: has balance {amount_to_display(balance, 18, Decimal('0.01'))} ETH "
                            f"and can handle less then {Cfg.native_error_threshold} transactions")
                    await db.upsert_balance(addr_id, St.native.v, balance, commit=True)
                else:
                    if not isinstance(err, (httpx.HTTPStatusError, httpx.TimeoutException)):
                        log_params = {"addr_id": addr_id,
                                      "address": address,
                                      "balance": balance,
                                      "error": traceback.format_exc(),
                                      "traceback": tb}
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
                        provider = await variables.providers_pool.get()
                        reqs.append(asyncio.create_task(trc20_balance(provider,
                                                                      addr_id,
                                                                      contract_address,
                                                                      address)))

            results = await asyncio.gather(*reqs)

            for balance, err, req_ident in results:
                addr_id, contract_address = req_ident
                if not err:
                    await db.upsert_balance(addr_id, contract_address, balance, commit=True)
                elif not isinstance(err, httpx.HTTPStatusError):
                    log_params = {"addr_id": addr_id, "contract_address": contract_address, "balance": balance,
                                  "error": traceback.format_exc()}
                    common_logger.error(f"admin_coins_bal {log_params}")


async def update_in_memory_last_handled_block():
    async with read_async_session() as session:
        db = DB(session)
        block = await db.get_last_handled_block()
        if not block:
            if Cfg.start_block == "latest":
                provider = await variables.providers_pool.get()
                async with MyAsyncEth(provider, Cfg.network_id) as client:
                    block = await client.latest_block_number() - Cfg.block_offset
                    await db.insert_last_handled_block(block, commit=True)
            else:
                block = Cfg.start_block
                await db.insert_last_handled_block(Cfg.start_block, commit=True)
    common_logger.info(f"Last handled block: {block}")
    variables.last_handled_block = block


async def get_trusted_block():
    provider = await variables.providers_pool.get()
    async with MyAsyncEth(provider, Cfg.network_id) as client:
        block = await client.latest_block_number() - Cfg.block_offset
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


async def update_gas_price():
    provider = await variables.providers_pool.get()
    try:
        async with MyAsyncEth(provider, Cfg.network_id) as client:
            gas_price = int((await client.gas_price()) * 1.5)
    except Exception as exc:
        common_logger.error(exc)
    else:
        variables.gas_price_event.clear()
        variables.gas_price = gas_price
    finally:
        variables.gas_price_event.set()


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
                variables.user_accounts_low_case[user_address.lower()] = address_id
            else:
                variables.handler_accounts[user_address] = address_id
                variables.handler_accounts_low_case[user_address.lower()] = address_id
    finally:
        variables.user_accounts_event.set()


async def coins_txs_parser(transactions: list[dict], coins: dict[str, dict]):
    """
    This function parses the ERC20 events and creates the deposit records.
    :param transactions: List[dict]
    :param coins: key - contract_address string low, value - dict with keys: name, min_amount
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
                    log_params = {"amount": amount,
                                  "contract_address": unit["contract_address"],
                                  "id": unit['transactionHash'],
                                  "min_amount": coin[Coins.min_amount.key]}
                    common_logger.warning(f"deposit less then minimum amount {log_params}")
    return deposits


async def native_txs_parser(block: dict, native_coin: dict, client: MyAsyncEth):
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
                        log_params = {"amount": amount,
                                      "id": unit['hash'],
                                      "min_amount": native_coin[Coins.min_amount.key]}
                        common_logger.info(f"deposit less then minimum amount {log_params}")
    return deposits


async def block_parser():
    provider = await variables.providers_pool.get()
    async with MyAsyncEth(provider, Cfg.network_id) as client:
        current_block = variables.last_handled_block + 1
        if variables.trusted_block - current_block < Cfg.block_offset * Cfg.allowed_slippage:
            try:
                variables.trusted_block = await client.latest_block_number() - Cfg.block_offset
            except Exception as exc:
                if not isinstance(exc, (httpx.HTTPStatusError, httpx.TimeoutException)):
                    log_params = {"error": traceback.format_exc()}
                    common_logger.error(f"block_parser {log_params}")
                return

        if variables.trusted_block > current_block:

            tasks = [asyncio.create_task(client.get_logs({"fromBlock": eth_utils.to_hex(current_block),
                                                          "toBlock": eth_utils.to_hex(current_block),
                                                          "topics": [
                                                              "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]})),
                     asyncio.create_task(client.get_block_by_number(eth_utils.to_hex(current_block)))]

            try:
                transactions, block = await asyncio.gather(*tasks)
            except Exception as exc:
                if not isinstance(exc, (httpx.HTTPStatusError, httpx.TimeoutException)):
                    log_params = {"error": traceback.format_exc()}
                    common_logger.error(f"block_parser {log_params}")
            else:
                async with write_async_session() as session:
                    db = DB(session)
                    resp = await db.get_coins(
                        [Coins.contract_address, Coins.name, Coins.current_rate, Coins.min_amount,
                         Coins.decimal])

                    coins = {coin[Coins.contract_address.key].lower(): coin for coin in resp if
                             coin[Coins.contract_address.key] != St.native.v}
                    coin_txs = await coins_txs_parser(transactions, coins)

                    native_coin = [coin for coin in resp if coin[Coins.contract_address.key] == St.native.v][0]
                    native_txs = await native_txs_parser(block, native_coin, client)

                    deposits = coin_txs + native_txs

                    if deposits:
                        await db.add_deposits(deposits)
                    await db.insert_last_handled_block(current_block, commit=True)
                    variables.last_handled_block = current_block


async def postpone_deposit_handling(db, deposit_id, tx_handler_period, address_id, **kwargs):
    time_to_tx_handler = datetime.now(timezone.utc) + timedelta(seconds=tx_handler_period)
    tx_handler_period += 30
    await db.update_user_address_by_id(address_id, {UserAddress.locked_by_tx.key: False}, commit=False)
    await db.update_deposit_by_id(deposit_id, {Deposits.locked_by_tx_handler.key: False,
                                               Deposits.time_to_tx_handler.key: time_to_tx_handler,
                                               Deposits.tx_handler_period.key: tx_handler_period,
                                               **kwargs},
                                  commit=True)


async def native_transfer_to_admin(provider,
                                   deposit_id,
                                   amount,
                                   user_private,
                                   admin_public,
                                   tx_handler_period,
                                   address_id,
                                   **_) -> tuple[None, Exception, tuple[type, type, type, str]] | \
                                           tuple[type, None, tuple[type, type, type, None]]:
    amount = int(amount)
    amount_with_fee: int = int(amount - variables.gas_price * 21000)
    try:
        async with MyAsyncEth(provider, Cfg.network_id) as client:
            await variables.gas_price_event.wait()
            res = await client.send_ether(admin_public, amount_with_fee, user_private, gas_price=variables.gas_price,
                                          gas=21000)
    except Exception as exc:
        return None, exc, (deposit_id, tx_handler_period, address_id, traceback.format_exc())
    else:
        return res, None, (deposit_id, tx_handler_period, address_id, None)


async def native_transfer_to_admin_on_provider_err(provider,
                                                   deposit_id,
                                                   tx_handler_period,
                                                   address_id,
                                                   tx_hash_out,
                                                   **_) -> tuple[None, Exception, tuple[type, type, type, str]] | \
                                                           tuple[type, None, tuple[type, type, type, None]]:
    try:
        async with MyAsyncEth(provider, Cfg.network_id) as client:
            await client.result(tx_hash_out)
    except Exception as exc:
        return None, exc, (deposit_id, tx_handler_period, address_id, traceback.format_exc())
    else:
        return tx_hash_out, None, (deposit_id, tx_handler_period, address_id, None)


async def coin_transfer_to_admin_on_provider_err(provider,
                                                 deposit_id,
                                                 approve_id,
                                                 tx_handler_period,
                                                 address_id,
                                                 tx_hash_out,

                                                 **_) -> tuple[None, Exception, tuple[type, type, type, type, str]] | \
                                                         tuple[type, None, tuple[type, type, type, type, None]]:
    try:
        async with MyAsyncEth(provider, Cfg.network_id) as client:
            await client.result(tx_hash_out)
    except Exception as exc:
        return None, exc, (deposit_id, tx_handler_period, approve_id, address_id, traceback.format_exc())
    else:
        return tx_hash_out, None, (deposit_id, tx_handler_period, approve_id, address_id, None)


async def tx_conductor_native():
    reqs = []
    async with write_async_session() as session:
        db = DB(session)
        deposits = await db.get_and_lock_pending_deposits_native()
        if deposits:
            for deposit in deposits:
                provider = await variables.providers_pool.get()
                if deposit[Deposits.tx_hash_out.key] is None:
                    reqs.append(asyncio.create_task(
                        native_transfer_to_admin(provider=provider, **deposit)))  # handle new deposit or failed deposit
                else:
                    reqs.append(asyncio.create_task(
                        native_transfer_to_admin_on_provider_err(
                            provider=provider,
                            **deposit)))  # handle deposit with provider network error

            results = await asyncio.gather(*reqs)

            for tx_hash, err, req_ident in results:
                deposit_id, tx_handler_period, address_id, tb = req_ident
                if not err:
                    await db.update_user_address_by_id(address_id, {UserAddress.locked_by_tx.key: False}, commit=False)
                    await db.update_deposit_by_id(deposit_id, {Deposits.tx_hash_out.key: tx_hash}, commit=True)
                else:
                    log_params = {"deposit_id": deposit_id,
                                  "tx_handler_period": tx_handler_period,
                                  "error": err,
                                  "traceback": tb}
                    if isinstance(err, (AlreadyKnown,
                                        UnderpricedTransaction,
                                        InsufficientFundsForTx,
                                        TransactionFailed)):
                        common_logger.error(f"tx_conductor_native error {log_params}")
                        await postpone_deposit_handling(db, deposit_id, tx_handler_period, address_id)
                    elif isinstance(err, ProviderConnectionErrorOnTx):
                        common_logger.error(f"tx_conductor_native ProviderConnectionErrorOnTx {log_params}")
                        await postpone_deposit_handling(db, deposit_id, tx_handler_period, address_id,
                                                        tx_hash_out=err.tx_hash)
                    elif isinstance(err, StuckTransaction):
                        common_logger.critical(f"tx_conductor_native StuckTransaction {log_params}")
                    else:
                        common_logger.critical(f"tx_conductor_native Unexpected error {log_params}")


async def tx_conductor_coin():
    reqs = []
    async with write_async_session() as session:
        db = DB(session)
        deposits = await db.get_and_lock_pending_deposits_coin()

        if deposits:
            for deposit in deposits:
                provider = await variables.providers_pool.get()
                if deposit[Deposits.tx_hash_out.key] is None:
                    reqs.append(asyncio.create_task(coin_transfer_to_admin(provider=provider, **deposit)))
                else:
                    reqs.append(
                        asyncio.create_task(coin_transfer_to_admin_on_provider_err(provider=provider, **deposit)))

            results = await asyncio.gather(*reqs)

            for tx_hash, err, req_ident in results:
                deposit_id, tx_handler_period, approve_id, address_id, tb = req_ident

                await db.update_user_address_by_id(approve_id, {UserAddress.locked_by_tx.key: False}, commit=False)

                if not err:
                    await db.update_user_address_by_id(address_id, {UserAddress.locked_by_tx.key: False}, commit=False)
                    await db.update_deposit_by_id(deposit_id, {Deposits.tx_hash_out.key: tx_hash}, commit=True)
                else:  # exception handling
                    log_params = {"deposit_id": deposit_id,
                                  "tx_handler_period": tx_handler_period,
                                  "error": err,
                                  "traceback": tb}
                    if isinstance(err, (PreparingTransactionError,
                                        AlreadyKnown,
                                        UnderpricedTransaction,
                                        InsufficientFundsForTx,
                                        TransactionFailed)):
                        common_logger.error(f"tx_conductor_coin error {log_params}")
                        await postpone_deposit_handling(db, deposit_id, tx_handler_period, address_id)

                    elif isinstance(err, ProviderConnectionErrorOnTx):
                        common_logger.error(f"tx_conductor_coin ProviderConnectionErrorOnTx {log_params}")
                        await postpone_deposit_handling(db, deposit_id, tx_handler_period, address_id,
                                                        tx_hash_out=err.tx_hash)
                    elif isinstance(err, StuckTransaction):
                        common_logger.critical(f"tx_conductor_coin StuckTransaction {log_params}")
                    else:
                        common_logger.critical(f"tx_conductor_coin Unexpected error {log_params}")


async def withdraw_handler():
    reqs = []
    async with write_async_session() as session:
        db = DB(session)
        withdrawals = await db.get_and_lock_pending_withdrawals()
        if withdrawals:
            for withdrawal in withdrawals:
                contract_address = withdrawal[Withdrawals.contract_address.key]
                provider = await variables.providers_pool.get()
                if contract_address == St.native.v:
                    reqs.append(asyncio.create_task(withdraw_native(provider, **withdrawal)))
                else:
                    reqs.append(asyncio.create_task(withdraw_coin(provider, **withdrawal)))

            results = await asyncio.gather(*reqs)

            for tx_hash, err, req_ident in results:
                withdrawal_id, tx_handler_period, adm_address_id = req_ident
                if not err:
                    await db.update_user_address_by_id(adm_address_id, {UserAddress.locked_by_tx.key: False},
                                                       commit=False)
                    await db.update_withdrawal_by_id(withdrawal_id, {Withdrawals.tx_hash_out.key: tx_hash}, commit=True)
                else:
                    log_params = {"withdrawal_id": withdrawal_id, "adm_address_id": adm_address_id,
                                  "tx_handler_period": tx_handler_period, "error": err}

                    if isinstance(err, (AlreadyKnown,
                                        UnderpricedTransaction,
                                        InsufficientFundsForTx,
                                        TransactionFailed)):
                        common_logger.error(f"withdraw_handler error {log_params}")
                        time_to_tx_handler = datetime.now(timezone.utc) + timedelta(tx_handler_period)
                        tx_handler_period += 15
                        await db.update_withdrawal_by_id(
                            withdrawal_id, {Withdrawals.admin_addr_id.key: None,
                                            Withdrawals.time_to_tx_handler.key: time_to_tx_handler,
                                            Withdrawals.tx_handler_period.key: tx_handler_period}, commit=True)

                    else:
                        if isinstance(err, (TransactionNotFound, StuckTransaction)):
                            common_logger.critical(f"withdraw_handler error {log_params}")
                        elif not isinstance(err, httpx.HTTPStatusError):
                            log_params["error"] = traceback.format_exc()
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
        startup_logger.error(f"launch failed {traceback.format_exc()}")
        raise Exception(f"launch failed {exc}")
    else:
        startup_logger.info("launch success")
        scheduler.add_job(update_in_memory_accounts, "interval", seconds=10, max_instances=1)
        scheduler.add_job(update_gas_price, "interval", seconds=60, max_instances=1)
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
    asyncio.run(main())
