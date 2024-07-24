# -*- coding: utf-8 -*-

from sqlalchemy import exc as sqlalchemy_exc
from fastapi.responses import JSONResponse
from fastapi import Response, Request
from decimal import Decimal
from fastapi import FastAPI
import traceback

from db.database import DB, write_async_session, read_async_session
from db.models import Coins, Users, Balances
from config import Config as Cfg, StatCode as St
from misc import get_logger, \
    quote_amount_to_amount, \
    get_round_for_rate, \
    amount_to_display, \
    amount_to_quote_amount, \
    std_logger
from web3_client import utils

app = FastAPI()
bd_logger = get_logger("bd_logger")
route_logger = get_logger("route_logger")


async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception:
        std_logger.critical(traceback.format_exc())
        return JSONResponse({"error": "Service temporary unavailable"}, 503)


app.middleware('http')(catch_exceptions_middleware)


def json_success_response(data: dict | list, status_code: int) -> Response:
    return JSONResponse(data, status_code)


def json_error_response(description: str, status_code: int) -> Response:
    json_data = {"error": description}
    return JSONResponse(json_data, status_code)


@app.get("/get_handled_blocks")
async def get_handled_blocks(request: Request):
    input_data = request.query_params
    try:
        limit = int(input_data.get("limit", 20))
        offset = int(input_data.get("offset", 0))
        assert limit >= 0
        assert offset >= 0
    except (KeyError, ValueError, AssertionError):
        return json_error_response("Not enough or wrong arguments", 400)
    else:
        if request.headers.get("Api-Key") == Cfg.PROC_HANDLER_API_KEY:
            async with read_async_session() as session:
                db = DB(session, route_logger)
                resp = await db.get_handled_blocks(limit, offset, for_json=True)
                return json_success_response(resp, 200)
        else:
            return json_error_response("Wrong Api-Key", 401)


@app.get("/get_handler_info")
async def get_handler_info(request: Request):
    if request.headers.get("Api-Key") == Cfg.PROC_HANDLER_API_KEY:
        async with read_async_session() as session:
            db = DB(session, route_logger)
            resp = await db.get_coins([Coins.contract_address,
                                       Coins.name,
                                       Coins.decimal,
                                       Coins.min_amount,
                                       Coins.is_active], for_json=True)
            coins = {}
            min_amount: Decimal
            for coin in resp:
                contract_address = coin.pop(Coins.contract_address.key)
                coins[contract_address] = coin
            output_data = {"name": Cfg.PROC_HANDLER_NAME, "display_name": Cfg.PROC_HANDLER_DISPLAY, "coins": coins}
            return json_success_response(output_data, 200)
    else:
        return json_error_response("Wrong Api-Key", 401)


@app.post("/add_account")
async def add_account(request: Request):
    output_data = {}
    input_data = await request.json()
    try:
        user_id = input_data["user_id"]
    except KeyError:
        return json_error_response("Not enough or wrong arguments", 400)
    else:
        if request.headers.get("Api-Key") == Cfg.PROC_HANDLER_API_KEY:
            async with write_async_session() as session:
                db = DB(session, route_logger)

                public, secret = utils.create_pair()

                admin_id = await db.get_random_user_id(St.SADMIN.v)
                approve_id = await db.get_random_user_id(St.APPROVE.v)

                if admin_id and approve_id:
                    try:
                        await db.add_account(user_id, admin_id, approve_id, public, secret)
                    except sqlalchemy_exc.IntegrityError as exc:
                        if "unique" in str(exc.orig):
                            return json_error_response("Account with such login already exists", 409)
                        else:
                            bd_logger.error(f"add_account {exc}")
                            return json_error_response("Service temporary unavailable", 503)
                    except Exception as exc:
                        bd_logger.critical(f"add_account {exc} type: {type(exc)}")
                        return json_error_response("Service temporary unavailable", 503)
                    else:
                        output_data["address"] = public
                        return json_success_response(output_data, 200)
                else:
                    return json_error_response("Can not find admin accounts", 404)
        else:
            return json_error_response("Wrong Api-Key", 401)


@app.get("/get_withdraw_info")
async def get_withdraw_info(request: Request):
    input_data = request.query_params
    try:
        user_id = input_data["user_id"]
        quote_amount = Decimal(input_data["quote_amount"])
        assert quote_amount > 0
    except (KeyError, ValueError):
        return json_error_response("Not enough or wrong arguments", 400)
    else:
        if request.headers.get("Api-Key") == Cfg.PROC_HANDLER_API_KEY:
            async with read_async_session() as session:
                db = DB(session, route_logger)
                user = await db.get_user_by_id(user_id, [Users.id])
                if user:
                    resp = await db.get_coins([Coins.contract_address,
                                               Coins.name,
                                               Coins.decimal,
                                               Coins.current_rate,
                                               Coins.min_amount,
                                               Coins.fee_amount,
                                               ])
                    coins = {"name": Cfg.PROC_HANDLER_NAME, "display_name": Cfg.PROC_HANDLER_DISPLAY, "coins": {}}

                    for coin in resp:
                        contract_address = coin.pop(Coins.contract_address.key)
                        rounding: Decimal = get_round_for_rate(coin[Coins.current_rate.key])
                        estimated_amount = quote_amount_to_amount(quote_amount,
                                                                  coin[Coins.current_rate.key],
                                                                  coin[Coins.decimal.key],
                                                                  )
                        coins["coins"][contract_address] = {
                            "name": coin[Coins.name.key],
                            "current_rate": str(coin[Coins.current_rate.key].quantize(rounding)),
                            "estimated_amount": amount_to_display(estimated_amount,
                                                                  coin[Coins.decimal.key],
                                                                  rounding),
                            "min_quote_amount": str(amount_to_quote_amount(coin[Coins.min_amount.key],
                                                                           coin[Coins.current_rate.key],
                                                                           coin[Coins.decimal.key],
                                                                           )),
                            "fee_quote_amount": str(amount_to_quote_amount(coin[Coins.fee_amount.key],
                                                                           coin[Coins.current_rate.key],
                                                                           coin[Coins.decimal.key],
                                                                           ))
                        }
                    output_data = coins
                    return json_success_response(output_data, 200)
                else:
                    return json_error_response("User not found", 404)
        else:
            return json_error_response("Wrong Api-Key", 401)


@app.get("/get_deposit_info")
async def get_deposit_info(request: Request):
    input_data = request.query_params
    try:
        user_id = input_data["user_id"]
    except KeyError:
        return json_error_response("Not enough or wrong arguments", 400)
    else:
        if request.headers.get("Api-Key") == Cfg.PROC_HANDLER_API_KEY:
            async with read_async_session() as session:
                db = DB(session, route_logger)
                address = await db.get_user_deposit_info(user_id)
                if address:
                    resp = await db.get_coins([Coins.contract_address,
                                               Coins.name,
                                               Coins.decimal,
                                               Coins.min_amount,
                                               Coins.current_rate,
                                               Coins.is_active], for_json=False)
                    coins = {}
                    min_amount: Decimal
                    for coin in resp:
                        contract_address = coin.pop(Coins.contract_address.key)
                        rounding: Decimal = get_round_for_rate(coin.pop(Coins.current_rate.key))
                        coin[Coins.min_amount.key] = amount_to_display(coin[Coins.min_amount.key],
                                                                       coin[Coins.decimal.key],
                                                                       rounding)
                        coins[contract_address] = coin
                    output_data = {"address": address, "display_name": Cfg.PROC_HANDLER_DISPLAY, "coins": coins}
                    return json_success_response(output_data, 200)
                else:
                    return json_error_response("User not found", 404)
        else:
            return json_error_response("Wrong Api-Key", 401)


@app.post("/create_withdrawal")
async def create_withdrawal(request: Request):
    output_data = {}
    input_data = await request.json()
    try:
        user_id: str = input_data["user_id"]
        contract_address: str = input_data["contract_address"]
        address = input_data["address"]
        quote_amount = Decimal(input_data["quote_amount"])  # amount in Cfg.quote_coin value(USDT)
        user_currency = input_data["user_currency"]  # internal currency that user wihdrawing e.g USD, EUR...
        assert quote_amount > 0
        assert utils.is_valid_address(address)
    except (ValueError, AssertionError, KeyError):
        return json_error_response("Not enough or wrong arguments", 400)
    else:
        if request.headers.get("Api-Key") == Cfg.PROC_HANDLER_API_KEY:
            async with write_async_session() as session:
                db = DB(session, route_logger)
                coin = await db.get_coin(contract_address, [Coins.decimal, Coins.current_rate])
                amount: int = quote_amount_to_amount(quote_amount,
                                                     coin[Coins.current_rate.key],
                                                     coin[Coins.decimal.key]
                                                     )
                try:
                    withdrawal_id = await db.add_withdrawal(user_id, address, amount, quote_amount, contract_address,
                                                            user_currency)
                except Exception as exc:
                    bd_logger.critical(f"{exc}")
                    return json_error_response("Service temporary unavailable", 503)
                else:
                    output_data["withdrawal_id"] = withdrawal_id
                    return json_success_response(output_data, 200)
        else:
            return json_error_response("Wrong Api-Key", 401)


@app.get("/admin/balances")
async def admin_balances(request: Request):
    if request.headers.get("Api-Key") == Cfg.PROC_HANDLER_API_KEY:
        async with read_async_session() as session:
            db = DB(session, route_logger)
            resp = await db.get_admin_balances()
            balances = {}
            for balance in resp:
                address_id = balances.setdefault(balance[Balances.address_id.key],
                                                 {Users.role.key: balance[Users.role.key], "balances": {}})
                address_id[Balances.coin_id.key]["balances"] = balance[Balances.balance.key]
            return json_success_response(resp, 200)
    else:
        return json_error_response("Wrong Api-Key", 401)


@app.get("/deposit/pending")
async def deposit_total(request: Request):
    if request.headers.get("Api-Key") == Cfg.PROC_HANDLER_API_KEY:
        async with read_async_session() as session:
            db = DB(session, route_logger)
            resp = await db.get_pending_deposits(for_json=True)
            return json_success_response(resp, 200)
    else:
        return json_error_response("Wrong Api-Key", 401)


@app.get("/withdrawal/pending")
async def deposit_total(request: Request):
    if request.headers.get("Api-Key") == Cfg.PROC_HANDLER_API_KEY:
        async with read_async_session() as session:
            db = DB(session, route_logger)
            resp = await db.get_pending_withdrawals(for_json=True)
            return json_success_response(resp, 200)
    else:
        return json_error_response("Wrong Api-Key", 401)


@app.get("/readiness")
async def readiness(request: Request):
    return json_success_response({}, 200)
