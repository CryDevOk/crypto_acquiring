# -*- coding: utf-8 -*-

from db.database import DB, write_async_session, read_async_session
from db.models import NetworkHandlers
from config import Config as Cfg, StatCode as St
from misc import get_logger, std_logger

from sqlalchemy import exc as sqlalchemy_exc
from fastapi.responses import JSONResponse
from fastapi import Response, Request, FastAPI
from api import handler_api_client
import asyncio
from decimal import Decimal
import os
from uuid import uuid4
import traceback

app = FastAPI()
route_logger = get_logger("route_logger")


async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception:
        std_logger.critical(traceback.format_exc())
        return JSONResponse({"error": "Service temporary unavailable"}, 503)


app.middleware('http')(catch_exceptions_middleware)


def json_success_response(data: dict, status_code: int) -> Response:
    return JSONResponse(data, status_code)


def json_error_response(description: str, status_code: int) -> Response:
    json_data = {"error": description}
    return JSONResponse(json_data, status_code)


async def get_deposit_info_by_handler(user_id: str, name, server_url, api_key):
    client = handler_api_client.Client(server_url, api_key)
    try:
        resp = await client.get_deposit_info(user_id=user_id)
    except Exception as exc:
        return name, None, exc
    else:
        return name, resp, None


async def get_withdraw_info_by_handler(user_id: str,
                                       quote_amount: str,
                                       name,
                                       server_url,
                                       api_key):
    client = handler_api_client.Client(server_url, api_key)
    try:
        resp = await client.get_withdraw_info(user_id=user_id, quote_amount=quote_amount)
    except Exception as exc:
        return name, None, exc
    else:
        return name, resp, None


async def verify_customer(customer_id, customer_api_key) -> bool:
    async with read_async_session() as session:
        db = DB(session, route_logger)
        result: bool = await db.verify_customer(customer_id, customer_api_key)
    return result


async def verify_customer_and_user(customer_id, customer_api_key, user_id) -> tuple[bool, bool]:
    async with read_async_session() as session:
        db = DB(session, route_logger)
        result = await db.verify_customer_and_user(customer_id, customer_api_key, user_id)
    return result


@app.post(f"/v1/api/private/user/add_customer")
async def add_customer(request: Request):
    input_data = await request.json()
    try:
        callback_url = input_data["callback_url"]
        callback_api_key = input_data["callback_api_key"]
    except KeyError:
        return json_error_response("Not enough or wrong arguments", 400)
    else:
        if request.headers.get("Api-Key") == Cfg.PROC_API_KEY:
            async with write_async_session() as session:
                db = DB(session, route_logger)
                customer_id = str(uuid4())
                api_key = os.urandom(32).hex()
                try:
                    await db.insert_customer(customer_id, callback_url, callback_api_key, api_key)
                except sqlalchemy_exc.IntegrityError as exc:
                    if "unique" in str(exc.orig):
                        return json_error_response("Customer already exists", 409)
                    else:
                        route_logger.error(f"Unexpected error: {exc}")
                        return json_error_response("Service temporary unavailable", 503)
                except Exception as exc:
                    route_logger.critical(f"Unexpected error: {exc}")
                    return json_error_response("Service temporary unavailable", 503)
                else:
                    await session.commit()
                    return json_success_response({"customer_id": customer_id, "api_key": api_key}, 200)
        else:
            return json_error_response("Wrong Api-Key", 401)


@app.post(f"/v1/api/private/user/update_customer_by_callback_url")
async def update_customer_by_callback_url(request: Request):
    input_data = await request.json()
    try:
        callback_url = input_data["callback_url"]
        callback_api_key = input_data.get("callback_api_key")
        api_key = input_data.get("api_key")
        assert callback_api_key or api_key, "at least one of the fields must be filled"
    except KeyError:
        return json_error_response("Not enough or wrong arguments", 400)
    else:
        if request.headers.get("Api-Key") == Cfg.PROC_API_KEY:
            async with read_async_session() as session:
                db = DB(session, route_logger)
                data_to_update = {}
                if callback_api_key:
                    data_to_update["callback_api_key"] = callback_api_key
                if api_key:
                    data_to_update["api_key"] = api_key

                try:
                    resp = await db.update_customer_by_callback_url(callback_url, data_to_update)
                except Exception as exc:
                    route_logger.critical(f"Unexpected error: {exc}")
                    return json_error_response("Service temporary unavailable", 503)
                else:
                    return json_success_response({"customer_id": resp["id"], "api_key": api_key}, 200)
        else:
            return json_error_response("Wrong Api-Key", 401)


@app.post(f"/v1/api/private/callback")
async def callback(request: Request):
    input_data = await request.json()
    try:
        callback_id = input_data["callback_id"]
        user_id = input_data["user_id"]
        path = input_data["path"]
        json_data = input_data["json_data"]
        assert isinstance(json_data, dict)
    except (KeyError, AssertionError):
        return json_error_response("Not enough or wrong arguments", 400)
    else:
        if request.headers.get("Api-Key") == Cfg.PROC_API_KEY:
            async with write_async_session() as session:
                db = DB(session, route_logger)
                try:
                    await db.insert_callback(callback_id, user_id, path, json_data)
                except sqlalchemy_exc.IntegrityError as exc:
                    if "unique" in str(exc.orig):
                        return json_error_response("Callback already exists", 409)
                    else:
                        route_logger.error(f"Unexpected error: {exc}")
                        return json_error_response("Service temporary unavailable", 503)
                except Exception as exc:
                    route_logger.critical(f"Unexpected error: {exc}")
                    return json_error_response("Service temporary unavailable", 503)
                else:
                    await session.commit()
                    return json_success_response({}, 200)
        else:
            return json_error_response("Wrong Api-Key", 401)


@app.get("/v1/api/private/get_tx_handlers")
async def get_tx_handlers(request: Request):
    input_data = request.query_params
    try:
        customer_id = input_data["customer_id"]
        customer_api_key = request.headers.get("Api-Key")
        assert customer_api_key
    except (KeyError, AssertionError, ValueError):
        return json_error_response("Not enough or wrong arguments", 400)
    else:
        if await verify_customer(customer_id, customer_api_key):
            async with read_async_session() as session:
                db = DB(session, route_logger)
                try:
                    resp = await db.get_handlers([NetworkHandlers.name, NetworkHandlers.display_name])
                except Exception as exc:
                    route_logger.critical(f"Unexpected error: {exc}")
                    return json_error_response("Service temporary unavailable", 503)
                else:
                    output_data = {name: {"display_name": display_name, "name": name} for name, display_name in resp}
                    return json_success_response(output_data, 200)
        else:
            return json_error_response("Wrong Api-Key", 401)


@app.post("/v1/api/private/user/add_account")
async def add_account(request: Request):
    input_data = await request.json()
    try:
        customer_id = input_data["customer_id"]
        customer_api_key = request.headers.get("Api-Key")
        assert customer_api_key

        user_id = input_data["user_id"]
    except (KeyError, AssertionError):
        return json_error_response("Not enough or wrong arguments", 400)
    else:
        if await verify_customer(customer_id, customer_api_key):
            async with write_async_session() as session:
                db = DB(session, route_logger)
                resp = await db.get_handlers([NetworkHandlers.name,
                                              NetworkHandlers.server_url,
                                              NetworkHandlers.api_key])

                try:
                    await db.insert_user(user_id, customer_id, St.USER.v)
                except sqlalchemy_exc.IntegrityError as exc:
                    await session.rollback()
                    if "unique" in str(exc.orig):
                        return json_error_response("User already exists", 409)
                    else:
                        route_logger.error(f"Unexpected error: {exc}")
                        return json_error_response("Service temporary unavailable", 503)
                except Exception as exc:
                    await session.rollback()
                    route_logger.critical(f"Unexpected error: {exc}")
                    return json_error_response("Service temporary unavailable", 503)
                else:
                    tasks = []
                    for name, server_url, api_key in resp:
                        client = handler_api_client.Client(server_url, api_key)
                        tasks.append(asyncio.create_task(client.add_account(user_id)))
                    try:
                        await asyncio.gather(*tasks)
                    except handler_api_client.ClientException as exc:
                        await session.rollback()
                        route_logger.critical(f"Error: {exc} type: {type(exc)}")
                        return json_error_response("Service temporary unavailable", 503)
                    else:
                        await session.commit()
                        return json_success_response({}, 200)
        else:
            return json_error_response("Wrong Api-Key", 401)


@app.get(f"/v1/api/private/user/get_withdraw_info")
async def get_withdraw_info(request: Request):
    input_data = request.query_params
    try:
        customer_id = input_data["customer_id"]
        customer_api_key = request.headers.get("Api-Key")
        assert customer_api_key

        user_id: str = input_data["user_id"]
        quote_amount = Decimal(input_data["quote_amount"])
        tx_handler: str = input_data.get("tx_handler")
        assert quote_amount > 0
    except (KeyError, AssertionError, ValueError):
        return json_error_response("Not enough or wrong arguments", 400)
    else:
        customer_verified, user_verified = await verify_customer_and_user(customer_id, customer_api_key, user_id)

        if customer_verified:
            if user_verified:
                async with read_async_session() as session:
                    db = DB(session, route_logger)
                    if tx_handler:
                        resp = await db.get_tx_handler(tx_handler,
                                                       [NetworkHandlers.name,
                                                        NetworkHandlers.server_url,
                                                        NetworkHandlers.api_key])
                        resp = [resp]
                    else:
                        resp = await db.get_handlers([NetworkHandlers.name,
                                                      NetworkHandlers.server_url,
                                                      NetworkHandlers.api_key])

                    tasks = []
                    for name, server_url, api_key in resp:
                        tasks.append(asyncio.create_task(get_withdraw_info_by_handler(user_id,
                                                                                      str(quote_amount),
                                                                                      name,
                                                                                      server_url,
                                                                                      api_key)
                                                         )
                                     )

                    results = await asyncio.gather(*tasks)
                    output_data = {}

                    for name, resp, exc in results:
                        if exc:
                            route_logger.error(f"get_withdraw_info: tx_handler error {exc}")
                        else:
                            output_data[name] = resp

                    if tx_handler:
                        return json_success_response(output_data[tx_handler], 200)
                    else:
                        return json_success_response(output_data, 200)
            else:
                return json_error_response("User not found", 404)
        else:
            return json_error_response("Wrong Api-Key", 401)


@app.get(f"/v1/api/private/user/get_deposit_info")
async def get_deposit_info(request: Request):
    input_data = request.query_params
    try:
        customer_id = input_data["customer_id"]
        customer_api_key = request.headers.get("Api-Key")
        assert customer_api_key

        user_id = input_data["user_id"]
    except KeyError:
        return json_error_response("Not enough or wrong arguments", 400)
    else:
        customer_verified, user_verified = await verify_customer_and_user(customer_id, customer_api_key, user_id)

        if customer_verified:
            if user_verified:
                async with read_async_session() as session:
                    db = DB(session, route_logger)
                    resp = await db.get_handlers([NetworkHandlers.name,
                                                  NetworkHandlers.server_url,
                                                  NetworkHandlers.api_key])
                    tasks = []
                    for name, server_url, api_key in resp:
                        tasks.append(
                            asyncio.create_task(get_deposit_info_by_handler(user_id, name, server_url, api_key)))

                    results = await asyncio.gather(*tasks)

                    output_data = {}

                    for name, resp, exc in results:
                        if exc:
                            route_logger.error(f"get_deposit_info: tx_handler error {exc}")
                        else:
                            output_data[name] = resp

                    return json_success_response(output_data, 200)
            else:
                return json_error_response("User not found", 404)
        else:
            return json_error_response("Wrong Api-Key", 401)


@app.post(f"/v1/api/private/user/create_withdrawal")
async def create_withdrawal(request: Request):
    input_data = await request.json()
    try:
        customer_id = input_data["customer_id"]
        customer_api_key = request.headers.get("Api-Key")
        assert customer_api_key

        user_id: str = input_data["user_id"]
        tx_handler = input_data["tx_handler"]
        contract_address: str = input_data["contract_address"]
        address = input_data["address"]
        quote_amount = input_data["quote_amount"]
        user_currency: str = input_data["user_currency"]
        assert Decimal(quote_amount) > 0
    except (ValueError, AssertionError, KeyError):
        return json_error_response("Not enough or wrong arguments", 400)
    else:
        customer_verified, user_verified = await verify_customer_and_user(customer_id, customer_api_key, user_id)

        if customer_verified:
            if user_verified:
                async with write_async_session() as session:
                    db = DB(session, route_logger)
                    server_url, api_key = await db.get_tx_handler(tx_handler,
                                                                  [NetworkHandlers.server_url, NetworkHandlers.api_key])
                    client = handler_api_client.Client(server_url, api_key)
                    try:
                        await client.create_withdrawal(user_id=user_id,
                                                       contract_address=contract_address,
                                                       address=address,
                                                       quote_amount=quote_amount,
                                                       user_currency=user_currency)
                    except handler_api_client.ClientException as exc:
                        route_logger.error(f"Error: {exc}")
                        return json_error_response("Service temporary unavailable", 503)
                    except Exception as exc:
                        route_logger.critical(f"Error: {exc}")
                        return json_error_response("Service temporary unavailable", 503)
                    else:
                        return json_success_response({}, 200)
            else:
                return json_error_response("User not found", 404)
        else:
            return json_error_response("Wrong Api-Key", 401)


@app.get("/readiness")
async def readiness():
    return json_success_response({}, 200)
