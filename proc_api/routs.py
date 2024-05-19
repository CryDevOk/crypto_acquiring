# -*- coding: utf-8 -*-

from db.database import DB, write_async_session, read_async_session
from misc import route_logger, app

from fastapi.responses import JSONResponse
from fastapi import Response, Request


def json_success_response(data: dict, status_code: int) -> Response:
    return JSONResponse(data, status_code)


def json_error_response(description: str, status_code: int) -> Response:
    json_data = {"error": description}
    return JSONResponse(json_data, status_code)


@app.get("/readiness")
async def readiness():
    return json_success_response({}, 200)
