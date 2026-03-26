import logging
from http import HTTPStatus
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pybotx import build_command_accepted_response
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

from bot.core import bot

app = FastAPI()


@app.on_event("startup")
async def startup():
    await bot.startup()


@app.on_event("shutdown")
async def shutdown():
    await bot.shutdown()


@app.post("/command")
async def command_handler(request: Request) -> JSONResponse:
    bot.async_execute_raw_bot_command(
        await request.json(),
        request_headers=request.headers,
    )
    return JSONResponse(
        build_command_accepted_response(),
        status_code=HTTPStatus.ACCEPTED,
    )


@app.get("/status")
async def status_handler(request: Request) -> JSONResponse:
    status = await bot.raw_get_status(
        dict(request.query_params),
        request_headers=request.headers,
    )
    return JSONResponse(status)


@app.post("/notification/callback")
async def callback_handler(request: Request) -> JSONResponse:
    await bot.set_raw_botx_method_result(
        await request.json(),
        verify_request=False,
    )
    return JSONResponse(
        build_command_accepted_response(),
        status_code=HTTPStatus.ACCEPTED,
    )
