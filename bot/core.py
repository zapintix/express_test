import logging
from pybotx import Bot, BotAccountWithSecret, IncomingMessage
from pybotx.client.exceptions.callbacks import CallbackNotReceivedError
from bot.handlers import collector
from bot.settings import settings

logger = logging.getLogger(__name__)


async def internal_error_handler(
    message: IncomingMessage, bot: Bot, exc: Exception
) -> None:
    if isinstance(exc, CallbackNotReceivedError):
        logger.warning("Callback not received (timeout), ignoring: %s", exc)
        return
    logger.exception("Internal bot error:")
    try:
        await bot.answer_message("Произошла внутренняя ошибка. Попробуйте позже.")
    except Exception:
        logger.exception("Failed to send error message to user")


bot = Bot(
    collectors=[collector],
    bot_accounts=[
        BotAccountWithSecret(
            id=settings.bot_id,
            cts_url=settings.botx_api_url,
            secret_key=settings.bot_secret_key,
        ),
    ],
    exception_handlers={Exception: internal_error_handler},
)
