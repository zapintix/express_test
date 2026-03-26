import os
from dataclasses import dataclass
from urllib.parse import urlparse
from uuid import UUID

from dotenv import load_dotenv

load_dotenv()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            "Set it in .env or pass it to the container environment."
        )
    return value.strip()


def _parse_uuid(name: str) -> UUID:
    value = _require_env(name)
    try:
        return UUID(value)
    except ValueError as exc:
        raise RuntimeError(
            f"Environment variable {name} must be a valid UUID, got: {value!r}."
        ) from exc


def _parse_url(name: str) -> str:
    value = _require_env(name)
    parsed = urlparse(value)
    if not parsed.scheme or not parsed.netloc:
        raise RuntimeError(
            f"Environment variable {name} must be a full URL, got: {value!r}."
        )
    return value


@dataclass(frozen=True)
class Settings:
    bot_id: UUID
    botx_api_url: str
    bot_secret_key: str
    communigate_username: str
    communigate_password: str


settings = Settings(
    bot_id=_parse_uuid("BOT_ID"),
    botx_api_url=_parse_url("BOTX_API_URL"),
    bot_secret_key=_require_env("BOT_SECRET_KEY"),
    communigate_username=_require_env("COMMUNIGATE_USERNAME"),
    communigate_password=_require_env("COMMUNIGATE_PASSWORD"),
)
