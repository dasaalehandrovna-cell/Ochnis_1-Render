import hmac
import logging
from contextlib import asynccontextmanager

from aiogram.types import Update
from fastapi import FastAPI, Header, HTTPException, Request

from bot_factory import create_bot
from config import settings
from dispatcher_factory import create_dispatcher
from lifecycle import on_shutdown, on_startup

logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

bot = create_bot()
dispatcher = create_dispatcher()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await on_startup(bot, dispatcher)
    try:
        yield
    finally:
        await on_shutdown(bot)


app = FastAPI(title="Telegram Finance Bot", lifespan=lifespan)


@app.get("/")
async def root() -> dict[str, str]:
    return {"status": "ok", "service": "telegram-finance-bot"}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "healthy"}


@app.post(settings.normalized_webhook_path)
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: str | None = Header(default=None),
) -> dict[str, bool]:
    supplied_secret = x_telegram_bot_api_secret_token or ""

    if not hmac.compare_digest(supplied_secret, settings.webhook_secret):
        raise HTTPException(status_code=403, detail="Неверный секрет webhook")

    try:
        payload = await request.json()
        update = Update.model_validate(payload, context={"bot": bot})
        await dispatcher.feed_update(bot, update)
    except Exception:
        logger.exception("Ошибка обработки обновления Telegram")
        # HTTP 500 просит Telegram повторить доставку обновления.
        raise HTTPException(
            status_code=500,
            detail="Не удалось обработать обновление",
        )

    return {"ok": True}
