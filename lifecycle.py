import logging

from aiogram import Bot, Dispatcher

from config import settings

logger = logging.getLogger(__name__)


async def on_startup(bot: Bot, dispatcher: Dispatcher) -> None:
    bot_info = await bot.get_me()
    webhook_url = (
        f"{settings.normalized_base_url}"
        f"{settings.normalized_webhook_path}"
    )

    await bot.set_webhook(
        url=webhook_url,
        secret_token=settings.webhook_secret,
        allowed_updates=dispatcher.resolve_used_update_types(),
        drop_pending_updates=False,
    )

    logger.info(
        "Бот запущен: @%s; webhook настроен: %s",
        bot_info.username,
        webhook_url,
    )


async def on_shutdown(bot: Bot) -> None:
    # Webhook оставляем зарегистрированным для последующих перезапусков Render.
    await bot.session.close()
    logger.info("HTTP-сессия Telegram закрыта")
