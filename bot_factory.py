from aiogram import Bot

from config import settings


def create_bot() -> Bot:
    return Bot(token=settings.bot_token)
