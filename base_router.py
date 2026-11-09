from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

router = Router(name="base")


@router.message(Command("start"))
async def start_handler(message: Message) -> None:
    await message.answer(
        "Бот запущен.\n"
        "Сейчас доступен базовый каркас. Финансовый учёт и пересылка "
        "будут подключены следующими модулями."
    )


@router.message(Command("help"))
async def help_handler(message: Message) -> None:
    await message.answer(
        "Доступные команды:\n"
        "/start — запуск\n"
        "/help — помощь"
    )
