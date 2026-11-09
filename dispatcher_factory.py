from aiogram import Dispatcher

from base_router import router
from update_middleware import UpdateLoggingMiddleware


def create_dispatcher() -> Dispatcher:
    dispatcher = Dispatcher()
    dispatcher.update.middleware(UpdateLoggingMiddleware())
    dispatcher.include_router(router)
    return dispatcher
