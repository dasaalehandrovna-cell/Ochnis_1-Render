from config import settings


def is_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in settings.admin_ids
