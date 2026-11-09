import uvicorn
from config import settings


if __name__ == "__main__":
    uvicorn.run(
        "webhook_app:app",
        host="0.0.0.0",
        port=settings.port,
        log_level=settings.log_level.lower(),
        proxy_headers=True,
        forwarded_allow_ips="*",
    )
