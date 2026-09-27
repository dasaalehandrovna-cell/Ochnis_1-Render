FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# OCH13 R1 intentionally has no MEGAcmd package and no MEGA apt repository.
# Runtime is release-flat: no source catalogs/manifests are needed in /app.
COPY bot.py start_front.py runtime_config.py FINALIZATION_GATE.py ./

RUN B_T=123456:STARTUPSMOKE DB_FILE=/tmp/och13_build_smoke.sqlite3 \
    MEGA_ENABLED=0 REDIS_ENABLED=0 TELEGRAM_BACKUP_ENABLED=0 \
    REDIS_URL= PEER_PRIVATE_URL= PEER_SERVICE_URL= PEER_SHARED_SECRET= \
    TRAFFIC_AUDIT_ENABLED=0 BOT_DEFER_MAIN_R54=1 \
    FINALIZATION_RUNTIME_BUILD=1 FINALIZATION_STARTUP_SMOKE=1 \
    python FINALIZATION_GATE.py \
 && rm -f /tmp/och13_build_smoke.sqlite3 /tmp/och13_build_smoke.sqlite3-wal /tmp/och13_build_smoke.sqlite3-shm

CMD ["python", "start_front.py"]
