FROM python:3.12-slim

# MEGAcmd is retained only as an emergency standalone R1 recovery tool.
# OCHNIS 13 does not start/login to MEGA on the normal path when LOCAL/R2 is valid.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg openssl \
 && mkdir -p /etc/apt/keyrings \
 && curl -fsSL https://mega.nz/linux/repo/Debian_12/Release.key | gpg --dearmor -o /etc/apt/keyrings/mega.gpg \
 && echo "deb [signed-by=/etc/apt/keyrings/mega.gpg] https://mega.nz/linux/repo/Debian_12/ ./" > /etc/apt/sources.list.d/mega.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends megacmd \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# OCHNIS 13 runtime is intentionally small at deploy root.
COPY bot.py runtime_flat.py start_front.py runtime_config.py FINALIZATION_GATE.py ./

RUN B_T=123456:STARTUPSMOKE DB_FILE=/tmp/och13_build_smoke.sqlite3 \
    MEGA_ENABLED=0 REDIS_ENABLED=0 TELEGRAM_BACKUP_ENABLED=0 \
    REDIS_URL= PEER_PRIVATE_URL= PEER_SERVICE_URL= PEER_SHARED_SECRET= \
    MEGA_EMAIL= MEGA_PASSWORD= TRAFFIC_AUDIT_ENABLED=0 \
    FINALIZATION_RUNTIME_BUILD=1 FINALIZATION_STARTUP_SMOKE=1 \
    python FINALIZATION_GATE.py \
 && rm -f /tmp/och13_build_smoke.sqlite3 /tmp/och13_build_smoke.sqlite3-wal /tmp/och13_build_smoke.sqlite3-shm

CMD ["python", "start_front.py"]
