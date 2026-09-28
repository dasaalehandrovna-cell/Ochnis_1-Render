# очнись_13.5 — Redis cache-only + MEGA repair

Дата: 2026-09-28

## Основание

По runtime-журналам 13.4 установлено:

- после EMPTY_INIT старые Telegram update могли повторно исполняться из remote event journal;
- при текущем запуске `REDIS_ENABLED=0` remote replay уже не происходил (`remote_event=0`);
- MEGA credentials присутствуют, startup login проходит, но root `/TelegramBotBackup_очнись` отсутствует;
- canonical publisher падал с `MEGA unavailable`, потому что вложенная проверка теряла control-plane lease и снова смотрела на runtime `MEGA_ENABLED=0`;
- MEGAcmd иногда отвечал `Already logged in. Please log out first.`, а runtime ошибочно трактовал это как login failure;
- отсутствие MEGA ошибочно могло переводить finance constitution в quarantine, хотя финансовая операция уже была зафиксирована в локальной SQLite;
- Telegram edit пересланной копии может законно вернуть `message can't be edited`; у кода уже был replace/re-send fallback, но первый отказ журналировался как красная runtime error.

## 1. Redis: только cache / coordination / dedupe

Удалён remote replay Telegram-команд:

- `split_recover_remote_events_v268()` теперь всегда возвращает 0;
- `_split_remote_pending_rows_v268()` не читает Redis/R2 как источник исполняемых update;
- remote witness больше не содержит полного Telegram payload;
- сохраняются только `update_id`, `chat_id`, тип, hash/размер, время и status;
- даже если Redis снова будет включён, его event metadata нельзя передать в `process_new_updates()`.

Добавлены packaged flags:

- `REDIS_EXECUTABLE_EVENT_REPLAY_ENABLED=0`
- `REDIS_RAW_TELEGRAM_PAYLOAD_ENABLED=0`
- `REDIS_RESTORE_AUTHORITY_ENABLED=0`

Startup restore остаётся: LOCAL -> R2 -> MEGA emergency -> EMPTY R1. Redis — не restore authority.

## 2. MEGA control-plane наследуется вложенными функциями

`mega_is_configured()` теперь считает активные `_V240_RECOVERY_AUTHORITY_ACTIVE` / `_V241_RESTORE_ACTIVE` полноценным control-plane lease.

Это значит:

- обычный FAST runtime по-прежнему держит `MEGA_ENABLED=0` и не держит MEGAcmd постоянно в RAM;
- canonical generation / restore временно получают доступ к MEGA;
- вложенные функции больше не теряют это разрешение и не отвечают ошибочно `MEGA unavailable`.

## 3. MEGA session reuse

`mega_login_if_needed()` исправлен:

- `mega-whoami` с return code 0 = активная session, даже если stdout не содержит старых текстовых маркеров;
- `mega-login` с текстом `Already logged in` = success, а не exception;
- если `mega-whoami` явно показывает другой e-mail, runtime выдаёт account mismatch вместо записи в чужой аккаунт;
- успешная session кешируется обычным TTL.

## 4. Root autoseed

Перед canonical generation runtime теперь:

1. получает control-plane lease;
2. подтверждает/возобновляет MEGA session;
3. создаёт canonical root `/TelegramBotBackup_очнись`, если его нет;
4. затем создаёт `database/generations/...`, manifests и `database/current_manifest.json`.

Создание root внутри уже идущей canonical-транзакции не запускает второй параллельный reseed.

## 5. MEGA outage больше не означает finance quarantine

Immutable finance event сначала сохраняется в локальную SQLite `data_constitution_pending`.

Если MEGA временно недоступна:

- финансовая операция считается локально сохранённой;
- event остаётся pending для будущей MEGA durability;
- ставится `constitution_ledger_durability_degraded_och135`;
- finance constitution НЕ переводится в quarantine только из-за отсутствия внешнего backup.

## 6. Telegram immutable edit

Для известных ответов Telegram вроде `message can't be edited` первый edit-failure теперь считается штатным переходом на replace fallback:

- journal: `forward_edit_replace_fallback_och135`;
- затем код удаляет старую destination copy, повторно отправляет новую и перебиндивает forward/finance identity;
- неожиданные edit errors по-прежнему идут в error log.

## 7. HEAVY / R2

HEAVY обновлён до `очнись_13.5-heavy`:

- event receipt сразу удаляет `payload`;
- Redis event store вычищает `payload`;
- Redis -> Worker event hydration отключён;
- `/internal/events/pending` отдаёт только non-executable metadata.

## Проверки

FAST:

- `py_compile` — PASS;
- FINALIZATION — **54/54 PASS**;
- semantic regression: control-plane with runtime MEGA OFF — PASS;
- semantic regression: `mega-whoami rc=0` without text marker — PASS;
- semantic regression: `Already logged in` — PASS;
- semantic regression: Redis witness contains no raw Telegram payload — PASS;
- semantic regression: remote event replay returns 0 — PASS.

HEAVY:

- `py_compile` — PASS;
- FINALIZATION — **20/20 PASS**.

Полный startup import локально не выполнялся: в текущем контейнере разработки отсутствует пакет `telebot`. Dockerfile FAST по-прежнему запускает FINALIZATION startup-smoke после установки `requirements.txt` во время Render build.
