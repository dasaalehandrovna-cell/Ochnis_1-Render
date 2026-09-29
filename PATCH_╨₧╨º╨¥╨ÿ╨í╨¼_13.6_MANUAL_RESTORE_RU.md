# очнись_13.6 — единое ручное восстановление

## Причина
В 13.5 ручной `/restore` уже принимал `latest_bot_state.sqlite3.gz` и доходил до подтверждения `replace`, но manual recovery оставался неодинаковым по источникам:

- pre_restore мог зависеть от MEGA/R2;
- Redis ещё встречался в старых recovery-seal путях, хотя по новой политике он cache-only;
- MEGA browser/file download зависел от выбранного владельца контура R1/R2 и не всегда переходил на вторую сторону;
- `/restore` не принимал raw `.sqlite3/.sqlite/.db`;
- canonical MEGA restore и folder-browser имели разные входы.

## Что исправлено

### 1. `/restore` — единая точка входа
Поддерживаются:

- `/restore` → режим загрузки файла;
- `/restore latest` → точная generation из canonical `current_manifest`;
- `/restore mega` → браузер всех папок MEGA;
- `/restore folder` / `/restore folders` → тот же MEGA browser;
- reply `/restore` на `.sqlite3.gz/.gz/.bin/.sqlite3/.sqlite/.db` → немедленная проверка и подтверждение.

Добавлены алиасы:

- `/restore_latest`;
- `/restore_mega`;
- `/restore_folder`.

### 2. Поддержка raw SQLite
Manual upload теперь принимает:

- `.sqlite3`;
- `.sqlite`;
- `.db`;
- `.sqlite3.gz/.gz`;
- `.bin` с raw SQLite или gzip snapshot;
- JSON/ISON/CSV — как раньше.

Raw SQLite сначала проверяется по magic `SQLite format 3`, затем упаковывается и проходит общий validator + `PRAGMA integrity_check`.

### 3. pre_restore больше не зависит от Redis
Перед destructive restore текущая база должна иметь durable safety copy.

Порядок:

1. Telegram durable pre_restore;
2. direct R1 MEGA pre_restore;
3. optional R2→MEGA fallback.

Redis не считается recovery anchor и не может разрешить destructive restore.

### 4. Telegram durable pre_restore
Добавлен стабильный Telegram slot:

`durable:manual_pre_restore`

Если MEGA/R2 временно недоступны, текущая SQLite всё равно может быть сохранена в Telegram до замены.

После успешного restore создаётся отдельный Telegram checkpoint восстановленной SQLite (`durable:manual_restore_latest`) и ставится canonical MEGA re-anchor.

### 5. Redis hard fence
- executable Telegram replay остаётся запрещён;
- manual Redis restore физически возвращает `disabled`;
- post-restore seal больше не публикует Redis FULL как recovery authority;
- Redis остаётся только cache/coordination/dedupe/locks.

### 6. MEGA folder browser: R2 → R1 fallback
Если MEGA route настроен на HEAVY, но R2 недоступен:

`browser R2 fail → direct R1 mega-ls(control-plane)`

Если route FAST, но R1 не может открыть MEGA:

`R1 fail → optional R2 fallback`.

### 7. MEGA selected-file restore: R2 → R1 fallback
Для выбранного файла:

`preferred owner → failure → second owner`

Direct R1 `mega-get` всегда запускается как `control_plane=True`, поэтому zero-resident `MEGA_ENABLED=0` обычного runtime не блокирует ручное восстановление.

### 8. Exact current_manifest restore
`_r81_mega_get_exact()` теперь явно делает login/control-plane и `mega-get(... control_plane=True)`.

### 9. После замены SQLite
Сохраняется существующий полный rehydrate-контур:

- `SQLITE.replace_database`;
- `load_data`;
- tenant bootstrap/isolation;
- post-restore rehydrate;
- forward/finance indexes;
- reminders reconcile;
- config/data constitution re-anchor;
- Telegram checkpoint;
- MEGA canonical checkpoint.

## Проверки

- `py_compile` — PASS;
- FINALIZATION — 46/46 PASS;
- semantic manual recovery — PASS:
  - R2 browser failure → R1 folder listing;
  - R2 file failure → R1 exact `mega-get`;
  - Telegram pre_restore slot works;
  - post-restore seal = Telegram + MEGA, `redis_ok=False`.

## Политика восстановления после 13.6

**Бизнес-истина:** SQLite.

**Ручное восстановление:** uploaded file / Telegram backup / exact MEGA generation / MEGA folder browser.

**Redis:** никогда не источник восстановления и никогда не источник повторного исполнения Telegram update.
