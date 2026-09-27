# очнись_13.3 RAM

Основа: очнись_13.2.

## Что изменено

- Автоматический restart по RAM жёстко отключён. На Render Free локальная SQLite эфемерна, поэтому Memory Guard не имеет права завершать R1 ради очистки памяти.
- SQLite readers переведены с thread-local схемы на фиксированный общий пул из 4 WAL/query-only соединений. Количество reader connections больше не растёт вслед за числом worker threads.
- Уменьшены штатные worker counts для UI/nav/window/finance/fin-forward/callback-ack/fast-export. FAST UI оставлен в 2 потока для отзывчивости кнопок.
- Уменьшены горячие runtime-регистры: operation ledger, finance integrity tail, forward outcomes, finance view cache, short callbacks, window diagnostics.
- R32 RAM coalescer/queue ограничен 768 logical keys. Primary SQLite остаётся authoritative; R32 остаётся continuity/outbox слоем.
- Memory Guard при каждом тике проверяет idle MEGAcmd и освобождает оставшиеся mega-cmd-server/mega-exec. Завершённые MEGA zombie children дополнительно reaped после idle cleanup.
- Добавлен Removed Chat RAM GC. Для bot_removed чата:
  - cold finance/history поля сохраняются в SQLite и выгружаются из Python RAM;
  - очищается finance view cache;
  - чат удаляется из finance_active_chats;
  - удаляются transient window diagnostics;
  - metadata/tombstone чата остаётся.
- Через 20 секунд после старта и далее раз в 15 минут выполняется RAM sweep уже восстановленных bot_removed чатов. Никаких Telegram/network вызовов этот sweep не делает.
- Финансовая история удалённых чатов автоматически НЕ удаляется.

## Проверки

- `python -m py_compile runtime_flat.py runtime_config.py bot.py start_front.py FINALIZATION_GATE.py` — PASS.
- `python FINALIZATION_GATE.py` — 43/43 PASS.
- Отдельный concurrency test SQLite reader pool: 24 потока × 50 чтений, errors=0, created connections=4, pool timeouts=0 — PASS.

## Что смотреть после deploy

В Watcher ожидается новая строка SQLite readers вида:
`pool 4 | avail ... | touched threads ... | created 4 | cache 128 KB/reader | timeouts ...`

Особенно сравнить с 13.2:
- Python RAM / container RAM после 30–60 минут;
- Python threads;
- SQLite readers `created` (должно оставаться 4 после старта, кроме rebuild пула при DB replacement);
- количество `mega-cmd-server` в Children после idle периода;
- Memory Guard trim frequency.
