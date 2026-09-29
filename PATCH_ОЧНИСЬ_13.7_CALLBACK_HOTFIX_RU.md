# очнись_13.7 — CALLBACK HOTFIX

Причина регрессии 13.6: перед `def _r71_contour_callback_guard(...)` исчезло сохранение предыдущего callback-owner: `_R71_CONTOUR_CORE = contour_callback_guard`. Поэтому все callbacks, не начинающиеся с `r71:`, падали с `NameError: _R71_CONTOUR_CORE is not defined`.

Исправлено:
- восстановлено `_R71_CONTOUR_CORE = contour_callback_guard` до установки R71 guard;
- `/start`/message handlers не менялись;
- finance/forward/restore/MEGA бизнес-логика 13.6 не переписывалась;
- FINALIZATION_GATE теперь проверяет порядок binding до definition;
- добавлен AST-fence на неинициализированные release-chain globals `CORE/PREV/ORIG/BASE/PARENT`.

Отдельно: ошибка `MEGA current_manifest verify failed: None != generation...` остаётся отдельной проблемой canonical manifest verification и не была причиной падения callback-кнопок.
