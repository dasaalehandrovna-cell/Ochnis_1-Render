# ОЧНИСЬ 13.4 STRAIGHT

## Цель
Выпрямлены три критических контура R1 без добавления нового wrapper-слоя:

1. обычные финансы;
2. финансовая пересылка;
3. обычная пересылка.

## Что изменено

- `add_record_to_chat` — один владелец: прямой вызов `_finance_add_record_base` + postcommit + origin identity. Удалено старое повторное определение и активный `_V262_BASE_ADD_RECORD`.
- `_v258_record_strong_keys` — одна реализация; локальные Telegram message id отделены от upstream/source ids.
- `normalize_chat_records` — одна реализация; origin-tagging встроен напрямую, без `_V262_BASE_NORMALIZE`.
- `_v260_bind_forward_finance_record` — одна реализация; bot/source/destination identity и origin key записываются в одном месте.
- `handle_finance_edit` — одно определение; первичный поиск bot-scoped; `source_order_msg_id` полностью исключён из edit fallback.
- `update_record_in_chat` — одно финальное определение.
- `get_forward_links` — одна реализация RAM -> durable local recovery; удалён active PARENT chain.
- `_forward_single_to_target` — exact-once/recovery и реальная Telegram delivery собраны в одной функции; удалён active PARENT chain.
- `schedule_forward_any_message` — permission/skip/outcome/pipeline в одной функции; public alias на canon/ORIG удалён.
- `forward_any_message` — contour gate + dispatch в одной функции; public PREV chain удалён.
- `resolve_forward_targets` — canonical id/suspended/tenant/finance map читаются в одной функции; active ORIG/canon chain удалён.
- `add_forward_link`, `remove_forward_link`, `set_forward_finance`, `remove_forward_finance` — public функции больше не ходят через PREV/canon поколения.
- `_persist_forward_finance_delivery_now` — batch/non-batch durability сведены в одну функцию без ORIG wrapper.

## Статический STRAIGHT gate
Критические имена обязаны иметь ровно одно top-level определение:

- `add_record_to_chat`
- `handle_finance_edit`
- `update_record_in_chat`
- `normalize_chat_records`
- `_v258_record_strong_keys`
- `_v260_bind_forward_finance_record`
- `get_forward_links`
- `_forward_single_to_target`
- `schedule_forward_any_message`
- `forward_any_message`
- `resolve_forward_targets`
- `add_forward_link`
- `remove_forward_link`
- `set_forward_finance`
- `remove_forward_finance`
- `_persist_forward_finance_delivery_now`

В исходнике этих функций gate запрещает активные ссылки на `_V152_ORIG`, `_V215_PREV`, `_V217_PREV`, `_V262_BASE`, `_OCH129_PARENT`, `_canon_`, `_v177_legacy`.

## Проверка журналов 26.09

- BOOT стартовал около 10:30:03, READY около 10:30:41.
- Уже около 10:31:41 memory guard вошёл в emergency примерно на 409 MB container RAM.
- R2 был недоступен значительную часть времени; R1 продолжал работать через fallback.
- Финансовая пересылка формально завершалась `ok=True`, но отдельные batch занимали 45-191 секунд.
- Перед выгрузкой Runtime ZIP контейнер снова доходил примерно до 512 MB, при этом `oom_kill=0`.
- Последние callbacks в журнале завершались `success=True`.
- После Runtime ZIP memory trim снизил container RAM примерно с 512 MB до 326 MB; экспорт завершился без ошибки.
- Следовательно, приложенные журналы не фиксируют момент падения/"смерти" бота. Они фиксируют тяжёлую деградацию latency/lock contention и заканчиваются самой выгрузкой.

## Проверки

`FINALIZATION OCHNIS 13.4: 47/47 PASS`.

Локальный startup-smoke в среде сборки не выполнялся из-за отсутствующего пакета `telebot`; это ограничение локальной среды, а не ошибка runtime. На Render `telebot` устанавливается из `requirements.txt`, и Docker build выполняет startup gate после установки зависимостей.
