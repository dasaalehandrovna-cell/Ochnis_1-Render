# v262.
import os
import io
import json
import csv
import copy
import re
import html
import logging
import sqlite3
import threading
import time
import zipfile
import gzip
import subprocess
import shutil
import tempfile
import calendar
import secrets
import hashlib
import queue
import heapq
import signal
import socket
import sys
import platform
import ctypes as _core_ctypes
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests
import urllib.parse
import telebot
from telebot import types
from telebot.types import InputMediaDocument, InputMediaPhoto, InputMediaVideo, InputMediaAudio, InputMediaAnimation
from flask import Flask, request
from collections import defaultdict, deque
from contextlib import contextmanager
from pathlib import Path

def _configure_glibc_allocator_v248() -> dict:
    """Keep glibc from creating one large malloc arena per worker thread.

    Safe no-op on non-glibc platforms. Environment can override 1..8; default 2.
    This must run before task-pool threads are created.
    """
    result = {'arena_max': 0, 'applied': False}
    try:
        arena_max = max(1, min(8, int(os.getenv('BOT_MALLOC_ARENA_MAX', os.getenv('MALLOC_ARENA_MAX', '2')) or '2')))
        result['arena_max'] = arena_max
        libc = _core_ctypes.CDLL(None)
        mallopt = getattr(libc, 'mallopt', None)
        if mallopt is not None:
            mallopt.argtypes = [_core_ctypes.c_int, _core_ctypes.c_int]
            mallopt.restype = _core_ctypes.c_int
            # glibc malloc.h: M_ARENA_MAX = -8
            result['applied'] = bool(mallopt(-8, arena_max))
    except Exception as exc:
        result['error'] = str(exc)[:180]
    return result

_GLIBC_ALLOCATOR_V248 = _configure_glibc_allocator_v248()
TRAFFIC_AUDIT_ENABLED = str(os.getenv('TRAFFIC_AUDIT_ENABLED', '1') or '1').strip().lower() not in {'0', 'false', 'no', 'off'}
_TRAFFIC_AUDIT_LOCK = threading.RLock()
_TRAFFIC_AUDIT_PROCESS_STARTED_AT = time.time()
_TRAFFIC_AUDIT_BASELINE_LOADED = False

def _traffic_empty_bucket():
    return {'outbound_bytes': 0, 'inbound_bytes': 0, 'calls': 0, 'errors': 0, 'categories': {}, 'operations': {}, 'days': {}, 'hours': {}}
_TRAFFIC_AUDIT_BASELINE = _traffic_empty_bucket()
_TRAFFIC_AUDIT_CURRENT = _traffic_empty_bucket()

def _traffic_day_key() -> str:
    try:
        tz = ZoneInfo(str(globals().get('DEFAULT_TZ') or 'America/Argentina/Buenos_Aires'))
        return datetime.now(timezone.utc).astimezone(tz).date().isoformat()
    except Exception:
        return datetime.now(timezone.utc).date().isoformat()

def _traffic_hour_key() -> str:
    try:
        tz = ZoneInfo(str(globals().get('DEFAULT_TZ') or 'America/Argentina/Buenos_Aires'))
        return datetime.now(timezone.utc).astimezone(tz).strftime('%Y-%m-%dT%H')
    except Exception:
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H')

def _traffic_add_row(bucket: dict, category: str, operation: str, outbound: int, inbound: int, error: bool=False, *, add_day: bool=True):
    outbound = max(0, int(outbound or 0))
    inbound = max(0, int(inbound or 0))
    category = str(category or 'other')
    operation = str(operation or category)[:180]
    bucket['outbound_bytes'] = int(bucket.get('outbound_bytes', 0) or 0) + outbound
    bucket['inbound_bytes'] = int(bucket.get('inbound_bytes', 0) or 0) + inbound
    bucket['calls'] = int(bucket.get('calls', 0) or 0) + 1
    if error:
        bucket['errors'] = int(bucket.get('errors', 0) or 0) + 1
    for root, key in ((bucket.setdefault('categories', {}), category), (bucket.setdefault('operations', {}), operation)):
        row = root.setdefault(key, {'outbound_bytes': 0, 'inbound_bytes': 0, 'calls': 0, 'errors': 0})
        row['outbound_bytes'] = int(row.get('outbound_bytes', 0) or 0) + outbound
        row['inbound_bytes'] = int(row.get('inbound_bytes', 0) or 0) + inbound
        row['calls'] = int(row.get('calls', 0) or 0) + 1
        if error:
            row['errors'] = int(row.get('errors', 0) or 0) + 1
    if add_day:
        day = _traffic_day_key()
        days = bucket.setdefault('days', {})
        drow = days.setdefault(day, {'outbound_bytes': 0, 'inbound_bytes': 0, 'calls': 0, 'errors': 0, 'categories': {}, 'operations': {}})
        drow.setdefault('categories', {})
        drow.setdefault('operations', {})
        drow['outbound_bytes'] += outbound
        drow['inbound_bytes'] += inbound
        drow['calls'] += 1
        drow['errors'] += int(bool(error))
        for root, key in ((drow['categories'], category), (drow['operations'], operation)):
            r = root.setdefault(key, {'outbound_bytes': 0, 'inbound_bytes': 0, 'calls': 0, 'errors': 0})
            r['outbound_bytes'] += outbound
            r['inbound_bytes'] += inbound
            r['calls'] += 1
            r['errors'] += int(bool(error))
        for old_day in sorted(days)[:-45]:
            days.pop(old_day, None)
        hour = _traffic_hour_key()
        hours = bucket.setdefault('hours', {})
        hrow = hours.setdefault(hour, {'outbound_bytes': 0, 'inbound_bytes': 0, 'calls': 0, 'errors': 0, 'categories': {}, 'operations': {}})
        hrow.setdefault('categories', {})
        hrow.setdefault('operations', {})
        hrow['outbound_bytes'] += outbound
        hrow['inbound_bytes'] += inbound
        hrow['calls'] += 1
        hrow['errors'] += int(bool(error))
        for root, key in ((hrow['categories'], category), (hrow['operations'], operation)):
            r = root.setdefault(key, {'outbound_bytes': 0, 'inbound_bytes': 0, 'calls': 0, 'errors': 0})
            r['outbound_bytes'] += outbound
            r['inbound_bytes'] += inbound
            r['calls'] += 1
            r['errors'] += int(bool(error))
        for old_hour in sorted(hours)[:-96]:
            hours.pop(old_hour, None)

def traffic_audit_record(category: str, operation: str, outbound_bytes: int=0, inbound_bytes: int=0, error: bool=False):
    if not TRAFFIC_AUDIT_ENABLED:
        return
    try:
        with _TRAFFIC_AUDIT_LOCK:
            _traffic_add_row(_TRAFFIC_AUDIT_CURRENT, category, operation, outbound_bytes, inbound_bytes, error)
    except Exception:
        pass

def _traffic_merge_plain(dst: dict, src: dict):
    for k in ('outbound_bytes', 'inbound_bytes', 'calls', 'errors'):
        dst[k] = int(dst.get(k, 0) or 0) + int((src or {}).get(k, 0) or 0)
    for group in ('categories', 'operations'):
        for key, row in ((src or {}).get(group) or {}).items():
            d = dst.setdefault(group, {}).setdefault(str(key), {'outbound_bytes': 0, 'inbound_bytes': 0, 'calls': 0, 'errors': 0})
            for k in ('outbound_bytes', 'inbound_bytes', 'calls', 'errors'):
                d[k] = int(d.get(k, 0) or 0) + int((row or {}).get(k, 0) or 0)
    for group_name, keep in (('days', 45), ('hours', 96)):
        for stamp, row in ((src or {}).get(group_name) or {}).items():
            d = dst.setdefault(group_name, {}).setdefault(str(stamp), {'outbound_bytes': 0, 'inbound_bytes': 0, 'calls': 0, 'errors': 0, 'categories': {}, 'operations': {}})
            d.setdefault('categories', {})
            d.setdefault('operations', {})
            for k in ('outbound_bytes', 'inbound_bytes', 'calls', 'errors'):
                d[k] = int(d.get(k, 0) or 0) + int((row or {}).get(k, 0) or 0)
            for subgroup in ('categories', 'operations'):
                for key, crow in ((row or {}).get(subgroup) or {}).items():
                    c = d[subgroup].setdefault(str(key), {'outbound_bytes': 0, 'inbound_bytes': 0, 'calls': 0, 'errors': 0})
                    for k in ('outbound_bytes', 'inbound_bytes', 'calls', 'errors'):
                        c[k] = int(c.get(k, 0) or 0) + int((crow or {}).get(k, 0) or 0)
        root = dst.get(group_name) or {}
        for old in sorted(root)[:-keep]:
            root.pop(old, None)
    return dst

def traffic_audit_load_baseline(snapshot: dict) -> bool:
    global _TRAFFIC_AUDIT_BASELINE_LOADED, _TRAFFIC_AUDIT_BASELINE
    try:
        payload = (snapshot or {}).get('audit') if isinstance(snapshot, dict) and 'audit' in snapshot else snapshot
        if not isinstance(payload, dict):
            return False
        with _TRAFFIC_AUDIT_LOCK:
            if _TRAFFIC_AUDIT_BASELINE_LOADED:
                return False
            base = _traffic_empty_bucket()
            _traffic_merge_plain(base, payload)
            _TRAFFIC_AUDIT_BASELINE = base
            _TRAFFIC_AUDIT_BASELINE_LOADED = True
        return True
    except Exception:
        return False

def traffic_audit_snapshot() -> dict:
    with _TRAFFIC_AUDIT_LOCK:
        total = _traffic_empty_bucket()
        _traffic_merge_plain(total, _TRAFFIC_AUDIT_BASELINE)
        _traffic_merge_plain(total, _TRAFFIC_AUDIT_CURRENT)
        current = json.loads(json.dumps(_TRAFFIC_AUDIT_CURRENT))
    return {'schema_version': 2, 'captured_at': datetime.now(timezone.utc).isoformat(timespec='seconds'), 'process_started_at': _TRAFFIC_AUDIT_PROCESS_STARTED_AT, 'baseline_loaded': bool(_TRAFFIC_AUDIT_BASELINE_LOADED), 'audit': total, 'current_process': current}

def _traffic_scope_bucket(scope: str='month') -> dict:
    snap = traffic_audit_snapshot()
    allb = snap['audit']
    scope = str(scope or 'month').lower()
    if scope in {'all', 'total'}:
        return allb
    if scope in {'process', 'run'}:
        return snap['current_process']
    out = _traffic_empty_bucket()
    today = _traffic_day_key()
    prefix = today[:7]
    for day, row in (allb.get('days') or {}).items():
        if scope == 'today' and day != today or (scope == 'month' and (not str(day).startswith(prefix))):
            continue
        out['outbound_bytes'] += int(row.get('outbound_bytes', 0) or 0)
        out['inbound_bytes'] += int(row.get('inbound_bytes', 0) or 0)
        out['calls'] += int(row.get('calls', 0) or 0)
        out['errors'] += int(row.get('errors', 0) or 0)
        for subgroup in ('categories', 'operations'):
            for key, crow in (row.get(subgroup) or {}).items():
                c = out[subgroup].setdefault(key, {'outbound_bytes': 0, 'inbound_bytes': 0, 'calls': 0, 'errors': 0})
                for k in ('outbound_bytes', 'inbound_bytes', 'calls', 'errors'):
                    c[k] += int(crow.get(k, 0) or 0)
        out['days'][str(day)] = json.loads(json.dumps(row))
    for hour, row in (allb.get('hours') or {}).items():
        day = str(hour)[:10]
        if scope == 'today' and day != today or (scope == 'month' and (not day.startswith(prefix))):
            continue
        out['hours'][str(hour)] = json.loads(json.dumps(row))
    return out

def _traffic_fmt_bytes(n: int) -> str:
    n = float(max(0, int(n or 0)))
    if n >= 1024 ** 3:
        return f'{n / 1024 ** 3:.2f} GB'
    if n >= 1024 ** 2:
        return f'{n / 1024 ** 2:.2f} MB'
    if n >= 1024:
        return f'{n / 1024:.1f} KB'
    return f'{int(n)} B'

def traffic_audit_text(scope: str='month') -> str:
    b = _traffic_scope_bucket(scope)
    labels = {'month': 'этот месяц', 'today': 'сегодня', 'process': 'этот процесс', 'all': 'вся сохранённая история'}
    label = labels.get(str(scope).lower(), str(scope))
    lines = [f'📶 МАКСИМАЛЬНЫЙ АУДИТ ТРАФИКА — {label}', f"Исходящий (главный ориентир): {_traffic_fmt_bytes(b.get('outbound_bytes', 0))}", f"Входящий: {_traffic_fmt_bytes(b.get('inbound_bytes', 0))}", f"Сетевых вызовов: {b.get('calls', 0)} · ошибок: {b.get('errors', 0)}", '', 'По системам:']
    rows = sorted((b.get('categories') or {}).items(), key=lambda kv: int((kv[1] or {}).get('outbound_bytes', 0) or 0), reverse=True)
    names = {'telegram': 'Telegram API', 'mega_put': 'MEGA upload', 'mega_get': 'MEGA download', 'mega_control': 'MEGA служебное', 'google': 'Google API', 'currency': 'Курс USD', 'self_http': 'Self/Render HTTP', 'peer_http': 'Второй Render HTTP', 'other_http': 'Прочий HTTP', 'web_http': 'HTTP responses/webhook'}
    for cat, row in rows:
        lines.append(f"• {names.get(cat, cat)}: ↑ {_traffic_fmt_bytes(row.get('outbound_bytes', 0))} · ↓ {_traffic_fmt_bytes(row.get('inbound_bytes', 0))} · {row.get('calls', 0)} выз. · err {row.get('errors', 0)}")
    ops = sorted((b.get('operations') or {}).items(), key=lambda kv: int((kv[1] or {}).get('outbound_bytes', 0) or 0), reverse=True)
    if ops:
        lines += ['', 'ТОП конкретных операций:']
        for op, row in ops[:10]:
            lines.append(f"• {op}: ↑ {_traffic_fmt_bytes(row.get('outbound_bytes', 0))} · {row.get('calls', 0)} выз.")
    hours = sorted((b.get('hours') or {}).items(), key=lambda kv: str(kv[0]), reverse=True)
    if hours:
        lines += ['', 'Последние часы (↑ исходящий):']
        for hour, row in hours[:8]:
            top = sorted((row.get('categories') or {}).items(), key=lambda kv: int((kv[1] or {}).get('outbound_bytes', 0) or 0), reverse=True)
            culprit = top[0][0] + ' ' + _traffic_fmt_bytes((top[0][1] or {}).get('outbound_bytes', 0)) if top else '—'
            lines.append(f"• {str(hour)[5:].replace('T', ' ')}: {_traffic_fmt_bytes(row.get('outbound_bytes', 0))} · {culprit}")
    lines += ['', '🧾 Сводка также пишется в обычные Render Logs при каждом checkpoint — без отдельного сетевого запроса.', 'ℹ️ Бот считает прикладные байты. Панель Render может быть выше из-за TLS/TCP/HTTP overhead и платформенного учёта.']
    return '\n'.join(lines)[:3900]
_TRAFFIC_AUDIT_LAST_RENDER_LOG_OUTBOUND = 0

def traffic_audit_render_log_summary(reason: str='periodic') -> str:
    """Write a no-network forensic summary to stdout/Render Logs.

    It cannot read Render billing counters, but it lets a pasted Render log reveal which
    application subsystem and concrete operation generated the bytes in that period.
    """
    global _TRAFFIC_AUDIT_LAST_RENDER_LOG_OUTBOUND
    try:
        snap = traffic_audit_snapshot()
        b = snap.get('audit') or {}
        total = int(b.get('outbound_bytes', 0) or 0)
        delta = max(0, total - int(_TRAFFIC_AUDIT_LAST_RENDER_LOG_OUTBOUND or 0))
        _TRAFFIC_AUDIT_LAST_RENDER_LOG_OUTBOUND = total
        cats = sorted((b.get('categories') or {}).items(), key=lambda kv: int((kv[1] or {}).get('outbound_bytes', 0) or 0), reverse=True)[:4]
        ops = sorted((b.get('operations') or {}).items(), key=lambda kv: int((kv[1] or {}).get('outbound_bytes', 0) or 0), reverse=True)[:4]
        cat_text = ', '.join((f"{k}={_traffic_fmt_bytes((v or {}).get('outbound_bytes', 0))}" for k, v in cats)) or 'none'
        op_text = ', '.join((f"{k}={_traffic_fmt_bytes((v or {}).get('outbound_bytes', 0))}" for k, v in ops)) or 'none'
        text = f"TRAFFIC_AUDIT_V209 reason={str(reason)[:40]} total_out={_traffic_fmt_bytes(total)} delta_since_log={_traffic_fmt_bytes(delta)} calls={int(b.get('calls', 0) or 0)} errors={int(b.get('errors', 0) or 0)} top_categories=[{cat_text}] top_operations=[{op_text}]"
        try:
            logging.info(text)
        except Exception:
            print(text, flush=True)
        return text
    except Exception as exc:
        try:
            logging.warning('TRAFFIC_AUDIT_V209 summary error: %s', exc)
        except Exception:
            pass
        return ''

def _traffic_value_size(value) -> int:
    try:
        if value is None:
            return 0
        if isinstance(value, (bytes, bytearray, memoryview)):
            return len(value)
        if isinstance(value, str):
            return len(value.encode('utf-8', errors='ignore'))
        if isinstance(value, dict):
            return len(urllib.parse.urlencode(value, doseq=True).encode())
        if isinstance(value, (list, tuple)):
            return len(urllib.parse.urlencode(value, doseq=True).encode())
        return len(str(value).encode())
    except Exception:
        return 0

def _traffic_file_size(value) -> int:
    try:
        obj = value
        if isinstance(value, tuple) and len(value) >= 2:
            obj = value[1]
        if isinstance(obj, (bytes, bytearray, memoryview)):
            return len(obj)
        if isinstance(obj, str) and os.path.isfile(obj):
            return os.path.getsize(obj)
        name = getattr(obj, 'name', None)
        if isinstance(name, str) and os.path.isfile(name):
            return os.path.getsize(name)
        fileno = getattr(obj, 'fileno', None)
        if callable(fileno):
            return os.fstat(fileno()).st_size
    except Exception:
        pass
    return 0

def _traffic_request_fallback_size(url: str, kwargs: dict) -> int:
    n = len(str(url or '').encode()) + 64
    n += _traffic_value_size(kwargs.get('params')) + _traffic_value_size(kwargs.get('data'))
    try:
        if kwargs.get('json') is not None:
            n += len(json.dumps(kwargs.get('json'), ensure_ascii=False, default=str, separators=(',', ':')).encode())
    except Exception:
        pass
    try:
        for _, v in (kwargs.get('files') or {}).items():
            n += _traffic_file_size(v) + 256
    except Exception:
        pass
    return n

def _traffic_classify_http(url: str, method: str) -> tuple[str, str]:
    try:
        u = urllib.parse.urlsplit(str(url or ''))
        host = (u.hostname or '').casefold()
        path = u.path or '/'
        last = path.rstrip('/').split('/')[-1] or '/'
    except Exception:
        host = ''
        last = '/'
    if host.endswith('api.telegram.org'):
        return ('telegram', f'telegram:{last}')
    if 'googleapis.com' in host or host.endswith('google.com'):
        return ('google', f'google:{method.upper()}:{host}:{last}')
    render_host = str(os.getenv('RENDER_EXTERNAL_HOSTNAME', '') or '').casefold()
    if render_host and host == render_host:
        return ('self_http', f'self:{method.upper()}:{last}')
    peer_url = str(os.getenv('PEER_KEEPALIVE_URL', '') or '')
    try:
        peer_fn = globals().get('keepalive_peer_target_url')
        if callable(peer_fn):
            peer_url = str(peer_fn() or peer_url)
    except Exception:
        pass
    try:
        peer_host = (urllib.parse.urlsplit(peer_url).hostname or '').casefold() if peer_url else ''
    except Exception:
        peer_host = ''
    if peer_host and host == peer_host:
        return ('peer_http', f'peer:{method.upper()}:{last}')
    rate_url = str(globals().get('USD_RATE_URL') or '')
    if 'dolarapi.com' in host or (rate_url and str(url or '') == rate_url):
        return ('currency', f'currency:{method.upper()}:{host}')
    return ('other_http', f"http:{method.upper()}:{host or 'unknown'}:{last}")

def _install_requests_traffic_audit():
    if not TRAFFIC_AUDIT_ENABLED:
        return False
    current = requests.sessions.Session.request
    if getattr(current, '_bot_traffic_audit_v205', False):
        return True
    original = current

    def wrapped(self, method, url, *args, **kwargs):
        fallback = _traffic_request_fallback_size(url, kwargs)
        category, operation = _traffic_classify_http(url, str(method))
        err = False
        response = None
        gate = globals().get('external_access_allowed_v233')
        if callable(gate) and (not gate(category)):
            try:
                traffic_audit_record(category, f'blocked:{operation}', 0, 0, False)
                logger_fn = globals().get('external_block_log_v233')
                if callable(logger_fn):
                    logger_fn(category, operation)
            except Exception:
                pass
            raise RuntimeError(f'external_local_only_v233:{category}:{operation}')
        try:
            response = original(self, method, url, *args, **kwargs)
            return response
        except Exception:
            err = True
            raise
        finally:
            out = fallback
            inbound = 0
            try:
                prep = getattr(response, 'request', None) if response is not None else None
                if prep is not None:
                    body = getattr(prep, 'body', None)
                    body_n = _traffic_value_size(body) if isinstance(body, (bytes, bytearray, memoryview, str)) else fallback
                    hdr = sum((len(str(k)) + len(str(v)) + 4 for k, v in (getattr(prep, 'headers', {}) or {}).items()))
                    out = max(fallback, len(str(getattr(prep, 'url', url) or '').encode()) + len(str(getattr(prep, 'method', method)).encode()) + hdr + body_n)
                if response is not None:
                    cl = (getattr(response, 'headers', {}) or {}).get('Content-Length')
                    if cl is not None and str(cl).isdigit():
                        inbound = int(cl)
                    elif not kwargs.get('stream', False):
                        inbound = len(getattr(response, 'content', b'') or b'')
                    if getattr(response, 'status_code', 200) >= 400:
                        err = True
            except Exception:
                pass
            traffic_audit_record(category, operation, out, inbound, err)
    wrapped._bot_traffic_audit_v205 = True
    wrapped._bot_traffic_audit_original = original
    requests.sessions.Session.request = wrapped
    return True
_install_requests_traffic_audit()
try:
    _BOT_THREAD_STACK_KB = max(512, min(8192, int(os.getenv('BOT_THREAD_STACK_KB', '768') or '768')))
    threading.stack_size(_BOT_THREAD_STACK_KB * 1024)
except Exception:
    _BOT_THREAD_STACK_KB = 0
window_locks = defaultdict(threading.Lock)

class KeyedTaskPool:

    def __init__(self, name: str, workers: int=4, max_pending: int=1000):
        self.name = str(name)
        self.workers = max(1, int(workers))
        self.max_pending = max(10, int(max_pending))
        self._ready = queue.Queue()
        self._lock = threading.RLock()
        self._by_key = defaultdict(deque)
        self._active_keys = set()
        self._pending = 0
        self._active_workers = 0
        self._submitted = 0
        self._completed = 0
        self._failed = 0
        self._rejected = 0
        self._max_wait = 0.0
        self._last_error = ''
        for idx in range(self.workers):
            t = threading.Thread(target=self._worker, name=f'{self.name}-{idx + 1}', daemon=True)
            t.start()

    def submit(self, key, func, *args, **kwargs) -> bool:
        key = str(key)
        with self._lock:
            if self._pending >= self.max_pending:
                self._rejected += 1
                return False
            self._by_key[key].append((func, args, kwargs, time.time()))
            self._pending += 1
            self._submitted += 1
            if key not in self._active_keys:
                self._active_keys.add(key)
                self._ready.put(key)
        return True

    def submit_unique(self, key, func, *args, **kwargs) -> bool:
        """Submit only when this logical key has no active/queued task.

        Used for heavy interactive file exports: repeated button presses must coalesce
        instead of building a long queue of the same ZIP/XLSX/journal job. Existing
        submit() semantics remain unchanged for finance/forward/business queues.
        """
        key = str(key)
        with self._lock:
            if key in self._active_keys or bool(self._by_key.get(key)):
                return False
            if self._pending >= self.max_pending:
                self._rejected += 1
                return False
            self._by_key[key].append((func, args, kwargs, time.time()))
            self._pending += 1
            self._submitted += 1
            self._active_keys.add(key)
            self._ready.put(key)
        return True

    def key_status(self, key) -> dict:
        """Small introspection helper for UI status; no queue mutation."""
        key = str(key)
        with self._lock:
            q = self._by_key.get(key)
            return {'active': key in self._active_keys, 'queued': len(q) if q else 0}

    def _worker(self):
        while True:
            key = self._ready.get()
            task = None
            with self._lock:
                q = self._by_key.get(key)
                if q:
                    task = q.popleft()
                    self._active_workers += 1
                else:
                    self._active_keys.discard(key)
                    self._by_key.pop(key, None)
            if task is None:
                self._ready.task_done()
                continue
            func, args, kwargs, enqueued_at = task
            wait = max(0.0, time.time() - enqueued_at)
            with self._lock:
                self._max_wait = max(self._max_wait, wait)
            try:
                func(*args, **kwargs)
                with self._lock:
                    self._completed += 1
            except Exception as exc:
                with self._lock:
                    self._failed += 1
                    self._last_error = str(exc)[:300]
                try:
                    log_error(f'POOL {self.name}: {exc}')
                except Exception:
                    logging.exception('POOL %s', self.name)
            finally:
                with self._lock:
                    self._pending = max(0, self._pending - 1)
                    self._active_workers = max(0, self._active_workers - 1)
                    q = self._by_key.get(key)
                    if q:
                        self._ready.put(key)
                    else:
                        self._by_key.pop(key, None)
                        self._active_keys.discard(key)
                task = func = args = kwargs = None
                self._ready.task_done()

    def wait_key_idle(self, key, timeout: float=15.0) -> bool:
        """Wait until all already-submitted tasks for one logical key finish.

        Used only by the durable MEGA witness before it declares a content update complete.
        It does not create new workers and does not affect unrelated chat keys.
        """
        key = str(key)
        deadline = time.monotonic() + max(0.0, float(timeout))
        while True:
            with self._lock:
                active = key in self._active_keys
                queued = bool(self._by_key.get(key))
            if not active and (not queued):
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.02)

    def stats(self) -> dict:
        with self._lock:
            return {'name': self.name, 'workers': self.workers, 'active': self._active_workers, 'pending': self._pending, 'keys': len(self._active_keys), 'submitted': self._submitted, 'completed': self._completed, 'failed': self._failed, 'rejected': self._rejected, 'max_wait': round(self._max_wait, 3), 'last_error': self._last_error}

class DelayedTaskScheduler:
    """Один поток хранит все логические таймеры без сотен threading.Timer."""

    def __init__(self, executor_pool: KeyedTaskPool):
        self.executor_pool = executor_pool
        self._cv = threading.Condition(threading.RLock())
        self._heap = []
        self._versions = {}
        self._deadlines = {}
        self._seq = 0
        self._submitted = 0
        self._executed = 0
        self._cancelled = 0
        self._failed_dispatch = 0
        threading.Thread(target=self._worker, name=f'{self.executor_pool.name}-scheduler', daemon=True).start()

    def _compact_locked(self, force: bool=False):
        live = len(self._deadlines)
        threshold = max(128, live * 4 + 64)
        if not force and len(self._heap) <= threshold:
            return 0
        before = len(self._heap)
        self._heap = [item for item in self._heap if int(self._versions.get(item[2], 0)) == int(item[3]) and self._deadlines.get(item[2]) == item[0]]
        heapq.heapify(self._heap)
        return max(0, before - len(self._heap))

    def compact(self) -> int:
        with self._cv:
            return self._compact_locked(True)

    def schedule(self, key, delay: float, func, *args, **kwargs):
        key = str(key)
        run_at = time.time() + max(0.0, float(delay or 0))
        with self._cv:
            self._seq += 1
            version = int(self._versions.get(key, 0)) + 1
            self._versions[key] = version
            self._deadlines[key] = run_at
            heapq.heappush(self._heap, (run_at, self._seq, key, version, func, args, kwargs))
            self._submitted += 1
            self._compact_locked(False)
            self._cv.notify_all()
        return run_at

    def cancel(self, key):
        key = str(key)
        with self._cv:
            self._versions[key] = int(self._versions.get(key, 0)) + 1
            if key in self._deadlines:
                self._deadlines.pop(key, None)
                self._cancelled += 1
            self._cv.notify_all()

    def deadline(self, key):
        with self._cv:
            return self._deadlines.get(str(key))

    def stats(self):
        with self._cv:
            return {'scheduled': len(self._deadlines), 'heap': len(self._heap), 'submitted': self._submitted, 'executed': self._executed, 'cancelled': self._cancelled, 'dispatch_failed': self._failed_dispatch}

    def _worker(self):
        while True:
            with self._cv:
                while not self._heap:
                    self._cv.wait()
                run_at, seq, key, version, func, args, kwargs = self._heap[0]
                wait = run_at - time.time()
                if wait > 0:
                    self._cv.wait(timeout=wait)
                    continue
                heapq.heappop(self._heap)
                if int(self._versions.get(key, 0)) != int(version):
                    continue
                self._deadlines.pop(key, None)
            dispatch_key = f'delay:{key}:{seq}'
            ok = self.executor_pool.submit(dispatch_key, self._execute, func, args, kwargs)
            if not ok:
                with self._cv:
                    self._failed_dispatch += 1
                    if int(self._versions.get(key, 0)) == int(version):
                        retry_at = time.time() + 0.5
                        self._seq += 1
                        self._deadlines[key] = retry_at
                        heapq.heappush(self._heap, (retry_at, self._seq, key, version, func, args, kwargs))
                        self._cv.notify_all()
                try:
                    log_error(f'DELAYED QUEUE FULL, RETRY: {key}')
                except Exception:
                    pass

    def _execute(self, func, args, kwargs):
        try:
            func(*args, **kwargs)
        finally:
            with self._cv:
                self._executed += 1

def _env_int(name: str, default: int, minimum: int=1, maximum: int=128) -> int:
    try:
        return max(minimum, min(maximum, int(os.getenv(name, str(default)) or default)))
    except Exception:
        return int(default)
WEBHOOK_TASK_POOL = KeyedTaskPool('content', _env_int('WEBHOOK_WORKERS', 2, 2, 8), _env_int('WEBHOOK_MAX_PENDING', 400, 50, 2000))
UI_TASK_POOL = KeyedTaskPool('ui', _env_int('UI_WORKERS', 2, 2, 8), _env_int('UI_MAX_PENDING', 400, 50, 2000))
CALLBACK_ACK_TASK_POOL = KeyedTaskPool('callback-ack', _env_int('CALLBACK_ACK_WORKERS', 1, 1, 3), _env_int('CALLBACK_ACK_MAX_PENDING', 600, 50, 3000))
RECOVERY_TASK_POOL = KeyedTaskPool('recovery', _env_int('RECOVERY_WORKERS', 1, 1, 3), _env_int('RECOVERY_MAX_PENDING', 300, 50, 1500))
REMINDER_TASK_POOL = KeyedTaskPool('reminder', _env_int('REMINDER_WORKERS', 1, 1, 3), _env_int('REMINDER_MAX_PENDING', 250, 20, 1000))
FINANCE_TASK_POOL = KeyedTaskPool('finance', _env_int('FINANCE_WORKERS', 2, 2, 8), _env_int('FINANCE_MAX_PENDING', 400, 50, 2000))
FIN_FORWARD_TASK_POOL = KeyedTaskPool('fin-forward', _env_int('FIN_FORWARD_WORKERS', 1, 1, 6), _env_int('FIN_FORWARD_MAX_PENDING', 500, 50, 2500))
FORWARD_TASK_POOL = KeyedTaskPool('forward', _env_int('FORWARD_WORKERS', 1, 1, 6), _env_int('FORWARD_MAX_PENDING', 500, 50, 2500))
BACKUP_TASK_POOL = KeyedTaskPool('backup', _env_int('BACKUP_WORKERS', 1, 1, 2), _env_int('BACKUP_MAX_PENDING', 120, 20, 500))
DELTA_TASK_POOL = KeyedTaskPool('delta', _env_int('DELTA_WORKERS', 1, 1, 2), _env_int('DELTA_MAX_PENDING', 300, 30, 1200))
EXPORT_TASK_POOL = KeyedTaskPool('export', _env_int('EXPORT_WORKERS', 1, 1, 2), _env_int('EXPORT_MAX_PENDING', 40, 5, 200))
BACKGROUND_TASK_POOL = KeyedTaskPool('background', _env_int('BACKGROUND_WORKERS', 1, 1, 4), _env_int('BACKGROUND_MAX_PENDING', 1600, 100, 6000))
MAINTENANCE_TASK_POOL = BACKGROUND_TASK_POOL
JOURNAL_TASK_POOL = BACKGROUND_TASK_POOL
GENERAL_TASK_POOL = BACKGROUND_TASK_POOL
DELAYED_TASK_POOL = KeyedTaskPool('scheduler', _env_int('SCHEDULER_WORKERS', 1, 1, 2), _env_int('SCHEDULER_MAX_PENDING', 1200, 100, 5000))
DOZVON_TASK_POOL = KeyedTaskPool('dozvon', _env_int('DOZVON_WORKERS', 1, 1, 2), _env_int('DOZVON_MAX_PENDING', 100, 10, 500))
DELAYED_SCHEDULER = DelayedTaskScheduler(DELAYED_TASK_POOL)
CALLBACK_ACK_SCHEDULER = DelayedTaskScheduler(CALLBACK_ACK_TASK_POOL)
try:
    WEBHOOK_ACK_WAIT_SECONDS = max(2.0, min(25.0, float(os.getenv('WEBHOOK_ACK_WAIT_SECONDS', '8') or '8')))
except Exception:
    WEBHOOK_ACK_WAIT_SECONDS = 8.0
try:
    WEBHOOK_STUCK_WARN_SECONDS = max(5.0, min(300.0, float(os.getenv('WEBHOOK_STUCK_WARN_SECONDS', '20') or '20')))
except Exception:
    WEBHOOK_STUCK_WARN_SECONDS = 20.0
try:
    WEBHOOK_DONE_TTL_SECONDS = max(60.0, min(3600.0, float(os.getenv('WEBHOOK_DONE_TTL_SECONDS', '600') or '600')))
except Exception:
    WEBHOOK_DONE_TTL_SECONDS = 600.0

class DurableUpdateDispatcher:
    """
    Независимый наблюдатель за входящими update.

    Он не выполняет бизнес-логику и не нарушает порядок операций одного чата.
    Его задача — не позволить webhook молча подтвердить Telegram update, который
    ещё только лежит в RAM-очереди. При timeout Telegram остаётся внешней
    долговечной очередью и повторяет update после рестарта/deploy.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._tickets = {}
        self._received = 0
        self._duplicates = 0
        self._completed = 0
        self._failed = 0
        self._timeouts = 0
        self._timeout_details = []
        self._retries = 0
        self._last_error = ''
        self._last_warn = {}
        threading.Thread(target=self._watchdog, name='update-dispatcher-watchdog', daemon=True).start()

    def claim(self, update_id, chat_id=None, update_type='other'):
        key = str(update_id)
        now = time.time()
        with self._lock:
            self._received += 1
            item = self._tickets.get(key)
            if item:
                state = item.get('state')
                if state == 'done':
                    self._duplicates += 1
                    return ('done', item)
                if state in {'queued', 'running'}:
                    self._duplicates += 1
                    return ('pending', item)
                self._retries += 1
                attempts = int(item.get('attempts', 1)) + 1
            else:
                attempts = 1
            event = threading.Event()
            item = {'update_id': key, 'chat_id': chat_id, 'type': str(update_type or 'other'), 'state': 'queued', 'created_at': now, 'started_at': None, 'finished_at': None, 'attempts': attempts, 'event': event, 'error': ''}
            self._tickets[key] = item
            return ('new', item)

    def mark_started(self, update_id):
        with self._lock:
            item = self._tickets.get(str(update_id))
            if item:
                item['state'] = 'running'
                item['started_at'] = time.time()

    def finish(self, update_id, success=True, error=''):
        with self._lock:
            item = self._tickets.get(str(update_id))
            if not item:
                return
            item['state'] = 'done' if success else 'failed'
            item['finished_at'] = time.time()
            item['error'] = str(error or '')[:500]
            if success:
                self._completed += 1
            else:
                self._failed += 1
                self._last_error = item['error']
            event = item.get('event')
        if event:
            event.set()

    def release_failed_enqueue(self, update_id, error='queue_full'):
        self.finish(update_id, False, error)

    def wait_result(self, item, timeout):
        event = item.get('event')
        if event and (not event.wait(max(0.1, float(timeout)))):
            with self._lock:
                self._timeouts += 1
                detail = {'update_id': str(item.get('update_id') or ''), 'chat_id': item.get('chat_id'), 'type': str(item.get('type') or 'other'), 'state': str(item.get('state') or ''), 'attempts': int(item.get('attempts') or 1), 'age_seconds': round(max(0.0, time.time() - float(item.get('created_at') or time.time())), 3), 'at': now_local().isoformat(timespec='milliseconds') if 'now_local' in globals() else ''}
                self._timeout_details.append(detail)
                if len(self._timeout_details) > 24:
                    self._timeout_details = self._timeout_details[-24:]
            try:
                bot_journal('dispatcher_ack_timeout_v224', item.get('chat_id'), json.dumps(detail, ensure_ascii=False, separators=(',', ':')), 'WARN')
            except Exception:
                pass
            return ('timeout', '')
        with self._lock:
            state = str(item.get('state') or '')
            return (state, str(item.get('error') or ''))

    def stats(self):
        now = time.time()
        with self._lock:
            pending = [x for x in self._tickets.values() if x.get('state') in {'queued', 'running'}]
            oldest = max([now - float(x.get('created_at', now)) for x in pending] or [0.0])
            return {'pending': len(pending), 'oldest': round(oldest, 2), 'received': self._received, 'duplicates': self._duplicates, 'completed': self._completed, 'failed': self._failed, 'timeouts': self._timeouts, 'timeout_details': list(self._timeout_details[-12:]), 'retries': self._retries, 'last_error': self._last_error, 'ack_wait': WEBHOOK_ACK_WAIT_SECONDS}

    def _watchdog(self):
        while True:
            try:
                time.sleep(2.0)
                now = time.time()
                stale_keys = []
                warnings = []
                with self._lock:
                    for key, item in list(self._tickets.items()):
                        state = item.get('state')
                        age = now - float(item.get('created_at', now))
                        if state in {'done', 'failed'}:
                            finished = float(item.get('finished_at') or item.get('created_at') or now)
                            if now - finished > WEBHOOK_DONE_TTL_SECONDS:
                                stale_keys.append(key)
                            continue
                        if age >= WEBHOOK_STUCK_WARN_SECONDS:
                            last = float(self._last_warn.get(key, 0) or 0)
                            if now - last >= WEBHOOK_STUCK_WARN_SECONDS:
                                self._last_warn[key] = now
                                warnings.append((key, item.get('chat_id'), item.get('type'), age, state))
                    for key in stale_keys:
                        self._tickets.pop(key, None)
                        self._last_warn.pop(key, None)
                for key, chat_id, typ, age, state in warnings:
                    try:
                        log_error(f'DISPATCHER STUCK: update={key} chat={chat_id} type={typ} state={state} age={age:.1f}s; Telegram will retry until 2xx')
                    except Exception:
                        pass
            except Exception:
                time.sleep(2.0)
UPDATE_DISPATCHER = DurableUpdateDispatcher()
chat_locks = defaultdict(threading.RLock)
data_lock = threading.RLock()
forward_map_lock = threading.RLock()
timer_lock = threading.RLock()
_state_context = threading.local()

def chat_lock_for(chat_id: int):
    return chat_locks[int(chat_id)]

@contextmanager
def locked_chat(chat_id: int):
    with chat_lock_for(int(chat_id)):
        yield

@contextmanager
def state_chat_context(chat_id):
    prev = getattr(_state_context, 'chat_id', None)
    try:
        _state_context.chat_id = int(chat_id) if chat_id is not None else None
        yield
    finally:
        _state_context.chat_id = prev

def current_state_chat_id():
    return getattr(_state_context, 'chat_id', None)

def _extract_update_chat_id(payload: dict):
    """Достаёт chat_id из сырого Telegram update до передачи в telebot."""
    try:
        for key in ('message', 'edited_message', 'channel_post', 'edited_channel_post'):
            item = payload.get(key)
            if isinstance(item, dict):
                chat = item.get('chat') or {}
                if 'id' in chat:
                    return int(chat['id'])
        cq = payload.get('callback_query')
        if isinstance(cq, dict):
            msg = cq.get('message') or {}
            chat = msg.get('chat') or {}
            if 'id' in chat:
                return int(chat['id'])
    except Exception:
        pass
    return None
try:
    FORWARD_FINANCE_PRIORITY_MAX_WAIT_SECONDS = max(0.0, min(10.0, float(os.getenv('FORWARD_FINANCE_PRIORITY_MAX_WAIT_SECONDS', '2.0') or '2.0')))
except Exception:
    FORWARD_FINANCE_PRIORITY_MAX_WAIT_SECONDS = 2.0

def _wait_for_finance_priority_before_forward(kind: str='forward') -> float:
    """
    v110: финансовая очередь имеет приоритет над пересылкой.
    Пересылка не блокирует finance-worker: она только коротко уступает CPU, пока
    в FINANCE_TASK_POOL есть pending/active работа. Есть жёсткий потолок ожидания,
    чтобы длинный поток финансов не мог навсегда остановить пересылку.
    """
    started = time.monotonic()
    limit = float(FORWARD_FINANCE_PRIORITY_MAX_WAIT_SECONDS or 0.0)
    if limit <= 0:
        return 0.0
    while True:
        try:
            fs = FINANCE_TASK_POOL.stats()
            busy = int(fs.get('pending', 0) or 0) > 0 or int(fs.get('active', 0) or 0) > 0
        except Exception:
            busy = False
        if not busy:
            break
        elapsed = time.monotonic() - started
        if elapsed >= limit:
            try:
                bot_journal('forward_priority_timeout', None, f'kind={kind} waited={elapsed:.3f}s finance still busy', 'WARN')
            except Exception:
                pass
            break
        time.sleep(0.02)
    waited = time.monotonic() - started
    if waited >= 0.05:
        try:
            bot_journal('forward_yielded_to_finance', None, f'kind={kind} waited={waited:.3f}s')
        except Exception:
            pass
    return waited

def _forward_with_finance_priority(source_chat_id: int, msg):
    _wait_for_finance_priority_before_forward('message')
    return forward_any_message(source_chat_id, msg)

def _forward_edit_with_finance_priority(msg):
    _wait_for_finance_priority_before_forward('edit')
    return propagate_edited_to_copies(msg)

def _forward_delete_with_finance_priority(source_chat_id: int, source_msg_id: int):
    _wait_for_finance_priority_before_forward('delete')
    return delete_forward_copies_for_source(source_chat_id, source_msg_id)

def _current_bot_id_for_forwarding() -> int:
    """Return this bot's Telegram user id without a network call when possible."""
    try:
        token = str(globals().get('BOT_TOKEN') or '').strip()
        head = token.split(':', 1)[0].strip()
        if head.isdigit():
            return int(head)
    except Exception:
        pass
    try:
        me = bot.get_me()
        return int(getattr(me, 'id', 0) or 0)
    except Exception:
        return 0

def _forward_anonymous_admin_message(msg) -> bool:
    """Telegram represents anonymous/send-as-group admins as a bot-like sender.

    Such messages are human-originated and must be eligible for configured forwarding.
    """
    try:
        sender = getattr(msg, 'from_user', None)
        if not sender or not bool(getattr(sender, 'is_bot', False)):
            return False
        username = str(getattr(sender, 'username', '') or '').lstrip('@').lower()
        if username == 'groupanonymousbot':
            return True
        sender_chat = getattr(msg, 'sender_chat', None)
        chat = getattr(msg, 'chat', None)
        if sender_chat is not None and chat is not None:
            return int(getattr(sender_chat, 'id', 0) or 0) == int(getattr(chat, 'id', 0) or 0) != 0
    except Exception:
        pass
    return False

def _forward_sender_skip_reason(msg) -> str:
    """Skip our own/other real bot messages, but allow anonymous human admins."""
    try:
        sender = getattr(msg, 'from_user', None)
        if not sender or not bool(getattr(sender, 'is_bot', False)):
            return ''
        sender_id = int(getattr(sender, 'id', 0) or 0)
        self_id = _current_bot_id_for_forwarding()
        if self_id and sender_id == self_id:
            return 'bot_sender'
        if _forward_anonymous_admin_message(msg):
            return ''
        return 'other_bot_sender'
    except Exception:
        return ''

def _forward_sender_skip_reason_raw(raw: dict) -> str:
    """Raw-payload twin of _forward_sender_skip_reason for durable recovery."""
    if not isinstance(raw, dict):
        return ''
    try:
        sender = raw.get('from') or {}
        if not isinstance(sender, dict) or not bool(sender.get('is_bot')):
            return ''
        sender_id = int(sender.get('id') or 0)
        self_id = _current_bot_id_for_forwarding()
        if self_id and sender_id == self_id:
            return 'bot_sender'
        username = str(sender.get('username') or '').lstrip('@').lower()
        if username == 'groupanonymousbot':
            return ''
        sender_chat = raw.get('sender_chat') or {}
        chat = raw.get('chat') or {}
        if isinstance(sender_chat, dict) and isinstance(chat, dict):
            sid = int(sender_chat.get('id') or 0)
            cid = int(chat.get('id') or 0)
            if sid and cid and (sid == cid):
                return ''
        return 'other_bot_sender'
    except Exception:
        return ''

def _v177_legacy_0001_schedule_forward_any_message(source_chat_id: int, msg):
    """Пересылка: порядок по исходному чату сохраняется; finance имеет приоритет.

    v121 keeps an explicit live outcome for the asynchronous worker. This prevents a
    successful handler from becoming MEGA/failed merely because forwarding was skipped
    by design or an album was still waiting for its delayed media-group flush.
    """
    try:
        sender_skip_reason = _forward_sender_skip_reason(msg)
        if sender_skip_reason:
            _forward_outcome_skip(source_chat_id, msg, sender_skip_reason)
            return
        if _forward_anonymous_admin_message(msg):
            try:
                bot_journal('anonymous_admin_forward_allowed', int(source_chat_id), f"msg={int(getattr(msg, 'message_id', 0) or 0)} sender_chat={int(getattr(getattr(msg, 'sender_chat', None), 'id', 0) or 0)}")
            except Exception:
                pass
        if getattr(msg, 'edit_date', None):
            _forward_outcome_skip(source_chat_id, msg, 'edited_source')
            return
    except Exception:
        pass
    _durable_note_forward_decision(int(source_chat_id), direct=False)
    try:
        mid = int(getattr(msg, 'message_id', 0) or 0)
        if mid:
            _forward_outcome_update(source_chat_id, mid, state='scheduled')
    except Exception:
        pass
    pipeline = globals().get('schedule_financial_forward_pipeline')
    if callable(pipeline):
        pipeline(int(source_chat_id), msg)
        return
    if not FIN_FORWARD_TASK_POOL.submit(int(source_chat_id), _forward_with_finance_priority, source_chat_id, msg):
        log_error(f'FIN-FORWARD QUEUE FULL, INLINE FALLBACK: {source_chat_id}')
        _forward_with_finance_priority(source_chat_id, msg)
try:
    _v177_legacy_0001_schedule_forward_any_message.__name__ = 'schedule_forward_any_message'
except Exception:
    pass

def schedule_propagate_edited_to_copies(msg):
    source_chat_id = int(getattr(getattr(msg, 'chat', None), 'id', 0) or 0)
    if not FORWARD_TASK_POOL.submit(source_chat_id, _forward_edit_with_finance_priority, msg):
        log_error(f'FORWARD EDIT QUEUE FULL, INLINE FALLBACK: {source_chat_id}')
        _forward_edit_with_finance_priority(msg)

def schedule_delete_forward_copies_for_source(source_chat_id: int, source_msg_id: int):
    if not FORWARD_TASK_POOL.submit(int(source_chat_id), _forward_delete_with_finance_priority, source_chat_id, source_msg_id):
        log_error(f'FORWARD DELETE QUEUE FULL, INLINE FALLBACK: {source_chat_id}')
        _forward_delete_with_finance_priority(source_chat_id, source_msg_id)
BOT_TOKEN = os.getenv('B_T', '').strip()
OWNER_ID = os.getenv('ID', '').strip()
RENDER_EXTERNAL_HOSTNAME = os.getenv('RENDER_EXTERNAL_HOSTNAME', '').strip()
_RENDER_HOST_URL = f'https://{RENDER_EXTERNAL_HOSTNAME}' if RENDER_EXTERNAL_HOSTNAME else ''
APP_URL = os.getenv('APP_URL', '').strip() or os.getenv('RENDER_EXTERNAL_URL', '').strip() or _RENDER_HOST_URL
WEBHOOK_URL = os.getenv('WEBHOOK_URL', '').strip() or APP_URL
try:
    PORT = int(os.getenv('PORT', '5000'))
except Exception:
    PORT = 5000
BACKUP_CHAT_ID = os.getenv('BACKUP_CHAT_ID', '').strip()
if not BOT_TOKEN:
    raise RuntimeError('B_T is not set')
RELEASE_SERIES = 'выс'
RELEASE_NUMBER = 262
VERSION = f'{RELEASE_SERIES}-{RELEASE_NUMBER}'
BOT_FILE_NAME = os.path.basename(__file__) if '__file__' in globals() else 'bot_v130_modular_split.py'
BOT_DISPLAY_NAME = VERSION

def _current_source_path() -> str:
    """Single-file path in legacy mode; reconstructed full source in modular mode."""
    helper = globals().get('_modular_merged_source_path')
    if callable(helper):
        try:
            return str(helper())
        except Exception:
            pass
    return os.path.abspath(__file__)

def version_animal_badge(version: str | None=None) -> str:
    """Для каждой новой версии — свой зверь и номер."""
    raw = str(version or VERSION)
    m = re.search(r'(?:^|[_-])v?(\d+)(?:\D*)$', raw, re.I)
    if m is None:
        m = re.search(r'(\d+)', raw)
    number = int(m.group(1)) if m else 0
    animals = ['🐺', '🦊', '🐯', '🐲', '🦅', '🐘', '🦉', '🐆', '🦈', '🦄', '🐻', '🦁', '🐼', '🐸', '🐙', '🦚', '🐬', '🦬', '🦏', '🐊']
    animal = animals[(number - 81) % len(animals)] if number else '🤖'
    return f'{animal}{number}' if number else animal
DEFAULT_TZ = 'America/Argentina/Buenos_Aires'
try:
    KEEP_ALIVE_INTERVAL_SECONDS = max(60, min(3600, int(os.getenv('KEEP_ALIVE_INTERVAL_SECONDS', '600') or '600')))
except Exception:
    KEEP_ALIVE_INTERVAL_SECONDS = 600
KEEP_ALIVE_ENABLED = str(os.getenv('KEEP_ALIVE_ENABLED', '1')).strip().lower() in {'1', 'true', 'yes', 'y', 'on', 'да'}
DB_FILE = os.getenv('DB_FILE', 'bot_state.sqlite3').strip() or 'bot_state.sqlite3'
DATA_FILE = 'data.json'
CSV_FILE = 'data.csv'
CSV_META_FILE = 'csv_meta.json'
UNIVERSAL_BACKUP_KIND = 'telegram_finance_bot_universal'
UNIVERSAL_BACKUP_SCHEMA_VERSION = 11

def _env_bool(name: str, default: str='0') -> bool:
    return str(os.getenv(name, default)).strip().lower() in {'1', 'true', 'yes', 'y', 'on', 'да'}
MEGA_ENABLED = _env_bool('MEGA_ENABLED', '0')
MEGA_AUTORESTORE = _env_bool('MEGA_AUTORESTORE', '1')
MEGA_EMAIL = os.getenv('MEGA_EMAIL', '').strip()
MEGA_PASSWORD = os.getenv('MEGA_PASSWORD', '').strip()
_MEGA_ROOT_RAW_V238 = str(os.getenv('MEGA_BACKUP_DIR', 'TelegramBotBackups') or 'TelegramBotBackups').strip().replace('\\', '/')
MEGA_CANONICAL_BACKUP_DIR_V238 = '/' + _MEGA_ROOT_RAW_V238.strip('/')
if MEGA_CANONICAL_BACKUP_DIR_V238 == '/':
    MEGA_CANONICAL_BACKUP_DIR_V238 = '/TelegramBotBackups'
MEGA_BACKUP_DIR = MEGA_CANONICAL_BACKUP_DIR_V238.rstrip('/')
MEGA_LEGACY_BACKUP_DIR = MEGA_BACKUP_DIR
MEGA_TARGET_BACKUP_DIR = MEGA_BACKUP_DIR
try:
    MEGA_TIMEOUT = int(os.getenv('MEGA_TIMEOUT', '120'))
except Exception:
    MEGA_TIMEOUT = 120
MEGA_LATEST_GLOBAL_NAME = os.getenv('MEGA_LATEST_GLOBAL_NAME', 'latest_global.json').strip() or 'latest_global.json'
MEGA_LOCAL_TMP_DIR = os.getenv('MEGA_LOCAL_TMP_DIR', '/tmp').strip() or '/tmp'
MEGA_CHAT_BACKUP_DIR = os.getenv('MEGA_CHAT_BACKUP_DIR', 'chats').strip().strip('/') or 'chats'
MEGA_MONTHLY_BACKUP_DIR = os.getenv('MEGA_MONTHLY_BACKUP_DIR', 'monthly').strip().strip('/') or 'monthly'
MEGA_HISTORY_BACKUP_DIR = os.getenv('MEGA_HISTORY_BACKUP_DIR', 'history').strip().strip('/') or 'history'
MEGA_DELTA_BACKUP_DIR = os.getenv('MEGA_DELTA_BACKUP_DIR', 'deltas').strip().strip('/') or 'deltas'
MEGA_TASKS_ENABLED = _env_bool('MEGA_TASKS_ENABLED', '1')
MEGA_TASK_BACKUP_DIR = os.getenv('MEGA_TASK_BACKUP_DIR', 'tasks').strip().strip('/') or 'tasks'
try:
    MEGA_TASK_DONE_KEEP = max(20, min(120, int(os.getenv('MEGA_TASK_DONE_KEEP', '30') or '30')))
except Exception:
    MEGA_TASK_DONE_KEEP = 30
try:
    MEGA_TASK_RECOVERY_LIMIT = max(20, min(2000, int(os.getenv('MEGA_TASK_RECOVERY_LIMIT', '500') or '500')))
except Exception:
    MEGA_TASK_RECOVERY_LIMIT = 500
try:
    MEGA_TASK_RECOVERY_DELAY_SECONDS = max(0.5, min(30.0, float(os.getenv('MEGA_TASK_RECOVERY_DELAY_SECONDS', '2') or '2')))
except Exception:
    MEGA_TASK_RECOVERY_DELAY_SECONDS = 2.0
try:
    MEGA_TASK_FINALIZE_RETRIES = max(1, min(5, int(os.getenv('MEGA_TASK_FINALIZE_RETRIES', '3') or '3')))
except Exception:
    MEGA_TASK_FINALIZE_RETRIES = 3
try:
    MEGA_TASK_PROCESSED_KEEP = max(100, min(5000, int(os.getenv('MEGA_TASK_PROCESSED_KEEP', '500') or '500')))
except Exception:
    MEGA_TASK_PROCESSED_KEEP = 500
try:
    MEGA_DELTA_DELAY_SECONDS = max(1.0, float(os.getenv('MEGA_DELTA_DELAY_SECONDS', '3') or '3'))
except Exception:
    MEGA_DELTA_DELAY_SECONDS = 3.0
try:
    MEGA_DELTA_PRIORITY_DELAY_SECONDS = max(1.0, float(os.getenv('MEGA_DELTA_PRIORITY_DELAY_SECONDS', '2') or '2'))
except Exception:
    MEGA_DELTA_PRIORITY_DELAY_SECONDS = 2.0
MEGA_GLOBAL_QUIET_SECONDS = 0.0
MEGA_GLOBAL_MAX_INTERVAL_SECONDS = 21600.0
try:
    MEGA_GLOBAL_HISTORY_KEEP = min(2, max(1, int(os.getenv('MEGA_GLOBAL_HISTORY_KEEP', '2') or '2')))
except Exception:
    MEGA_GLOBAL_HISTORY_KEEP = 2
try:
    MEGA_FILE_HISTORY_KEEP = min(2, max(1, int(os.getenv('MEGA_FILE_HISTORY_KEEP', '2') or '2')))
except Exception:
    MEGA_FILE_HISTORY_KEEP = 2
try:
    MEGA_DELTA_KEEP_FILES = max(120, min(2000, int(os.getenv('MEGA_DELTA_KEEP_FILES', '500') or '500')))
except Exception:
    MEGA_DELTA_KEEP_FILES = 500
try:
    MEGA_DELTA_RESTORE_LIMIT = max(50, int(os.getenv('MEGA_DELTA_RESTORE_LIMIT', '1000') or '1000'))
except Exception:
    MEGA_DELTA_RESTORE_LIMIT = 1000
try:
    MEGA_GLOBAL_MIN_SAFE_BYTES = max(2048, int(os.getenv('MEGA_GLOBAL_MIN_SAFE_BYTES', '8192') or '8192'))
except Exception:
    MEGA_GLOBAL_MIN_SAFE_BYTES = 8192
try:
    MEGA_GLOBAL_MAX_RECORD_DROP = min(0.95, max(0.05, float(os.getenv('MEGA_GLOBAL_MAX_RECORD_DROP', '0.30') or '0.30')))
except Exception:
    MEGA_GLOBAL_MAX_RECORD_DROP = 0.3
ALLOW_EMPTY_MEGA_RESTORE = _env_bool('ALLOW_EMPTY_MEGA_RESTORE', '0')
try:
    MEGA_RESTORE_DISCOVERY_RETRIES = max(1, min(6, int(os.getenv('MEGA_RESTORE_DISCOVERY_RETRIES', '3') or '3')))
except Exception:
    MEGA_RESTORE_DISCOVERY_RETRIES = 3
try:
    MEGA_RESTORE_DISCOVERY_RETRY_SECONDS = max(0.5, float(os.getenv('MEGA_RESTORE_DISCOVERY_RETRY_SECONDS', '3') or '3'))
except Exception:
    MEGA_RESTORE_DISCOVERY_RETRY_SECONDS = 3.0
RESTORE_GUARD_ACTIVE = False
RESTORE_GUARD_REASON = ''
MEGA_GLOBAL_BACKUP_LOCK = threading.RLock()
MEGA_COMMAND_LOCK = threading.RLock()
CRITICAL_DELTA_LOCK = threading.RLock()
forward_map = {}
backup_flags = {'channel': True}
restore_mode = None
_media_group_cache = {}
_media_group_timers = {}
FORWARD_MEDIA_GROUP_DELAY = 0.8
_FORWARD_OUTCOME_LOCK = threading.RLock()
_FORWARD_OUTCOMES = {}
_FORWARD_OUTCOME_MAX = 800

def _forward_outcome_key(source_chat_id: int, source_msg_id: int):
    return (int(source_chat_id), int(source_msg_id))

def _forward_outcome_prune_locked():
    if len(_FORWARD_OUTCOMES) <= _FORWARD_OUTCOME_MAX:
        return
    ordered = sorted(_FORWARD_OUTCOMES.items(), key=lambda kv: float((kv[1] or {}).get('updated_at', 0.0) or 0.0))
    for key, _item in ordered[:max(1, len(ordered) - _FORWARD_OUTCOME_MAX)]:
        _FORWARD_OUTCOMES.pop(key, None)

def _forward_outcome_update(source_chat_id: int, source_msg_id: int, state: str | None=None, dst_chat_id: int | None=None, dst_state: str | None=None, dst_msg_id: int | None=None, error: str=''):
    try:
        key = _forward_outcome_key(source_chat_id, source_msg_id)
        with _FORWARD_OUTCOME_LOCK:
            item = _FORWARD_OUTCOMES.setdefault(key, {'state': '', 'targets': {}, 'updated_at': time.time()})
            if state:
                item['state'] = str(state)
            if dst_chat_id is not None:
                dst = int(dst_chat_id)
                target = item.setdefault('targets', {}).setdefault(dst, {})
                if dst_state:
                    target['state'] = str(dst_state)
                if dst_msg_id:
                    target['dst_msg_id'] = int(dst_msg_id)
                if error:
                    target['error'] = str(error)[:500]
            item['updated_at'] = time.time()
            _forward_outcome_prune_locked()
    except Exception:
        pass

def _forward_outcome_snapshot(source_chat_id: int, source_msg_id: int) -> dict:
    try:
        key = _forward_outcome_key(source_chat_id, source_msg_id)
        with _FORWARD_OUTCOME_LOCK:
            return copy.deepcopy(_FORWARD_OUTCOMES.get(key) or {})
    except Exception:
        return {}

def _forward_outcome_skip(source_chat_id: int, msg, reason: str):
    try:
        mid = int(getattr(msg, 'message_id', 0) or 0)
        if mid:
            _forward_outcome_update(source_chat_id, mid, state=f'skip:{reason}')
            bot_journal('forward_not_expected', source_chat_id, f'msg={mid} reason={reason}')
    except Exception:
        pass
_forward_state_timer = None
_owner_json_restore_prompts = {}
_owner_json_restore_prompt_lock = threading.RLock()
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)
BOT_ERROR_LOG = deque(maxlen=200)
error_log_lock = threading.RLock()
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None, threaded=False)
app = Flask(__name__)
data = {}
finance_active_chats = set()

class SQLiteState:

    def __init__(self, path: str):
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute('PRAGMA journal_mode=WAL')
            cur.execute('PRAGMA synchronous=NORMAL')
            cur.execute('PRAGMA temp_store=FILE')
            cur.execute('PRAGMA cache_size=-4096')
            cur.execute('PRAGMA mmap_size=0')
            cur.execute('PRAGMA foreign_keys=ON')
            cur.execute('CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)')
            cur.execute('CREATE TABLE IF NOT EXISTS chats (chat_id TEXT PRIMARY KEY, v TEXT NOT NULL)')
            cur.execute('CREATE TABLE IF NOT EXISTS meta (kind TEXT NOT NULL, k TEXT NOT NULL, v TEXT NOT NULL, PRIMARY KEY(kind, k))')
            cur.execute("CREATE TABLE IF NOT EXISTS cold_fields (chat_id TEXT NOT NULL, k TEXT NOT NULL, v TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT '', PRIMARY KEY(chat_id, k))")
            self.conn.commit()

    def _dump(self, obj) -> str:
        return json.dumps(obj, ensure_ascii=False, separators=(',', ':'))

    def _load(self, raw, default=None):
        if raw is None:
            return default
        try:
            return json.loads(raw)
        except Exception:
            return default

    def get_kv(self, key: str, default=None):
        with self.lock:
            row = self.conn.execute('SELECT v FROM kv WHERE k=?', (key,)).fetchone()
        return self._load(row[0], default) if row else default

    def set_kv(self, key: str, obj):
        payload = self._dump(obj)
        with self.lock:
            self.conn.execute('INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v', (key, payload))
            self.conn.commit()

    def load_root(self):
        return self.get_kv('root', None)

    def save_root(self, obj):
        self.set_kv('root', obj)

    def load_chats(self) -> dict:
        with self.lock:
            rows = self.conn.execute('SELECT chat_id, v FROM chats').fetchall()
        out = {}
        for row in rows:
            val = self._load(row[1], {})
            if isinstance(val, dict):
                out[str(row[0])] = val
        return out

    def save_chats(self, chats: dict):
        chats = chats or {}
        with self.lock:
            existing = {str(r[0]) for r in self.conn.execute('SELECT chat_id FROM chats').fetchall()}
            for chat_id, payload in chats.items():
                self.conn.execute('INSERT INTO chats(chat_id,v) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET v=excluded.v', (str(chat_id), self._dump(payload)))
            for stale in existing - {str(k) for k in chats.keys()}:
                self.conn.execute('DELETE FROM chats WHERE chat_id=?', (stale,))
            self.conn.commit()

    def save_chat(self, chat_id, payload: dict):
        """Точечно сохраняет только один изменившийся чат."""
        with self.lock:
            self.conn.execute('INSERT INTO chats(chat_id,v) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET v=excluded.v', (str(chat_id), self._dump(payload or {})))
            self.conn.commit()

    def delete_chat(self, chat_id):
        with self.lock:
            self.conn.execute('DELETE FROM chats WHERE chat_id=?', (str(chat_id),))
            self.conn.commit()

    def get_meta(self, kind: str, key: str, default=None):
        with self.lock:
            row = self.conn.execute('SELECT v FROM meta WHERE kind=? AND k=?', (kind, key)).fetchone()
        return self._load(row[0], default) if row else default

    def set_meta(self, kind: str, key: str, obj):
        payload = self._dump(obj)
        with self.lock:
            self.conn.execute('INSERT INTO meta(kind,k,v) VALUES(?,?,?) ON CONFLICT(kind,k) DO UPDATE SET v=excluded.v', (kind, key, payload))
            self.conn.commit()

    def get_cold(self, chat_id, key: str, default=None):
        with self.lock:
            row = self.conn.execute('SELECT v FROM cold_fields WHERE chat_id=? AND k=?', (str(chat_id), str(key))).fetchone()
        return self._load(row[0], default) if row else default

    def set_cold(self, chat_id, key: str, obj):
        payload = self._dump(obj)
        stamp = datetime.now(timezone.utc).isoformat(timespec='seconds')
        with self.lock:
            self.conn.execute('INSERT INTO cold_fields(chat_id,k,v,updated_at) VALUES(?,?,?,?) ON CONFLICT(chat_id,k) DO UPDATE SET v=excluded.v,updated_at=excluded.updated_at', (str(chat_id), str(key), payload, stamp))
            self.conn.commit()

    def delete_cold(self, chat_id, key: str):
        with self.lock:
            self.conn.execute('DELETE FROM cold_fields WHERE chat_id=? AND k=?', (str(chat_id), str(key)))
            self.conn.commit()

    def cold_count(self, chat_id=None, key: str | None=None) -> int:
        sql = 'SELECT COUNT(*) FROM cold_fields'
        params = []
        where = []
        if chat_id is not None:
            where.append('chat_id=?')
            params.append(str(chat_id))
        if key is not None:
            where.append('k=?')
            params.append(str(key))
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        with self.lock:
            row = self.conn.execute(sql, tuple(params)).fetchone()
        return int(row[0] or 0) if row else 0

    def cold_keys_for_chat(self, chat_id) -> list[str]:
        with self.lock:
            rows = self.conn.execute('SELECT k FROM cold_fields WHERE chat_id=?', (str(chat_id),)).fetchall()
        return [str(r[0]) for r in rows]

    def cold_chat_ids(self, keys=None) -> list[int]:
        """Return chat IDs that have authoritative low-RAM cold fields.

        Data Constitution must enumerate these rows too: after idle eviction a finance
        chat can legitimately exist only in cold_fields and must not look like data loss.
        """
        params = []
        sql = 'SELECT DISTINCT chat_id FROM cold_fields'
        if keys:
            clean = [str(x) for x in keys if str(x)]
            if clean:
                sql += ' WHERE k IN (' + ','.join(('?' for _ in clean)) + ')'
                params.extend(clean)
        with self.lock:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
        out = []
        for row in rows:
            try:
                out.append(int(row[0]))
            except Exception:
                pass
        return sorted(set(out))

    def backup_to(self, target_path: str):
        """Consistent on-disk SQLite snapshot without materializing bot state in Python RAM."""
        target_path = str(target_path)
        os.makedirs(os.path.dirname(target_path) or '.', exist_ok=True)
        with self.lock:
            try:
                self.conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
            except Exception:
                pass
            dest = sqlite3.connect(target_path)
            try:
                self.conn.backup(dest, pages=128, sleep=0.01)
                dest.commit()
            finally:
                dest.close()
        return target_path

    def replace_database(self, source_path: str):
        """Replace ephemeral working DB with a restored MEGA snapshot and reopen connection."""
        source_path = str(source_path)
        if not os.path.exists(source_path):
            raise FileNotFoundError(source_path)
        with self.lock:
            try:
                self.conn.close()
            except Exception:
                pass
            for suffix in ('', '-wal', '-shm'):
                try:
                    if os.path.exists(self.path + suffix):
                        os.remove(self.path + suffix)
                except Exception:
                    pass
            shutil.copy2(source_path, self.path)
            self.conn = sqlite3.connect(self.path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self._init_db()
SQLITE = SQLiteState(DB_FILE)
LOWRAM_ENABLED = _env_bool('LOWRAM_ENABLED', '1')
LOWRAM_COLD_KEYS = {'records', 'daily_records', 'daily_records_by_date', 'ars_records', 'ars_daily_records', 'ars_daily_records_by_date', 'usd_records', 'usd_daily_records', 'usd_daily_records_by_date', 'secret_messages'}
LOWRAM_LIST_KEYS = {'records', 'ars_records', 'usd_records', 'secret_messages'}
LOWRAM_DB_REMOTE_DIR_NAME = 'database'
LOWRAM_DB_LATEST_NAME = 'latest_bot_state.sqlite3.gz'
LOWRAM_DB_HISTORY_KEEP = max(2, min(30, int(os.getenv('LOWRAM_DB_HISTORY_KEEP', '6') or '6')))
LOWRAM_LEGACY_GLOBAL_JSON = _env_bool('LOWRAM_LEGACY_GLOBAL_JSON', '0')
_LOWRAM_DB_RESTORED_THIS_BOOT = False
_LOWRAM_DB_RESTORE_DETAIL = ''
_LOWRAM_LOCK = threading.RLock()
_LOWRAM_STATS = {'cold_loads': 0, 'cold_saves': 0, 'cold_evictions': 0, 'db_snapshots': 0, 'db_snapshot_errors': 0, 'db_restores': 0, 'last_snapshot_at': '', 'last_restore_at': '', 'last_error': ''}

def _lowram_default_for_key(key: str):
    return [] if str(key) in LOWRAM_LIST_KEYS else {}

def _lowram_touch(chat_id: int, key: str):
    try:
        _LOWRAM_STATS['last_access'] = now_local().isoformat(timespec='seconds')
        _LOWRAM_STATS['last_chat'] = int(chat_id)
        _LOWRAM_STATS['last_key'] = str(key)
    except Exception:
        pass

class ColdChatStore(dict):
    """dict-compatible chat state with lazy large fields backed by SQLite."""

    def __init__(self, chat_id: int, initial=None):
        super().__init__(initial or {})
        self._chat_id = int(chat_id)
        self._cold_loaded = {k for k in LOWRAM_COLD_KEYS if dict.__contains__(self, k)}

    def _ensure_cold(self, key: str):
        key = str(key)
        if not LOWRAM_ENABLED or key not in LOWRAM_COLD_KEYS:
            return
        if dict.__contains__(self, key):
            _lowram_touch(self._chat_id, key)
            return
        value = SQLITE.get_cold(self._chat_id, key, _lowram_default_for_key(key))
        if value is None:
            value = _lowram_default_for_key(key)
        dict.__setitem__(self, key, value)
        self._cold_loaded.add(key)
        with _LOWRAM_LOCK:
            _LOWRAM_STATS['cold_loads'] += 1
        _lowram_touch(self._chat_id, key)

    def __getitem__(self, key):
        self._ensure_cold(key)
        return dict.__getitem__(self, key)

    def get(self, key, default=None):
        self._ensure_cold(key)
        if dict.__contains__(self, key):
            return dict.get(self, key)
        return default

    def setdefault(self, key, default=None):
        self._ensure_cold(key)
        if dict.__contains__(self, key):
            return dict.__getitem__(self, key)
        if default is None and str(key) in LOWRAM_COLD_KEYS:
            default = _lowram_default_for_key(str(key))
        dict.__setitem__(self, key, default)
        if str(key) in LOWRAM_COLD_KEYS:
            self._cold_loaded.add(str(key))
            _lowram_touch(self._chat_id, str(key))
        return default

    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        if str(key) in LOWRAM_COLD_KEYS:
            self._cold_loaded.add(str(key))
            _lowram_touch(self._chat_id, str(key))

def _lowram_wrap_store(chat_id, store):
    if isinstance(store, ColdChatStore):
        return store
    return ColdChatStore(int(chat_id), store if isinstance(store, dict) else {})

def _lowram_store_meta_payload(store: dict) -> dict:
    return {str(k): v for k, v in dict.items(store) if str(k) not in LOWRAM_COLD_KEYS}

def _lowram_rebuild_daily(records):
    daily = {}
    for rec in records or []:
        if not isinstance(rec, dict):
            continue
        try:
            dk = _record_day_key(rec) if '_record_day_key' in globals() else str(rec.get('day_key') or '')
        except Exception:
            dk = str(rec.get('day_key') or '')
        if dk:
            rec['day_key'] = dk
            daily.setdefault(str(dk), []).append(rec)
    return daily

def _lowram_flush_chat(chat_id: int, store: dict | None=None, evict: bool=False):
    if not LOWRAM_ENABLED:
        return
    try:
        cid = int(chat_id)
    except Exception:
        return
    store = store if isinstance(store, dict) else (data.get('chats', {}) or {}).get(str(cid)) if isinstance(data, dict) else None
    if not isinstance(store, dict):
        return
    for rec_key, daily_key in (('records', 'daily_records'), ('ars_records', 'ars_daily_records'), ('usd_records', 'usd_daily_records')):
        if dict.__contains__(store, rec_key):
            records = dict.__getitem__(store, rec_key) or []
            daily = _lowram_rebuild_daily(records)
            dict.__setitem__(store, daily_key, daily)
            if isinstance(store, ColdChatStore):
                store._cold_loaded.add(daily_key)
    for key in LOWRAM_COLD_KEYS:
        if dict.__contains__(store, key):
            SQLITE.set_cold(cid, key, dict.__getitem__(store, key))
            with _LOWRAM_LOCK:
                _LOWRAM_STATS['cold_saves'] += 1
    if evict:
        removed = 0
        for key in list(LOWRAM_COLD_KEYS):
            if dict.__contains__(store, key):
                dict.pop(store, key, None)
                removed += 1
        if isinstance(store, ColdChatStore):
            store._cold_loaded.clear()
        if removed:
            with _LOWRAM_LOCK:
                _LOWRAM_STATS['cold_evictions'] += 1

def _lowram_memory_snapshot() -> dict:
    """Small, dependency-safe RAM snapshot used by LOW-RAM cleanup.

    The runtime watcher helper is defined later in the file, so resolve it dynamically.
    This avoids the v114/v115 NameError that disabled post-update GC and flooded the journal.
    """
    for name in ('_runtime_memory_stats', '_memory_usage_snapshot'):
        fn = globals().get(name)
        if callable(fn):
            try:
                snap = fn()
                if isinstance(snap, dict):
                    return snap
            except Exception:
                pass
    return {}

def _lowram_release_chat(chat_id):
    """Called only after the update/finalizer finished, so temporary history can leave RAM."""
    if not LOWRAM_ENABLED or chat_id is None:
        return
    try:
        with data_lock:
            store = (data.get('chats', {}) or {}).get(str(int(chat_id)))
            if isinstance(store, dict):
                _lowram_flush_chat(int(chat_id), store, evict=True)
                SQLITE.save_chat(int(chat_id), _lowram_store_meta_payload(store))
        if _lowram_memory_snapshot().get('rss_mb', 0) >= 320:
            import gc
            gc.collect()
    except Exception as exc:
        with _LOWRAM_LOCK:
            _LOWRAM_STATS['last_error'] = str(exc)[:300]
        log_error(f'LOWRAM release chat={chat_id}: {exc}')

def _lowram_prepare_loaded_data(d: dict, migrate_existing: bool=True) -> dict:
    if not LOWRAM_ENABLED or not isinstance(d, dict):
        return d
    chats = d.setdefault('chats', {})
    for cid_s, raw in list(chats.items()):
        try:
            cid = int(cid_s)
        except Exception:
            continue
        raw = raw if isinstance(raw, dict) else {}
        if migrate_existing:
            for key in LOWRAM_COLD_KEYS:
                if key in raw:
                    SQLITE.set_cold(cid, key, raw.get(key))
                    raw.pop(key, None)
        chats[str(cid)] = _lowram_wrap_store(cid, raw)
    return d

def _lowram_materialize_chat_snapshot(chat_id: int, store: dict | None=None) -> dict:
    """Plain JSON-ready chat snapshot. Loads only one chat's cold fields at a time."""
    cid = int(chat_id)
    store = store if isinstance(store, dict) else (data.get('chats', {}) or {}).get(str(cid)) or {}
    snap = _lowram_store_meta_payload(store)
    for key in LOWRAM_COLD_KEYS:
        if dict.__contains__(store, key):
            value = dict.__getitem__(store, key)
        else:
            value = SQLITE.get_cold(cid, key, _lowram_default_for_key(key))
        if value not in (None, [], {}):
            snap[key] = value
    return snap

def _lowram_flush_all_hot(evict: bool=False):
    if not LOWRAM_ENABLED or not isinstance(data, dict):
        return
    chats = data.get('chats', {}) or {}
    with data_lock:
        meta_chats = {}
        for cid_s, store in list(chats.items()):
            try:
                cid = int(cid_s)
            except Exception:
                continue
            if isinstance(store, dict):
                _lowram_flush_chat(cid, store, evict=evict)
                meta_chats[str(cid)] = _lowram_store_meta_payload(store)
        if meta_chats:
            SQLITE.save_chats(meta_chats)
        SQLITE.save_root(_sqlite_pack_root(data))

def lowram_status_text() -> str:
    mem = _lowram_memory_snapshot()
    with _LOWRAM_LOCK:
        st = dict(_LOWRAM_STATS)
    loaded = 0
    try:
        for store in ((data or {}).get('chats', {}) or {}).values():
            if isinstance(store, dict):
                loaded += sum((1 for k in LOWRAM_COLD_KEYS if dict.__contains__(store, k)))
    except Exception:
        pass
    return f"LOW-RAM: {('✅ ВКЛ' if LOWRAM_ENABLED else '⬜ ВЫКЛ')} | RAM {mem.get('rss_mb', '?')} MB\nCold fields loaded now: {loaded}; loads={st.get('cold_loads', 0)} saves={st.get('cold_saves', 0)} evictions={st.get('cold_evictions', 0)}\nSQLite cold rows: {SQLITE.cold_count()} | DB snapshots={st.get('db_snapshots', 0)} restores={st.get('db_restores', 0)}\nПоследний DB snapshot: {st.get('last_snapshot_at') or '—'}; restore: {st.get('last_restore_at') or '—'}\nОшибка: {st.get('last_error') or 'нет'}"

def _sqlite_pack_root(d: dict) -> dict:
    return {k: v for k, v in (d or {}).items() if k != 'chats'}

def _sqlite_unpack_data(root: dict | None, chats: dict | None) -> dict:
    d = default_data()
    if isinstance(root, dict):
        for k, v in root.items():
            d[k] = v
    d['chats'] = chats if isinstance(chats, dict) else {}
    return d

def _import_legacy_global_json_to_db(path: str=DATA_FILE, force: bool=False) -> bool:
    root = SQLITE.load_root()
    chats = SQLITE.load_chats()
    if not force and (root is not None or chats):
        return False
    payload = _load_json(path, None)
    if not isinstance(payload, dict):
        return False
    SQLITE.save_root(_sqlite_pack_root(payload))
    SQLITE.save_chats(payload.get('chats', {}) or {})
    legacy_csv_meta = _load_json(CSV_META_FILE, None)
    if isinstance(legacy_csv_meta, dict):
        SQLITE.set_meta('csv_meta', 'main', legacy_csv_meta)
    legacy_backup_meta = _load_json(CHAT_BACKUP_META_FILE, None)
    if isinstance(legacy_backup_meta, dict):
        SQLITE.set_meta('chat_backup_meta', 'main', legacy_backup_meta)
    return True

def _v177_legacy_0002_log_info(msg: str):
    logger.info(msg)
try:
    _v177_legacy_0002_log_info.__name__ = 'log_info'
except Exception:
    pass

def _v177_legacy_0003_log_error(msg: str):
    logger.error(msg)
    try:
        if 'bot_journal' in globals():
            bot_journal('error', None, str(msg), 'ERROR')
    except Exception:
        pass
    try:
        with error_log_lock:
            BOT_ERROR_LOG.append({'ts': now_local().strftime('%Y-%m-%d %H:%M:%S') if 'now_local' in globals() else time.strftime('%Y-%m-%d %H:%M:%S'), 'msg': str(msg)[:900]})
    except Exception:
        pass
try:
    _v177_legacy_0003_log_error.__name__ = 'log_error'
except Exception:
    pass

def get_recent_errors(limit: int=20):
    try:
        with error_log_lock:
            return list(BOT_ERROR_LOG)[-int(limit):]
    except Exception:
        return []
BOT_JOURNAL_MAX = int(os.getenv('BOT_JOURNAL_MAX', '1600') or '1600')
BOT_JOURNAL_FILE = os.getenv('BOT_JOURNAL_FILE', 'bot_journal.jsonl').strip() or 'bot_journal.jsonl'
BOT_ACTION_LOG = deque(maxlen=BOT_JOURNAL_MAX)
bot_journal_lock = threading.RLock()
_JOURNAL_FILE_LOCK = threading.RLock()
try:
    BOT_JOURNAL_LOCAL_MAX_BYTES = max(2 * 1024 * 1024, min(64 * 1024 * 1024, int(os.getenv('BOT_JOURNAL_LOCAL_MAX_BYTES', str(16 * 1024 * 1024)) or str(16 * 1024 * 1024))))
except Exception:
    BOT_JOURNAL_LOCAL_MAX_BYTES = 16 * 1024 * 1024
try:
    BOT_JOURNAL_LOCAL_KEEP_FILES = max(1, min(8, int(os.getenv('BOT_JOURNAL_LOCAL_KEEP_FILES', '3') or '3')))
except Exception:
    BOT_JOURNAL_LOCAL_KEEP_FILES = 3
BOT_JOURNAL_DURABLE_ENABLED = str(os.getenv('BOT_JOURNAL_DURABLE_ENABLED', '1') or '1').strip().lower() not in {'0', 'false', 'no', 'off'}
BOT_CRITICAL_JOURNAL_DURABLE_ENABLED = str(os.getenv('BOT_CRITICAL_JOURNAL_DURABLE_ENABLED', '1') or '1').strip().lower() not in {'0', 'false', 'no', 'off'}
try:
    BOT_JOURNAL_DURABLE_FLUSH_SECONDS = max(60.0, min(1800.0, float(os.getenv('BOT_JOURNAL_DURABLE_FLUSH_SECONDS', '600') or '600')))
except Exception:
    BOT_JOURNAL_DURABLE_FLUSH_SECONDS = 600.0
try:
    BOT_JOURNAL_DURABLE_FLUSH_ROWS = max(100, min(5000, int(os.getenv('BOT_JOURNAL_DURABLE_FLUSH_ROWS', '800') or '800')))
except Exception:
    BOT_JOURNAL_DURABLE_FLUSH_ROWS = 800
try:
    BOT_JOURNAL_DURABLE_REMOTE_KEEP = max(200, min(3000, int(os.getenv('BOT_JOURNAL_DURABLE_REMOTE_KEEP', '360') or '360')))
except Exception:
    BOT_JOURNAL_DURABLE_REMOTE_KEEP = 360
try:
    BOT_JOURNAL_DURABLE_RESTORE_FILES = max(10, min(200, int(os.getenv('BOT_JOURNAL_DURABLE_RESTORE_FILES', '80') or '80')))
except Exception:
    BOT_JOURNAL_DURABLE_RESTORE_FILES = 80
_JOURNAL_DURABLE_LOCK = threading.RLock()
_JOURNAL_DURABLE_BUFFER = []
_JOURNAL_DURABLE_SEQ = 0
_JOURNAL_DURABLE_THREAD_STARTED = False
_JOURNAL_DURABLE_STATS = {'uploaded_chunks': 0, 'uploaded_rows': 0, 'upload_errors': 0, 'uploaded_payload_bytes': 0, 'raw_estimated_bytes': 0, 'restored_chunks': 0, 'restored_rows': 0, 'last_upload_at': '', 'last_upload_file': '', 'last_error': ''}
try:
    BOT_JOURNAL_ERROR_FLUSH_COOLDOWN_SECONDS = max(60.0, min(7200.0, float(os.getenv('BOT_JOURNAL_ERROR_FLUSH_COOLDOWN_SECONDS', '1800') or '1800')))
except Exception:
    BOT_JOURNAL_ERROR_FLUSH_COOLDOWN_SECONDS = 1800.0
_JOURNAL_ERROR_FLUSH_LOCK = threading.RLock()
_JOURNAL_ERROR_FLUSH_LAST = {}
_JOURNAL_DURABLE_STATS.update({'immediate_error_flushes': 0, 'suppressed_immediate_error_flushes': 0, 'last_immediate_error_at': ''})

def _journal_error_flush_fingerprint(action: str, chat_id, level: str, detail) -> str:
    """Stable network-flush fingerprint without weakening the full local journal."""
    base = {'action': str(action or '')[:160], 'chat_id': str(chat_id or ''), 'level': str(level or '').upper()}
    text = str(detail or '')
    try:
        payload = json.loads(text)
    except Exception:
        payload = None
    if isinstance(payload, dict):
        stable = {}
        for key in ('method', 'purpose', 'error', 'reason', 'event', 'from_marker', 'to_marker', 'kind'):
            value = payload.get(key)
            if value not in (None, ''):
                stable[key] = str(value)[:700]
        if stable:
            base['detail'] = stable
        else:
            text = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    if 'detail' not in base:
        normalized = text[:1200]
        normalized = re.sub('"request_id"\\s*:\\s*"w?\\d+"', '"request_id":"<id>"', normalized, flags=re.I)
        normalized = re.sub('\\bw\\d+\\b', '<request>', normalized)
        normalized = re.sub('\\b[0-9a-f]{16,64}\\b', '<hash>', normalized, flags=re.I)
        normalized = re.sub('(?<![A-Za-z])\\d{6,}(?![A-Za-z])', '<n>', normalized)
        base['detail'] = normalized
    raw = json.dumps(base, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()

def _journal_should_accelerate_error_flush(action: str, chat_id, level: str, detail) -> bool:
    fp = _journal_error_flush_fingerprint(action, chat_id, level, detail)
    now_m = time.monotonic()
    cooldown = float(BOT_JOURNAL_ERROR_FLUSH_COOLDOWN_SECONDS)
    with _JOURNAL_ERROR_FLUSH_LOCK:
        last = float(_JOURNAL_ERROR_FLUSH_LAST.get(fp, 0.0) or 0.0)
        if last and now_m - last < cooldown:
            _JOURNAL_DURABLE_STATS['suppressed_immediate_error_flushes'] = int(_JOURNAL_DURABLE_STATS.get('suppressed_immediate_error_flushes', 0) or 0) + 1
            return False
        _JOURNAL_ERROR_FLUSH_LAST[fp] = now_m
        if len(_JOURNAL_ERROR_FLUSH_LAST) > 512:
            cutoff = now_m - max(cooldown * 2.0, 3600.0)
            stale = [k for k, v in _JOURNAL_ERROR_FLUSH_LAST.items() if float(v or 0.0) < cutoff]
            for key in stale[:384]:
                _JOURNAL_ERROR_FLUSH_LAST.pop(key, None)
        _JOURNAL_DURABLE_STATS['immediate_error_flushes'] = int(_JOURNAL_DURABLE_STATS.get('immediate_error_flushes', 0) or 0) + 1
        _JOURNAL_DURABLE_STATS['last_immediate_error_at'] = _journal_ts() if '_journal_ts' in globals() else datetime.now().isoformat(timespec='seconds')
        return True
JOURNAL_V208_SETTINGS_KEY = 'diagnostic_journal_v208'
JOURNAL_V208_INTERVAL_CHOICES = (300, 600, 900, 1800)

def _journal_v208_settings(create: bool=True) -> dict:
    try:
        gs = data.setdefault('_global_settings', {}) if create else data.get('_global_settings') or {}
    except Exception:
        return {}
    row = gs.get(JOURNAL_V208_SETTINGS_KEY)
    if not isinstance(row, dict):
        if not create:
            return {}
        row = {}
        gs[JOURNAL_V208_SETTINGS_KEY] = row
    row.setdefault('compact_remote_enabled', True)
    row.setdefault('flush_seconds', 600)
    row.setdefault('schema', 1)
    return row

def journal_compact_remote_enabled() -> bool:
    try:
        return bool(_journal_v208_settings(True).get('compact_remote_enabled', True))
    except Exception:
        return True

def journal_compact_remote_effective_enabled() -> bool:
    configured = journal_compact_remote_enabled()
    try:
        gate = globals().get('v176_process_enabled')
        if callable(gate):
            return bool(configured and gate('journal_mega'))
    except Exception:
        pass
    return bool(configured)

def set_journal_compact_remote_enabled(enabled: bool) -> bool:
    row = _journal_v208_settings(True)
    row['compact_remote_enabled'] = bool(enabled)
    try:
        save_data(data, root_only=True)
    except TypeError:
        try:
            save_data(data)
        except Exception:
            pass
    except Exception:
        pass
    try:
        fn = globals().get('schedule_delta_backup')
        if callable(fn):
            fn(int(globals().get('OWNER_ID') or 0) or None, delay=0.8, reason='journal_compact_remote_toggle')
    except Exception:
        pass
    return bool(enabled)

def journal_compact_interval_seconds() -> int:
    try:
        value = int(_journal_v208_settings(True).get('flush_seconds', 600) or 600)
    except Exception:
        value = 600
    return min(JOURNAL_V208_INTERVAL_CHOICES, key=lambda x: abs(int(x) - int(value)))

def set_journal_compact_interval_seconds(seconds: int) -> int:
    try:
        value = int(seconds)
    except Exception:
        value = 600
    value = min(JOURNAL_V208_INTERVAL_CHOICES, key=lambda x: abs(int(x) - int(value)))
    _journal_v208_settings(True)['flush_seconds'] = int(value)
    try:
        save_data(data, root_only=True)
    except TypeError:
        try:
            save_data(data)
        except Exception:
            pass
    except Exception:
        pass
    try:
        fn = globals().get('schedule_delta_backup')
        if callable(fn):
            fn(int(globals().get('OWNER_ID') or 0) or None, delay=0.8, reason='journal_compact_interval')
    except Exception:
        pass
    try:
        sched = globals().get('DELAYED_SCHEDULER')
        if sched is not None:
            sched.cancel('journal-durable-tick')
            sched.schedule('journal-durable-tick', float(value), globals().get('_journal_durable_tick'))
    except Exception:
        pass
    return int(value)

def journal_v208_apply_defaults() -> bool:
    """One-time v208 migration: restore full local diagnostics + compact durable MEGA batches."""
    try:
        gs = data.setdefault('_global_settings', {})
        if bool(gs.get('journal_v208_full_default_applied')):
            return False
        gs['bot_journal_enabled'] = True
        gs.setdefault('bot_journal_verbose_telegram', False)
        cfg = _journal_v208_settings(True)
        cfg['compact_remote_enabled'] = True
        cfg['flush_seconds'] = 600
        root = gs.setdefault('process_control_v176', {})
        for code in ('btn_chain', 'btn_press', 'win_journal', 'win_diag', 'journal_mega'):
            root[code] = True
        gs['journal_v208_full_default_applied'] = True
        gs['journal_v208_applied_at'] = _journal_ts()
        try:
            save_data(data, root_only=True)
        except TypeError:
            try:
                save_data(data)
            except Exception:
                pass
        except Exception:
            pass
        return True
    except Exception:
        return False

def _journal_ts() -> str:
    try:
        return now_local().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    except Exception:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

def is_journal_registration_enabled() -> bool:
    """v208: полный диагностический action-journal включён по умолчанию; владелец может выключить вручную."""
    try:
        d = globals().get('data')
        if isinstance(d, dict):
            gs = d.setdefault('_global_settings', {})
            if not bool(gs.get('journal_v208_full_default_applied')):
                return True
            return bool(gs.get('bot_journal_enabled', True))
    except Exception:
        pass
    return True

def set_journal_registration_enabled(enabled: bool):
    try:
        d = globals().get('data')
        if isinstance(d, dict):
            gs = d.setdefault('_global_settings', {})
            gs['bot_journal_enabled'] = bool(enabled)
            gs['journal_v208_full_default_applied'] = True
            if 'save_data' in globals():
                save_data(d)
    except Exception:
        pass

def toggle_journal_registration() -> bool:
    new_value = not is_journal_registration_enabled()
    set_journal_registration_enabled(new_value)
    return new_value

def journal_toggle_label() -> str:
    return '✅ Полный журнал ВКЛ' if is_journal_registration_enabled() else '⬜ Полный журнал ВЫКЛ'

def is_chat_journal_enabled(chat_id: int) -> bool:
    try:
        store = get_chat_store(int(chat_id))
        return bool(store.setdefault('settings', {}).get('journal_enabled', False))
    except Exception:
        return False

def set_chat_journal_enabled(chat_id: int, enabled: bool):
    store = get_chat_store(int(chat_id))
    store.setdefault('settings', {})['journal_enabled'] = bool(enabled)
    save_data(data, chat_ids=[int(chat_id)])
    schedule_config_backup_for_chats(int(chat_id))

def toggle_chat_journal(chat_id: int) -> bool:
    new_value = not is_chat_journal_enabled(int(chat_id))
    set_chat_journal_enabled(int(chat_id), new_value)
    return new_value

def chat_journal_toggle_label(chat_id: int, short: bool=False) -> str:
    enabled = is_chat_journal_enabled(int(chat_id))
    if short:
        return '✅ 📓' if enabled else '⬜ 📓'
    return '✅ Журнал чата ВКЛ' if enabled else '⬜ Журнал чата ВЫКЛ'

def journal_should_record(chat_id=None) -> bool:
    if is_journal_registration_enabled():
        return True
    if chat_id is None:
        return False
    return is_chat_journal_enabled(int(chat_id))
_BASE_BOT_BEHAVIOR_PROFILES = {'v97_current': {'title': 'v97 Все правки чата / USD v93 сохранён', 'ui_edit_interval': 0.03, 'fast_tg_gap': 0.01, 'info_layout': 'v87', 'per_chat_journal': True, 'mega_priority': True, 'keepalive_menu': True, 'article_buttons': False, 'financial_value_buttons': True, 'financial_buttons_per_row': 1, 'gomonk_wallets': True, 'remaining_window': True, 'usd_categories': True, 'daily_usd': True, 'forward_copy_edit': True, 'usd_transactions': True, 'description': 'v93 USD-транзакции + все исправления из текущего чата до отдельной команды восстановления USD-кнопки.'}, 'v93_current': {'title': 'v93 USD / 💰Перес редактирование', 'ui_edit_interval': 0.03, 'fast_tg_gap': 0.01, 'info_layout': 'v87', 'per_chat_journal': True, 'mega_priority': True, 'keepalive_menu': True, 'article_buttons': False, 'financial_value_buttons': True, 'financial_buttons_per_row': 1, 'gomonk_wallets': True, 'remaining_window': True, 'usd_categories': True, 'daily_usd': True, 'forward_copy_edit': True, 'usd_transactions': True, 'description': 'v92 + безопасный отдельный учёт USD-транзакций и улучшенное окно редактирования бот-копии.'}, 'v92_current': {'title': 'v92 💰Перес / редактирование копий', 'ui_edit_interval': 0.03, 'fast_tg_gap': 0.01, 'info_layout': 'v87', 'per_chat_journal': True, 'mega_priority': True, 'keepalive_menu': True, 'article_buttons': False, 'financial_value_buttons': True, 'financial_buttons_per_row': 1, 'gomonk_wallets': True, 'remaining_window': True, 'usd_categories': True, 'daily_usd': True, 'forward_copy_edit': True, 'usd_transactions': True, 'description': 'v91 + режим 💰Перес: обычно / кнопка / слеш для редактирования бот-копии и связанной финансовой записи.'}, 'v91_current': {'title': 'v91 Статьи / Excel стат', 'ui_edit_interval': 0.03, 'fast_tg_gap': 0.01, 'info_layout': 'v87', 'per_chat_journal': True, 'mega_priority': True, 'keepalive_menu': True, 'article_buttons': False, 'financial_value_buttons': True, 'financial_buttons_per_row': 1, 'gomonk_wallets': True, 'remaining_window': True, 'usd_categories': True, 'daily_usd': True, 'description': 'Текущая версия: v90 delta/snapshots + порядок статей, сортировка ПРОЧЕЕ, Excel стат и компактная история MEGA.'}, 'v90_current': {'title': 'v90 Delta / snapshots', 'ui_edit_interval': 0.03, 'fast_tg_gap': 0.01, 'info_layout': 'v87', 'per_chat_journal': True, 'mega_priority': True, 'keepalive_menu': True, 'article_buttons': False, 'financial_value_buttons': True, 'financial_buttons_per_row': 1, 'gomonk_wallets': True, 'remaining_window': True, 'usd_categories': True, 'daily_usd': True, 'description': 'Текущая версия: быстрые immutable delta, редкие full snapshots, безопасные файлы чатов и восстановление global + delta.'}, 'v88_current': {'title': 'v88 Чистые статьи / полная валюта', 'ui_edit_interval': 0.03, 'fast_tg_gap': 0.01, 'info_layout': 'v87', 'per_chat_journal': True, 'mega_priority': True, 'keepalive_menu': True, 'article_buttons': False, 'financial_value_buttons': True, 'financial_buttons_per_row': 1, 'gomonk_wallets': True, 'remaining_window': True, 'usd_categories': True, 'daily_usd': True, 'description': 'Текущая версия: статьи без @имени бота и полноценные ARS / ARS-USD / USD во всех окнах статей.'}, 'v87_current': {'title': 'v87 Валюты / быстрый возврат', 'ui_edit_interval': 0.03, 'fast_tg_gap': 0.01, 'info_layout': 'v87', 'per_chat_journal': True, 'mega_priority': True, 'keepalive_menu': True, 'article_buttons': False, 'financial_value_buttons': True, 'financial_buttons_per_row': 1, 'gomonk_wallets': True, 'remaining_window': True, 'usd_categories': True, 'daily_usd': True, 'description': 'Текущая версия: ARS / ARS-USD / USD, быстрый возврат в основное окно и навигация Ф91.'}, 'v86_current': {'title': 'v86 Левые фин-кнопки / USD', 'ui_edit_interval': 0.05, 'fast_tg_gap': 0.015, 'info_layout': 'v86', 'per_chat_journal': True, 'mega_priority': True, 'keepalive_menu': True, 'article_buttons': False, 'financial_value_buttons': True, 'financial_buttons_per_row': 1, 'gomonk_wallets': True, 'remaining_window': True, 'usd_categories': True, 'daily_usd': True, 'description': 'Текущая версия: фин-кнопки по одной строке со сдвигом влево, гомонки и USD в окне дня.'}, 'v85_current': {'title': 'v85 Гомонки / USD', 'ui_edit_interval': 0.05, 'fast_tg_gap': 0.015, 'info_layout': 'v85', 'per_chat_journal': True, 'mega_priority': True, 'keepalive_menu': True, 'article_buttons': False, 'financial_value_buttons': True, 'financial_buttons_per_row': 1, 'gomonk_wallets': True, 'remaining_window': True, 'usd_categories': True, 'daily_usd': False, 'description': 'Прежняя v85: быстрые кнопки, финансы по одной в ряд, гомонки, остатки после расходов и USD.'}, 'v84_current': {'title': 'v84 Фин-кнопки', 'ui_edit_interval': 0.2, 'fast_tg_gap': 0.05, 'info_layout': 'v84', 'per_chat_journal': True, 'mega_priority': True, 'keepalive_menu': True, 'article_buttons': False, 'financial_value_buttons': True, 'financial_buttons_per_row': 2, 'gomonk_wallets': False, 'remaining_window': False, 'usd_categories': False, 'daily_usd': False, 'description': 'Прежняя v84: финансовые записи-кнопки по две в ряд.'}, 'v83_flexible': {'title': 'v83 Гибкая', 'ui_edit_interval': 0.2, 'fast_tg_gap': 0.05, 'info_layout': 'v83', 'per_chat_journal': True, 'mega_priority': True, 'keepalive_menu': True, 'article_buttons': True, 'financial_value_buttons': False, 'financial_buttons_per_row': 0, 'gomonk_wallets': False, 'remaining_window': False, 'usd_categories': False, 'daily_usd': False, 'description': 'Поведение прежней v83: индивидуальные журналы, keep-alive и исторический режим статей-кнопок.'}, 'v82_stable': {'title': 'v82 Стабильная', 'ui_edit_interval': 0.35, 'fast_tg_gap': 0.08, 'info_layout': 'v82', 'per_chat_journal': False, 'mega_priority': True, 'keepalive_menu': False, 'article_buttons': False, 'financial_value_buttons': False, 'financial_buttons_per_row': 0, 'gomonk_wallets': False, 'remaining_window': False, 'usd_categories': False, 'daily_usd': False, 'description': 'Интерфейс и набор кнопок v82: универсальный MEGA-бэкап без функций v83/v84.'}, 'v81_compatible': {'title': 'v81 Совместимость', 'ui_edit_interval': 1.15, 'fast_tg_gap': 0.2, 'info_layout': 'v81', 'per_chat_journal': False, 'mega_priority': False, 'keepalive_menu': False, 'article_buttons': False, 'financial_value_buttons': False, 'financial_buttons_per_row': 0, 'gomonk_wallets': False, 'remaining_window': False, 'usd_categories': False, 'daily_usd': False, 'description': 'Интерфейс и осторожное поведение v81 без новых кнопок; выбор версии остаётся доступен.'}}

def _modern_behavior_profile(title: str, description: str) -> dict:
    cfg = dict(_BASE_BOT_BEHAVIOR_PROFILES['v97_current'])
    cfg.update({'title': str(title), 'description': str(description), 'info_layout': 'v87'})
    return cfg
_MODERN_BEHAVIOR_PROFILES = {'v148_current': _modern_behavior_profile('v148 Пространства / изоляция', 'Многоконтурная изоляция чатов, пользователей, настроек, напоминаний, финансов и пересылок с владельцем платформы и владельцами пространств.'), 'v131_current': _modern_behavior_profile('v131 Modular stability / instant 💰Перес', 'Модульный контроль версий, мгновенный слеш 💰Перес для новых текстовых фин-копий, safe supergroup migration witness и защита длинных окон статей.'), 'v130_current': _modern_behavior_profile('v130 Modular split / v129 behavior', 'Физически разделён на модули без изменения бизнес-логики v129; Google existing Sheet / Notes / stability сохранены.'), 'v129_current': _modern_behavior_profile('v129 Google existing Sheet / Notes / stability', 'Google Sheets экспорт пишет в заранее расшаренную таблицу владельца, создавая отдельную вкладку с native Notes; Ф40 защищён от слишком длинных сообщений.'), 'v128_current': _modern_behavior_profile('v128 Google Sheets Notes / Gomonk fix', 'Нативные примечания Google Sheets для Excel статей и исправление кнопки Гомонковые во всех современных профилях.'), 'v127_current': _modern_behavior_profile('v127 Excel Notes exact validation', 'Исправление ложной проверки expected/actual в Excel статьи и проверка текста каждого Примечания внутри XLSX.'), 'v126_current': _modern_behavior_profile('v126 Кнопки / Excel Примечания', 'Аудит callback-кнопок, channel-safe вставка без Telegram 400 и Excel статьи с примечаниями без автора/современных комментариев.'), 'v125_current': _modern_behavior_profile('v125 Быстрый 💰Перес / Excel Примечания', '💰Перес обновляет только свежие копии за 3 дня без длинной очереди; режим Excel глобальный, Примечания отделены от Комментариев.'), 'v124_current': _modern_behavior_profile('v124 Global 💰Перес / версии / файлы', 'Глобальный 💰Перес с ретро-обновлением старых копий, постраничный Ф132 и загрузки исходника/журнала в Ф89.'), 'v123_current': _modern_behavior_profile('v123 Edit consistency / 💰Перес safe', 'Единая логика редактирования, exact edit witnesses и безопасное завершение 40-секундного окна 💰Перес.'), 'v122_current': _modern_behavior_profile('v122 Excel notes / balances / finance witness', 'Примечания Excel, остатки и расширенные доказательства финансового редактирования.'), 'v121_current': _modern_behavior_profile('v121 Forward outcome / Excel exports', 'Уточнение результата пересылки и сохранение всех вариантов Excel/экспорта.'), 'v120_current': _modern_behavior_profile('v120 Single-flight exports / forward witness', 'Повторные нажатия экспорта не копятся в очереди; видимое время формирования; исправление ложного ambiguous forward для worker-skip.'), 'v119_current': _modern_behavior_profile('v119 Excel / runtime export / exact edit', 'Новый Excel с заливками и примечаниями, экспорт runtime из MEGA, исправление ложного source_finance при редактировании.'), 'v118_current': _modern_behavior_profile('v118 Runtime slots / restart forensics', 'Rotating runtime slots, корректный watcher_mega_ok и диагностика рестартов Render.'), 'v117_current': _modern_behavior_profile('v117 Secret routes / Telegram maintenance', 'Исправление secret-route witness и отдельная throttled maintenance-очередь Telegram edits.'), 'v116_current': _modern_behavior_profile('v116 Stable LOW-RAM / exact effects', 'LOW-RAM cleanup, exact-effects forwarding и журнал через отдельный EXPORT pool.'), 'v115_current': _modern_behavior_profile('v115 Stable LOW-RAM core', 'Стабилизация LOW-RAM, SQLite snapshots и fallback runtime recovery.'), 'v114_current': _modern_behavior_profile('v114 LOW-RAM SQLite / MEGA core', 'Cold history в SQLite и уменьшение RAM без удаления пользовательских функций.'), 'v113_current': _modern_behavior_profile('v113 Memory guard stability', 'Контроль памяти Render и аварийная очистка диагностических данных.'), 'v112_current': _modern_behavior_profile('v112 Runtime forensics stability', 'Durable runtime heartbeat, exception hooks и исправленный atomic JSON dump.'), 'v111_current': _modern_behavior_profile('v111 Durable journal / Render history', 'Append-only журнал действий в MEGA и история между restart/deploy.'), 'v110_current': _modern_behavior_profile('v110 Finance priority / max diagnostics', 'FINANCE → FORWARD приоритет и расширенная диагностика задержек.'), 'v109_current': _modern_behavior_profile('v109 Exact-once finance safe recovery', 'Operation keys, no blind replay running-задач и защита от дублей финансов.'), 'v108_current': _modern_behavior_profile('v108 BOOT/SHUTDOWN / fin windows', 'BOOT/READY/SHUTDOWN watcher и восстановление финансовых окон.'), 'v107_current': _modern_behavior_profile('v107 All forwarding durable', 'Durable witness для всех пересылаемых типов контента.'), 'v106_current': _modern_behavior_profile('v106 Deploy-safe all directions', 'Deploy-safe пересылка и восстановление направлений без потери сообщений.'), 'v105_current': _modern_behavior_profile('v105 MEGA durable tasks', 'Внешние карточки критических Telegram update в MEGA до выполнения.'), 'v104_current': _modern_behavior_profile('v104 Durable dispatcher / timers', 'Диспетчер update, bounded queues и устойчивые внутренние таймеры.'), 'v103_current': _modern_behavior_profile('v103 Compact MEGA delta safe', 'Компактные delta без раздувания полной истории и safe full snapshot fallback.'), 'v102_current': _modern_behavior_profile('v102 Supergroup migration / forward retry', 'Автомиграция group→supergroup и одноразовый безопасный retry пересылки.'), 'v101_current': _modern_behavior_profile('v101 MEGA restore / durable forward finance', 'Повторный discovery restore и немедленное durable сохранение финансовой пересылки.'), 'v100_current': _modern_behavior_profile('v100 Factory defaults / file identity', 'Заводские настройки, имя файла/версии и улучшения Ф9998.'), 'v99_current': _modern_behavior_profile('v99 Manual MEGA restore menu', 'Ручное полное обновление состояния из MEGA через INFO.'), 'v98_current': _modern_behavior_profile('v98 Buttons / Restore guard', 'Рабочий /buttons и постоянный ручной override Restore guard.')}
BOT_BEHAVIOR_PROFILES = {**_MODERN_BEHAVIOR_PROFILES, **_BASE_BOT_BEHAVIOR_PROFILES}
DEFAULT_BOT_BEHAVIOR_PROFILE = 'v148_current'

def active_bot_behavior_profile() -> str:
    try:
        key = str((data or {}).setdefault('_global_settings', {}).get('bot_behavior_profile') or DEFAULT_BOT_BEHAVIOR_PROFILE)
    except Exception:
        key = DEFAULT_BOT_BEHAVIOR_PROFILE
    return key if key in BOT_BEHAVIOR_PROFILES else DEFAULT_BOT_BEHAVIOR_PROFILE

def active_bot_behavior_profile_info() -> dict:
    return BOT_BEHAVIOR_PROFILES.get(active_bot_behavior_profile(), BOT_BEHAVIOR_PROFILES[DEFAULT_BOT_BEHAVIOR_PROFILE])

def _version_mode_snapshot_fields() -> tuple[tuple[str, ...], tuple[str, ...]]:
    global_fields = ('buttons_current_window', 'forward_menu_new_style', 'icon_button_mode', 'total_secret_mask_enabled', 'finance_day_start_5am', 'mega_backup_priority')
    chat_fields = ('buttons_current_window', 'journal_enabled', 'main_article_buttons_enabled', 'main_financial_value_buttons_enabled', 'gomonk_enabled', 'gomonk_entries', 'remaining_with_gomonk', 'usd_gomonk_enabled', 'usd_gomonk_entries', 'usd_remaining_with_gomonk', 'usd_display_enabled', 'currency_mode', 'remaining_show_ost_label', 'quick_balance_enabled', 'category_usd_enabled', 'expense_category_order_slugs', 'quick_balance_behavior', 'quick_balance_user_selected', 'hidden_finance', 'usd_transactions_view')
    return (global_fields, chat_fields)

def save_version_mode_snapshot(profile_key: str | None=None):
    try:
        key = str(profile_key or active_bot_behavior_profile())
        if key not in BOT_BEHAVIOR_PROFILES:
            return
        gs = data.setdefault('_global_settings', {})
        snapshots = gs.setdefault('version_mode_snapshots', {})
        global_fields, chat_fields = _version_mode_snapshot_fields()
        snap = {'global': {name: gs.get(name) for name in global_fields if name in gs}, 'chats': {}, 'saved_at': now_local().isoformat(timespec='seconds')}
        for cid, store in (data.get('chats', {}) or {}).items():
            if not isinstance(store, dict):
                continue
            settings = store.setdefault('settings', {})
            snap['chats'][str(cid)] = {name: settings.get(name) for name in chat_fields if name in settings}
        snapshots[key] = snap
    except Exception as e:
        log_error(f'save_version_mode_snapshot: {e}')

def restore_version_mode_snapshot(profile_key: str):
    try:
        gs = data.setdefault('_global_settings', {})
        snap = (gs.setdefault('version_mode_snapshots', {}) or {}).get(str(profile_key)) or {}
        global_fields, chat_fields = _version_mode_snapshot_fields()
        global_values = snap.get('global') if isinstance(snap, dict) else {}
        if isinstance(global_values, dict):
            for name in global_fields:
                if name in global_values:
                    gs[name] = global_values[name]
        chat_values = snap.get('chats') if isinstance(snap, dict) else {}
        if isinstance(chat_values, dict):
            for cid, values in chat_values.items():
                if not isinstance(values, dict):
                    continue
                store = get_chat_store(int(cid))
                settings = store.setdefault('settings', {})
                for name in chat_fields:
                    if name in values:
                        settings[name] = values[name]
    except Exception as e:
        log_error(f'restore_version_mode_snapshot({profile_key}): {e}')

def version_mode_feature(name: str) -> bool:
    try:
        return bool(active_bot_behavior_profile_info().get(str(name), False))
    except Exception:
        return False

def version_mode_layout() -> str:
    try:
        return str(active_bot_behavior_profile_info().get('info_layout') or 'v87')
    except Exception:
        return 'v87'

def set_bot_behavior_profile(profile_key: str) -> str:
    profile_key = str(profile_key or '').strip()
    if profile_key not in BOT_BEHAVIOR_PROFILES:
        profile_key = DEFAULT_BOT_BEHAVIOR_PROFILE
    previous = active_bot_behavior_profile()
    if previous != profile_key:
        save_version_mode_snapshot(previous)
    data.setdefault('_global_settings', {})['bot_behavior_profile'] = profile_key
    if previous != profile_key:
        restore_version_mode_snapshot(profile_key)
    save_data(data, full=True)
    try:
        with _ui_edit_lock:
            _ui_edit_last_ts.clear()
            _ui_edit_pending.clear()
    except Exception:
        pass
    try:
        schedule_config_backup_for_chats(delay=1.0)
    except Exception:
        pass
    return profile_key

def bot_behavior_profile_label() -> str:
    return '🧩 ' + str(active_bot_behavior_profile_info().get('title') or active_bot_behavior_profile())

def effective_ui_edit_interval() -> float:
    """v246: keep Telegram edits responsive in every compatibility profile.

    Old profiles still control layout/feature semantics, but their historical 0.2-1.15s
    UI debounce is no longer allowed to make window switches and toggles feel frozen.
    Environment override remains available, capped to a safe floor.
    """
    raw = os.getenv('UI_EDIT_MIN_INTERVAL_SECONDS')
    if raw not in (None, ''):
        try:
            return max(0.02, min(0.08, float(raw)))
        except Exception:
            pass
    configured = float(active_bot_behavior_profile_info().get('ui_edit_interval', 0.03))
    return max(0.02, min(0.05, configured))

def effective_fast_telegram_gap() -> float:
    configured = float(active_bot_behavior_profile_info().get('fast_tg_gap', 0.02))
    return max(0.005, min(0.03, configured))

def main_article_buttons_enabled(chat_id: int) -> bool:
    try:
        return bool(get_chat_store(int(chat_id)).setdefault('settings', {}).get('main_article_buttons_enabled', False))
    except Exception:
        return False

def set_main_article_buttons_enabled(chat_id: int, enabled: bool):
    store = get_chat_store(int(chat_id))
    store.setdefault('settings', {})['main_article_buttons_enabled'] = bool(enabled)
    save_data(data, chat_ids=[int(chat_id)])
    schedule_config_backup_for_chats(int(chat_id))

def toggle_main_article_buttons(chat_id: int) -> bool:
    new_value = not main_article_buttons_enabled(int(chat_id))
    set_main_article_buttons_enabled(int(chat_id), new_value)
    return new_value

def main_article_buttons_label(chat_id: int) -> str:
    return '✅ Статьи-кнопки ВКЛ' if main_article_buttons_enabled(int(chat_id)) else '⬜ Статьи-кнопки ВЫКЛ'

def main_financial_value_buttons_enabled(chat_id: int) -> bool:
    try:
        return bool(get_chat_store(int(chat_id)).setdefault('settings', {}).get('main_financial_value_buttons_enabled', False))
    except Exception:
        return False

def effective_main_article_buttons_enabled(chat_id: int) -> bool:
    return bool(version_mode_feature('article_buttons') and main_article_buttons_enabled(int(chat_id)))

def effective_main_financial_value_buttons_enabled(chat_id: int) -> bool:
    return bool(version_mode_feature('financial_value_buttons') and main_financial_value_buttons_enabled(int(chat_id)))

def set_main_financial_value_buttons_enabled(chat_id: int, enabled: bool):
    store = get_chat_store(int(chat_id))
    store.setdefault('settings', {})['main_financial_value_buttons_enabled'] = bool(enabled)
    save_data(data, chat_ids=[int(chat_id)])
    schedule_config_backup_for_chats(int(chat_id))

def toggle_main_financial_value_buttons(chat_id: int) -> bool:
    new_value = not main_financial_value_buttons_enabled(int(chat_id))
    set_main_financial_value_buttons_enabled(int(chat_id), new_value)
    return new_value

def main_financial_value_buttons_label(chat_id: int) -> str:
    return '✅ Финансы-кнопки ВКЛ' if main_financial_value_buttons_enabled(int(chat_id)) else '⬜ Финансы-кнопки ВЫКЛ'
FIN_BUTTON_RIGHT_PAD = max(0, min(18, int(os.getenv('FIN_BUTTON_RIGHT_PAD', '10') or '10')))
FIN_BUTTON_PAD_CHAR = '⠀'
BUTTON_LABEL_TARGET_CHARS = 41

def pad_button_label_41(value: str, target: int=BUTTON_LABEL_TARGET_CHARS) -> str:
    """Единое визуальное выравнивание кнопок: максимум/минимум ровно 41 символ."""
    text = re.sub('\\s+', ' ', str(value or '').strip())
    target = max(1, int(target or BUTTON_LABEL_TARGET_CHARS))
    if len(text) > target:
        text = text[:max(1, target - 1)] + '…'
    if len(text) < target:
        text += FIN_BUTTON_PAD_CHAR * (target - len(text))
    return text

def financial_record_button_label(rec: dict, chat_id: int | None=None) -> str:
    view_usd = False
    try:
        view_usd = bool(chat_id is not None and usd_transactions_view_enabled(int(chat_id)))
    except Exception:
        view_usd = False
    try:
        amount = float((rec or {}).get('usd_amount' if view_usd else 'amount', 0) or 0)
    except Exception:
        amount = 0.0
    if view_usd:
        sid = str((rec or {}).get('usd_short_id') or f"U{(rec or {}).get('id', '')}")
        note = re.sub('\\s+', ' ', str((rec or {}).get('usd_note') or (rec or {}).get('note') or '').strip())
        amount_text = f"{('+' if amount >= 0 else '-')}${fmt_num_plain(abs(amount))}"
    else:
        sid = str((rec or {}).get('short_id') or f"R{(rec or {}).get('id', '')}")
        note = re.sub('\\s+', ' ', str((rec or {}).get('note') or '').strip())
        if chat_id is not None and version_mode_feature('daily_usd'):
            amount_text = format_chat_amount(int(chat_id), amount, mixed_space=False)
        else:
            amount_text = fmt_num(amount)
    if len(note) > 31:
        note = note[:30] + '…'
    label = f'{sid} {amount_text}'
    if note:
        label += f' {note}'
    return pad_button_label_41(label)

def financial_value_records_for_day(chat_id: int, day_key: str) -> list[dict]:
    try:
        store = get_chat_store(int(chat_id))
        recs = store.get('daily_records', {}).get(str(day_key), []) or []
        view_usd = bool(usd_transactions_view_enabled(int(chat_id)))
        if view_usd:
            return sorted((r for r in recs if isinstance(r, dict) and abs(float(r.get('usd_amount', 0) or 0)) > 0), key=record_sort_key)
        return sorted((r for r in recs if isinstance(r, dict) and (not bool(r.get('usd_only', False)))), key=record_sort_key)
    except Exception:
        return []

def _owner_setting_value(key: str, default=False, chat_id: int | None=None):
    """Настройка owner scope; для старых данных сохраняет fallback на глобальное значение."""
    try:
        cid = int(chat_id) if chat_id is not None else current_state_chat_id()
        if cid is not None:
            scoped = owner_scoped_settings(cid)
            if key in scoped:
                return scoped.get(key)
        return (data or {}).setdefault('_global_settings', {}).get(key, default)
    except Exception:
        return default

def _set_owner_setting_value(key: str, value, chat_id: int | None=None):
    cid = int(chat_id) if chat_id is not None else current_state_chat_id()
    if cid is not None:
        owner_scoped_settings(cid)[key] = value
        save_data(data, chat_ids=[cid])
        schedule_config_backup_for_chats(cid, delay=0.3)
    else:
        data.setdefault('_global_settings', {})[key] = value
        save_data(data)

def _buttons_current_window_scoped_present(chat_id: int | None=None) -> bool:
    """True when the canonical owner-scoped value was explicitly stored for this chat.

    v207: old builds kept a second per-chat flag in store.settings and combined both
    flags with OR.  That made OFF ineffective whenever the legacy flag remained True.
    The owner-scoped value is now authoritative; the legacy value is only a one-time
    migration source for chats that have never received the canonical setting.
    """
    try:
        cid = int(chat_id) if chat_id is not None else current_state_chat_id()
        if cid is None:
            return False
        return 'buttons_current_window' in owner_scoped_settings(int(cid))
    except Exception:
        return False

def buttons_current_window_enabled(chat_id: int | None=None) -> bool:
    try:
        cid = int(chat_id) if chat_id is not None else current_state_chat_id()
    except Exception:
        cid = None
    if cid is not None and (not _buttons_current_window_scoped_present(cid)):
        try:
            settings = get_chat_store(int(cid)).setdefault('settings', {})
            if 'buttons_current_window' in settings:
                legacy = bool(settings.get('buttons_current_window', False))
                owner_scoped_settings(int(cid))['buttons_current_window'] = legacy
                try:
                    save_data(data, chat_ids=[int(cid)])
                    schedule_config_backup_for_chats(int(cid), delay=0.3)
                except Exception:
                    pass
                return legacy
        except Exception:
            pass
    return bool(_owner_setting_value('buttons_current_window', False, cid))

def chat_buttons_current_window_enabled(chat_id: int) -> bool:
    try:
        return bool(buttons_current_window_enabled(int(chat_id)))
    except Exception:
        return False

def set_buttons_current_window_enabled(enabled: bool, chat_id: int | None=None):
    try:
        cid = int(chat_id) if chat_id is not None else current_state_chat_id()
        value = bool(enabled)
        _set_owner_setting_value('buttons_current_window', value, cid)
        if cid is not None:
            try:
                get_chat_store(int(cid)).setdefault('settings', {})['buttons_current_window'] = value
                save_data(data, chat_ids=[int(cid)])
            except Exception:
                pass
    except Exception as e:
        log_error(f'set_buttons_current_window_enabled: {e}')

def toggle_chat_buttons_current_window(chat_id: int) -> bool:
    new_value = not chat_buttons_current_window_enabled(int(chat_id))
    set_buttons_current_window_enabled(new_value, int(chat_id))
    return new_value

def toggle_buttons_current_window(chat_id: int | None=None) -> bool:
    new_value = not buttons_current_window_enabled(chat_id)
    set_buttons_current_window_enabled(new_value, chat_id)
    return new_value

def buttons_current_window_label(chat_id: int | None=None) -> str:
    return '✅ В текущем окне' if buttons_current_window_enabled(chat_id) else '⬜ В текущем окне'

def forward_menu_new_style_enabled(chat_id: int | None=None) -> bool:
    return bool(_owner_setting_value('forward_menu_new_style', False, chat_id))

def _v177_legacy_0004_set_forward_menu_new_style_enabled(enabled: bool, chat_id: int | None=None):
    try:
        _set_owner_setting_value('forward_menu_new_style', bool(enabled), chat_id)
    except Exception as e:
        log_error(f'set_forward_menu_new_style_enabled: {e}')
try:
    _v177_legacy_0004_set_forward_menu_new_style_enabled.__name__ = 'set_forward_menu_new_style_enabled'
except Exception:
    pass

def _v177_legacy_0005_toggle_forward_menu_new_style(chat_id: int | None=None) -> bool:
    new_value = not forward_menu_new_style_enabled(chat_id)
    set_forward_menu_new_style_enabled(new_value, chat_id)
    return new_value
try:
    _v177_legacy_0005_toggle_forward_menu_new_style.__name__ = 'toggle_forward_menu_new_style'
except Exception:
    pass

def forward_menu_style_label(chat_id: int | None=None) -> str:
    return '🧩 Пересылка: по-новому' if forward_menu_new_style_enabled(chat_id) else '🔁 Пересылка: обычно'

def icon_button_mode_enabled(chat_id: int | None=None) -> bool:
    if chat_id is None:
        chat_id = current_state_chat_id()
        if chat_id is None and OWNER_ID:
            try:
                chat_id = int(OWNER_ID)
            except Exception:
                chat_id = None
    return bool(_owner_setting_value('icon_button_mode', True, chat_id))

def set_icon_button_mode_enabled(enabled: bool, chat_id: int | None=None):
    try:
        _set_owner_setting_value('icon_button_mode', bool(enabled), chat_id)
    except Exception as e:
        log_error(f'set_icon_button_mode_enabled: {e}')

def toggle_icon_button_mode(chat_id: int | None=None) -> bool:
    new_value = not icon_button_mode_enabled(chat_id)
    set_icon_button_mode_enabled(new_value, chat_id)
    return new_value

def icon_button_mode_label(chat_id: int | None=None) -> str:
    return '🔣 Кнопки: значки' if icon_button_mode_enabled(chat_id) else '🔤 Кнопки: текст'

def total_secret_mask_enabled(chat_id: int | None=None) -> bool:
    try:
        if chat_id is not None:
            scoped = owner_scoped_settings(int(chat_id))
            if 'total_secret_mask_enabled' in scoped:
                return bool(scoped.get('total_secret_mask_enabled'))
        gs = (data or {}).setdefault('_global_settings', {})
        return bool(gs.get('total_secret_mask_enabled', False))
    except Exception:
        return False

def set_total_secret_mask_enabled(enabled: bool, chat_id: int | None=None):
    try:
        if chat_id is not None:
            owner_scoped_settings(int(chat_id))['total_secret_mask_enabled'] = bool(enabled)
            save_data(data, chat_ids=[int(chat_id)])
            schedule_config_backup_for_chats(int(chat_id), delay=0.3)
        else:
            data.setdefault('_global_settings', {})['total_secret_mask_enabled'] = bool(enabled)
            save_data(data)
    except Exception as e:
        log_error(f'set_total_secret_mask_enabled: {e}')

def toggle_total_secret_mask(chat_id: int | None=None) -> bool:
    new_value = not total_secret_mask_enabled(chat_id)
    set_total_secret_mask_enabled(new_value, chat_id)
    return new_value

def total_secret_mask_label(chat_id: int | None=None) -> str:
    return '✅ 🪷 Маска: ВКЛ' if total_secret_mask_enabled(chat_id) else '⬜ 🪷 Маска: ВЫКЛ'

def verbose_telegram_journal_enabled() -> bool:
    """Успешные Telegram API-вызовы очень шумные. Отдельный opt-in даже при полном журнале."""
    try:
        if _env_bool('BOT_JOURNAL_VERBOSE_TELEGRAM', '0'):
            return True
    except Exception:
        pass
    try:
        return bool((data or {}).setdefault('_global_settings', {}).get('bot_journal_verbose_telegram', False))
    except Exception:
        return False

def set_verbose_telegram_journal_enabled(enabled: bool) -> bool:
    try:
        gs = (data or {}).setdefault('_global_settings', {})
        gs['bot_journal_verbose_telegram'] = bool(enabled)
        try:
            save_data(data, root_only=True)
        except TypeError:
            save_data(data)
        try:
            fn = globals().get('schedule_delta_backup')
            if callable(fn):
                fn(int(globals().get('OWNER_ID') or 0) or None, delay=0.8, reason='journal_verbose_telegram')
        except Exception:
            pass
    except Exception:
        pass
    return bool(enabled)

def toggle_verbose_telegram_journal() -> bool:
    return set_verbose_telegram_journal_enabled(not verbose_telegram_journal_enabled())

def _journal_rotate_local_locked() -> None:
    try:
        if not os.path.exists(BOT_JOURNAL_FILE) or os.path.getsize(BOT_JOURNAL_FILE) < BOT_JOURNAL_LOCAL_MAX_BYTES:
            return
        oldest = f'{BOT_JOURNAL_FILE}.{BOT_JOURNAL_LOCAL_KEEP_FILES}'
        try:
            if os.path.exists(oldest):
                os.remove(oldest)
        except Exception:
            pass
        for idx in range(BOT_JOURNAL_LOCAL_KEEP_FILES - 1, 0, -1):
            src = f'{BOT_JOURNAL_FILE}.{idx}'
            dst = f'{BOT_JOURNAL_FILE}.{idx + 1}'
            if os.path.exists(src):
                try:
                    os.replace(src, dst)
                except Exception:
                    pass
        try:
            os.replace(BOT_JOURNAL_FILE, f'{BOT_JOURNAL_FILE}.1')
        except Exception:
            pass
    except Exception:
        pass

def _journal_write_row(row: dict):
    try:
        line = json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n'
        with _JOURNAL_FILE_LOCK:
            _journal_rotate_local_locked()
            with open(BOT_JOURNAL_FILE, 'a', encoding='utf-8') as jf:
                jf.write(line)
    except Exception:
        pass

def _v177_legacy_0006_bot_journal(action: str, chat_id=None, detail: str='', level: str='INFO'):
    """Пишет действие в общий журнал: команды, кнопки, функции, Telegram API, backup, ошибки."""
    try:
        action_name = str(action or '')
        always_record = action_name.startswith('window_')
        if not always_record and action_name not in {'journal_toggle', 'journal_chat_toggle', 'journal_export_requested'} and (str(level or 'INFO').upper() != 'ERROR'):
            if not journal_should_record(chat_id):
                return None
        _ws = WEBHOOK_TASK_POOL.stats()
        _uis = UI_TASK_POOL.stats() if 'UI_TASK_POOL' in globals() else {}
        _fs = FINANCE_TASK_POOL.stats()
        _fws = FORWARD_TASK_POOL.stats()
        _ds = DELTA_TASK_POOL.stats() if 'DELTA_TASK_POOL' in globals() else {}
        row = {'ts': _journal_ts(), 'level': str(level or 'INFO'), 'action': str(action or '')[:160], 'chat_id': str(chat_id) if chat_id is not None else '', 'chat_name': '', 'detail': str(detail or '')[:3000], 'thread': threading.current_thread().name, 'profile': active_bot_behavior_profile() if 'data' in globals() and isinstance(data, dict) else 'startup', 'bot_version': str(globals().get('VERSION') or 'startup'), 'render_commit': str(os.getenv('RENDER_GIT_COMMIT', '') or ''), 'webhook_pending': _ws.get('pending', 0), 'webhook_active': _ws.get('active', 0), 'ui_pending': _uis.get('pending', 0), 'ui_active': _uis.get('active', 0), 'finance_pending': _fs.get('pending', 0), 'finance_active': _fs.get('active', 0), 'forward_pending': _fws.get('pending', 0), 'forward_active': _fws.get('active', 0), 'delta_pending': _ds.get('pending', 0), 'general_pending': GENERAL_TASK_POOL.stats().get('pending', 0), 'backup_pending': BACKUP_TASK_POOL.stats().get('pending', 0), 'runtime_phase': str((globals().get('_RUNTIME_STATE') or {}).get('phase') or ''), 'runtime_ready': bool((globals().get('_RUNTIME_STATE') or {}).get('ready', False))}
        try:
            if chat_id is not None:
                row['chat_name'] = get_chat_display_name(int(chat_id))
        except Exception:
            pass
        with bot_journal_lock:
            BOT_ACTION_LOG.append(row)
        _critical_action = action_name.startswith(('runtime_shutdown_', 'runtime_fatal_', 'runtime_thread_unhandled_exception'))
        _critical_remote = BOT_CRITICAL_JOURNAL_DURABLE_ENABLED and (str(level or 'INFO').upper() in {'ERROR', 'CRITICAL'} or _critical_action)
        if BOT_JOURNAL_DURABLE_ENABLED or _critical_remote:
            try:
                _copy = dict(row)
                _copy['critical_remote'] = bool(_critical_remote)
                _buffer_len = 0
                with _JOURNAL_DURABLE_LOCK:
                    _JOURNAL_DURABLE_BUFFER.append(_copy)
                    _buffer_len = len(_JOURNAL_DURABLE_BUFFER)
                if _buffer_len >= BOT_JOURNAL_DURABLE_FLUSH_ROWS and journal_compact_remote_enabled():
                    sched = globals().get('DELAYED_SCHEDULER')
                    flush_fn = globals().get('journal_flush_to_mega')
                    if sched is not None and callable(flush_fn):
                        sched.schedule('journal-size-flush', 1.0, flush_fn, True)
            except Exception:
                pass
        if not JOURNAL_TASK_POOL.submit('journal-file', _journal_write_row, dict(row)):
            _journal_write_row(row)
        if str(level or '').upper() in {'ERROR', 'CRITICAL'}:
            try:
                flush_fn = globals().get('journal_flush_to_mega')
                scheduler = globals().get('DELAYED_SCHEDULER')
                if callable(flush_fn) and scheduler is not None and _journal_should_accelerate_error_flush(action_name, chat_id, level, detail):
                    scheduler.schedule('journal-error-flush', 5.0, flush_fn, True)
            except Exception:
                pass
        return row
    except Exception:
        return None
try:
    _v177_legacy_0006_bot_journal.__name__ = 'bot_journal'
except Exception:
    pass

def get_recent_journal(limit: int=200):
    try:
        with bot_journal_lock:
            return list(BOT_ACTION_LOG)[-int(limit):]
    except Exception:
        return []

def format_journal_text(limit: int=120) -> str:
    rows = get_recent_journal(limit)
    if not rows:
        return '📓 Журнал пока пуст.'
    lines = [f'📓 Журнал действий бота, последние {len(rows)} записей:']
    for r in rows:
        chat = r.get('chat_name') or r.get('chat_id') or '-'
        detail = r.get('detail') or ''
        if len(detail) > 500:
            detail = detail[:500] + '…'
        lines.append(f"\n• {r.get('ts', '')} [{r.get('level', '')}] {r.get('action', '')}\n  чат: {chat}\n  {detail}".rstrip())
    text = wm_owner('\n'.join(lines), 9)
    return text[-3900:] if len(text) > 3900 else text

def _safe_diag_call(name: str, func, default=None):
    try:
        return func()
    except Exception as e:
        return {'error': f'{name}: {e}'} if default is None else default

def _journal_read_file_rows(limit: int=20000) -> list[dict]:
    """Читает хвост текущего + локально ротированных JSONL, не загружая всё в RAM."""
    rows = []
    try:
        max_rows = max(1, int(limit))
        raw = deque(maxlen=max_rows)
        paths = []
        for idx in range(BOT_JOURNAL_LOCAL_KEEP_FILES, 0, -1):
            path = f'{BOT_JOURNAL_FILE}.{idx}'
            if os.path.exists(path):
                paths.append(path)
        if os.path.exists(BOT_JOURNAL_FILE):
            paths.append(BOT_JOURNAL_FILE)
        for path in paths:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    raw.extend(f)
            except Exception:
                continue
        for line in raw:
            try:
                item = json.loads(line)
                if isinstance(item, dict):
                    rows.append(item)
            except Exception:
                continue
    except Exception:
        return []
    return rows

def _v177_legacy_0008_atomic_json_dump(path: str, payload) -> None:
    """Atomically write JSON on the local ephemeral disk before a MEGA upload.

    v111 referenced this helper before it existed, so both runtime_latest and the
    durable journal failed to persist.  Keep it tiny and dependency-free.
    """
    target = os.path.abspath(str(path))
    parent = os.path.dirname(target) or '.'
    os.makedirs(parent, exist_ok=True)
    tmp = f'{target}.tmp.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}'
    try:
        with open(tmp, 'w', encoding='utf-8') as fh:
            json.dump(payload, fh, ensure_ascii=False, separators=(',', ':'))
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except Exception:
                pass
        os.replace(tmp, target)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
try:
    _v177_legacy_0008_atomic_json_dump.__name__ = '_atomic_json_dump'
except Exception:
    pass

def _journal_durable_remote_dir() -> str:
    base = str(globals().get('MEGA_BACKUP_DIR') or MEGA_LEGACY_BACKUP_DIR).rstrip('/')
    return f'{base}/runtime/journal'

def _journal_row_key(row: dict):
    return (str(row.get('ts') or ''), str(row.get('action') or ''), str(row.get('chat_id') or ''), str(row.get('detail') or ''), str(row.get('thread') or ''))

def _journal_merge_rows(*groups, limit: int=20000) -> list[dict]:
    seen = set()
    out = []
    for group in groups:
        for row in group or []:
            if not isinstance(row, dict):
                continue
            key = _journal_row_key(row)
            if key in seen:
                continue
            seen.add(key)
            out.append(row)
    try:
        out.sort(key=lambda r: str(r.get('ts') or ''))
    except Exception:
        pass
    return out[-max(1, int(limit)):]

def _journal_write_gzip_payload(path: str, payload: dict) -> tuple[int, int]:
    raw = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    with gzip.open(path, 'wb', compresslevel=6) as fh:
        fh.write(raw)
    try:
        packed = os.path.getsize(path)
    except Exception:
        packed = 0
    return (len(raw), int(packed))

def _journal_load_chunk_rows(local_path: str) -> list[dict]:
    """Read both historical plain JSON chunks and v208 gzip JSON chunks."""
    try:
        path = str(local_path or '')
        if path.endswith('.gz'):
            with gzip.open(path, 'rt', encoding='utf-8') as fh:
                doc = json.load(fh)
        else:
            doc = _load_json(path, {})
        rows = doc.get('rows') if isinstance(doc, dict) else []
        return [dict(r) for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []
    except Exception:
        return []

def journal_flush_to_mega(force: bool=False) -> bool:
    """v208: durable full diagnostic journal as batched gzip chunks; never one network call per row."""
    global _JOURNAL_DURABLE_SEQ
    if not BOT_JOURNAL_DURABLE_ENABLED:
        return False
    if not journal_compact_remote_enabled():
        fn = globals().get('journal_flush_critical_to_mega')
        return bool(fn(force)) if callable(fn) else True
    if not globals().get('mega_is_configured') or not mega_is_configured():
        return False
    with _JOURNAL_DURABLE_LOCK:
        if not _JOURNAL_DURABLE_BUFFER:
            return True
        if not force and len(_JOURNAL_DURABLE_BUFFER) < BOT_JOURNAL_DURABLE_FLUSH_ROWS:
            return False
        rows = list(_JOURNAL_DURABLE_BUFFER)
        _JOURNAL_DURABLE_BUFFER.clear()
        _JOURNAL_DURABLE_SEQ += 1
        seq = _JOURNAL_DURABLE_SEQ
    tmp = None
    try:
        remote_dir = _journal_durable_remote_dir()
        mega_ensure_remote_path(remote_dir)
        os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
        stamp = now_local().strftime('%Y%m%d_%H%M%S_%f') if 'now_local' in globals() else datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        inst = mega_safe_name(str(os.getenv('RENDER_INSTANCE_ID', 'local') or 'local')[-18:], 'instance')
        name = f'journal_{stamp}_{inst}_{seq:06d}.json.gz'
        tmp = os.path.join(MEGA_LOCAL_TMP_DIR, name)
        payload = {'kind': 'telegram_bot_journal_chunk', 'schema_version': 3, 'compression': 'gzip', 'bot_version': globals().get('VERSION', ''), 'created_at': _journal_ts(), 'render_instance_id': str(os.getenv('RENDER_INSTANCE_ID', '') or ''), 'render_git_commit': str(os.getenv('RENDER_GIT_COMMIT', '') or ''), 'row_count': len(rows), 'rows': rows}
        raw_bytes, packed_bytes = _journal_write_gzip_payload(tmp, payload)
        _mega_run('mega-put', [tmp, remote_dir], check=True, timeout=MEGA_TIMEOUT)
        _JOURNAL_DURABLE_STATS['uploaded_chunks'] += 1
        _JOURNAL_DURABLE_STATS['uploaded_rows'] += len(rows)
        _JOURNAL_DURABLE_STATS['uploaded_payload_bytes'] += int(packed_bytes)
        _JOURNAL_DURABLE_STATS['raw_estimated_bytes'] += int(raw_bytes)
        _JOURNAL_DURABLE_STATS['last_upload_at'] = _journal_ts()
        _JOURNAL_DURABLE_STATS['last_upload_file'] = name
        _JOURNAL_DURABLE_STATS['last_error'] = ''
        if _JOURNAL_DURABLE_STATS['uploaded_chunks'] % 50 == 0:
            try:
                _mega_prune_remote_history(remote_dir, 'journal_*.json.gz', BOT_JOURNAL_DURABLE_REMOTE_KEEP)
            except Exception:
                pass
        return True
    except Exception as e:
        _JOURNAL_DURABLE_STATS['upload_errors'] += 1
        _JOURNAL_DURABLE_STATS['last_error'] = str(e)[:500]
        with _JOURNAL_DURABLE_LOCK:
            _JOURNAL_DURABLE_BUFFER[0:0] = rows
            if len(_JOURNAL_DURABLE_BUFFER) > 5000:
                del _JOURNAL_DURABLE_BUFFER[:-5000]
        return False
    finally:
        try:
            if tmp and os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

def journal_flush_critical_to_mega(force: bool=False) -> bool:
    """Persist only ERROR/CRITICAL/lifecycle rows when routine MEGA journal is disabled."""
    if not BOT_CRITICAL_JOURNAL_DURABLE_ENABLED or not mega_is_configured():
        return True
    rows = []
    try:
        with _JOURNAL_DURABLE_LOCK:
            rows = [dict(r) for r in _JOURNAL_DURABLE_BUFFER if bool((r or {}).get('critical_remote'))]
        if not rows:
            return True
        remote_dir = _journal_durable_remote_dir()
        mega_ensure_remote_path(remote_dir)
        global _JOURNAL_DURABLE_SEQ
        with _JOURNAL_DURABLE_LOCK:
            _JOURNAL_DURABLE_SEQ += 1
            seq = _JOURNAL_DURABLE_SEQ
        stamp = now_local().strftime('%Y%m%d_%H%M%S_%f')
        inst = mega_safe_name(_runtime_instance_id() if '_runtime_instance_id' in globals() else str(os.getpid()), 'instance')
        name = f'journal_critical_{stamp}_{inst}_{seq:06d}.json.gz'
        tmp = os.path.join(MEGA_LOCAL_TMP_DIR, name)
        critical_rows = rows[-100:]
        try:
            with bot_journal_lock:
                recent_context = [dict(r) for r in list(BOT_ACTION_LOG)[-60:]]
        except Exception:
            recent_context = []
        try:
            with globals().get('_RUNTIME_LOCK', threading.RLock()):
                runtime_context = [dict(r) for r in list(globals().get('_RUNTIME_EVENTS') or [])[-30:]]
                runtime_state = dict(globals().get('_RUNTIME_STATE') or {})
        except Exception:
            runtime_context = []
            runtime_state = {}
        payload = {'kind': 'telegram_bot_critical_journal_chunk', 'schema_version': 2, 'bot_version': globals().get('VERSION', ''), 'created_at': _journal_ts(), 'render_instance_id': str(os.getenv('RENDER_INSTANCE_ID', '') or ''), 'render_git_commit': str(os.getenv('RENDER_GIT_COMMIT', '') or ''), 'rows': critical_rows, 'recent_journal_context': recent_context, 'runtime_events': runtime_context, 'runtime_state': {k: runtime_state.get(k) for k in ('phase', 'ready', 'last_event', 'last_event_at', 'last_error', 'started_at', 'last_webhook_at', 'shutdown_started_at', 'shutdown_finished_at', 'shutdown_signal', 'fatal_main_exception', 'fatal_thread_exception')}}
        _raw_bytes, _packed_bytes = _journal_write_gzip_payload(tmp, payload)
        _mega_run('mega-put', [tmp, remote_dir], check=True, timeout=MEGA_TIMEOUT)
        keys = {_journal_row_key(r) for r in rows}
        with _JOURNAL_DURABLE_LOCK:
            _JOURNAL_DURABLE_BUFFER[:] = [r for r in _JOURNAL_DURABLE_BUFFER if _journal_row_key(r) not in keys]
        _JOURNAL_DURABLE_STATS['uploaded_chunks'] += 1
        _JOURNAL_DURABLE_STATS['uploaded_rows'] += len(critical_rows)
        _JOURNAL_DURABLE_STATS['uploaded_payload_bytes'] += int(_packed_bytes)
        _JOURNAL_DURABLE_STATS['raw_estimated_bytes'] += int(_raw_bytes)
        _JOURNAL_DURABLE_STATS['last_upload_at'] = _journal_ts()
        _JOURNAL_DURABLE_STATS['last_upload_file'] = name
        if _JOURNAL_DURABLE_STATS['uploaded_chunks'] % 25 == 0:
            try:
                _mega_prune_remote_history(remote_dir, 'journal_critical_*.json.gz', 400)
            except Exception:
                pass
        return True
    except Exception as exc:
        _JOURNAL_DURABLE_STATS['upload_errors'] += 1
        _JOURNAL_DURABLE_STATS['last_error'] = str(exc)[:700]
        return False
    finally:
        try:
            if 'tmp' in locals() and os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

def _journal_durable_tick():
    try:
        with _JOURNAL_DURABLE_LOCK:
            has_rows = bool(_JOURNAL_DURABLE_BUFFER)
        if has_rows:
            JOURNAL_TASK_POOL.submit_unique('journal-mega-flush', journal_flush_to_mega, True)
    except Exception as e:
        _JOURNAL_DURABLE_STATS['last_error'] = str(e)[:500]
    finally:
        try:
            DELAYED_SCHEDULER.schedule('journal-durable-tick', float(journal_compact_interval_seconds()), _journal_durable_tick)
        except Exception:
            pass

def journal_start_durable_loop():
    global _JOURNAL_DURABLE_THREAD_STARTED
    if not BOT_JOURNAL_DURABLE_ENABLED or _JOURNAL_DURABLE_THREAD_STARTED:
        return
    _JOURNAL_DURABLE_THREAD_STARTED = True
    try:
        DELAYED_SCHEDULER.schedule('journal-durable-tick', float(journal_compact_interval_seconds()), _journal_durable_tick)
    except Exception:
        pass

def _journal_read_mega_rows(limit: int=20000) -> list[dict]:
    """Читает последние durable journal chunks из MEGA, newest-first files -> chronological rows."""
    if not BOT_JOURNAL_DURABLE_ENABLED or not globals().get('mega_is_configured') or (not mega_is_configured()):
        return []
    remote_dir = _journal_durable_remote_dir()
    try:
        estimated_files = max(3, int(max(1, int(limit)) / max(1, BOT_JOURNAL_DURABLE_FLUSH_ROWS)) + 3)
        files = _mega_find_remote_files(remote_dir, 'journal_*', min(BOT_JOURNAL_DURABLE_RESTORE_FILES, estimated_files))
    except Exception:
        return []
    chunks = []
    total = 0
    for remote in files:
        local = None
        try:
            local = _mega_download_remote_path(remote)
            rows = _journal_load_chunk_rows(local) if local else []
            if rows:
                chunks.append(rows)
                total += len(rows)
                if total >= int(limit):
                    break
        except Exception:
            continue
        finally:
            try:
                if local:
                    shutil.rmtree(os.path.dirname(local), ignore_errors=True)
            except Exception:
                pass
    merged = []
    for rows in reversed(chunks):
        merged.extend(rows)
    return _journal_merge_rows(merged, limit=limit)

def journal_restore_from_mega(limit: int=200) -> dict:
    """BOOT: restores only a small recent tail. Full history stays in MEGA and is streamed on export."""
    remote_rows = _journal_read_mega_rows(limit)
    local_rows = _journal_read_file_rows(limit)
    merged = _journal_merge_rows(remote_rows, local_rows, limit=limit)
    if remote_rows:
        try:
            with bot_journal_lock:
                BOT_ACTION_LOG.clear()
                BOT_ACTION_LOG.extend(merged[-BOT_JOURNAL_MAX:])
            with open(BOT_JOURNAL_FILE, 'w', encoding='utf-8') as f:
                for row in merged:
                    f.write(json.dumps(row, ensure_ascii=False) + '\n')
        except Exception as e:
            _JOURNAL_DURABLE_STATS['last_error'] = str(e)[:500]
    _JOURNAL_DURABLE_STATS['restored_rows'] = len(remote_rows)
    _JOURNAL_DURABLE_STATS['restored_chunks'] = 0 if not remote_rows else 1
    return {'remote_rows': len(remote_rows), 'merged_rows': len(merged)}

def journal_durable_stats() -> dict:
    with _JOURNAL_DURABLE_LOCK:
        out = dict(_JOURNAL_DURABLE_STATS)
        out['buffer_rows'] = len(_JOURNAL_DURABLE_BUFFER)
    raw = int(out.get('raw_estimated_bytes', 0) or 0)
    packed = int(out.get('uploaded_payload_bytes', 0) or 0)
    out.update({'enabled': BOT_JOURNAL_DURABLE_ENABLED, 'compact_remote_enabled': journal_compact_remote_enabled(), 'compression': 'gzip', 'flush_seconds': journal_compact_interval_seconds(), 'flush_rows': BOT_JOURNAL_DURABLE_FLUSH_ROWS, 'compression_ratio': round(float(packed) / float(raw), 4) if raw else None, 'local_max_bytes': BOT_JOURNAL_LOCAL_MAX_BYTES, 'local_keep_files': BOT_JOURNAL_LOCAL_KEEP_FILES, 'remote_dir': _journal_durable_remote_dir()})
    return out

def journal_v208_status_text() -> str:
    try:
        stats = journal_durable_stats()
    except Exception:
        stats = {}
    full = is_journal_registration_enabled()
    remote = journal_compact_remote_effective_enabled()
    verbose = verbose_telegram_journal_enabled()
    interval = journal_compact_interval_seconds() // 60
    buf = int(stats.get('buffer_rows', 0) or 0)
    chunks = int(stats.get('uploaded_chunks', 0) or 0)
    rows = int(stats.get('uploaded_rows', 0) or 0)
    packed = int(stats.get('uploaded_payload_bytes', 0) or 0)
    raw = int(stats.get('raw_estimated_bytes', 0) or 0)
    ratio = 100.0 * packed / raw if raw else None
    immediate = int(stats.get('immediate_error_flushes', 0) or 0)
    suppressed = int(stats.get('suppressed_immediate_error_flushes', 0) or 0)

    def _sz(n):
        n = float(max(0, int(n or 0)))
        if n >= 1024 * 1024:
            return f'{n / 1024 / 1024:.2f} MB'
        if n >= 1024:
            return f'{n / 1024:.1f} KB'
        return f'{int(n)} B'
    lines = ['📓 ПОЛНЫЙ ДИАГНОСТИЧЕСКИЙ ЖУРНАЛ', '', f"Полная регистрация действий: {('✅ ВКЛ' if full else '⬜ ВЫКЛ')}", f"Сжатая фиксация в MEGA: {('✅ ВКЛ' if remote else '⬜ ВЫКЛ')}", f'Интервал durable batch: {interval} мин или {BOT_JOURNAL_DURABLE_FLUSH_ROWS} строк', f"Успешные Telegram API (очень шумно): {('✅ ВКЛ' if verbose else '⬜ ВЫКЛ')}", f'Сейчас ждут отправки: {buf} строк', f'Этот процесс: {rows} строк / {chunks} batch / {_sz(packed)} в MEGA', f'ERROR fast-flush: {immediate}; повторов объединено до batch: {suppressed}']
    if ratio is not None:
        lines.append(f'После gzip: {ratio:.1f}% от исходного JSON')
    lines += ['', 'Локально журнал пишется постоянно и ротируется на диске. В MEGA он уходит gzip-пакетом. Первая уникальная ERROR/CRITICAL ускоряет фиксацию; одинаковые повторы в течение cooldown остаются в полном локальном журнале и уходят ближайшим штатным batch без лишних MEGA-запросов.']
    return '\n'.join(lines)

def journal_v208_interval_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=4)
    cur = journal_compact_interval_seconds()
    buttons = []
    for sec in JOURNAL_V208_INTERVAL_CHOICES:
        label = f"{('✅ ' if sec == cur else '')}{sec // 60} мин"
        buttons.append(IB(label, callback_data=f'journal_compact_interval:{sec}'))
    kb.row(*buttons)
    kb.row(IB('🔙 Назад в журнал', callback_data='journal_open'))
    return kb

def _journal_warm_tail_job():
    """Post-READY warm-up only; never delays BOOT or Telegram availability."""
    try:
        if runtime_is_shutting_down():
            return
        if _runtime_watcher_should_yield_to_critical_mega():
            DELAYED_SCHEDULER.schedule('journal-warm-tail', 30.0, _journal_warm_tail_job)
            return
        pressure = _runtime_memory_pressure()
        if str(pressure.get('level')) in {'high', 'critical'}:
            DELAYED_SCHEDULER.schedule('journal-warm-tail', 60.0, _journal_warm_tail_job)
            return
        jr = journal_restore_from_mega(40)
        runtime_event('journal_warm_tail', f"remote_rows={jr.get('remote_rows', 0)} merged_rows={jr.get('merged_rows', 0)}")
    except Exception as e:
        runtime_event('journal_warm_tail_error', str(e), 'WARN')

def _journal_render_safe_env() -> dict:
    keys = ('RENDER', 'RENDER_CPU_COUNT', 'RENDER_INSTANCE_ID', 'RENDER_GIT_COMMIT', 'RENDER_GIT_BRANCH', 'RENDER_GIT_REPO_SLUG', 'RENDER_SERVICE_ID', 'RENDER_SERVICE_NAME', 'RENDER_SERVICE_TYPE', 'RENDER_REGION', 'RENDER_EXTERNAL_HOSTNAME', 'RENDER_EXTERNAL_URL', 'IS_PULL_REQUEST')
    return {k: str(os.getenv(k, '') or '') for k in keys}

def _journal_diagnostic_snapshot() -> dict:
    snap = runtime_snapshot({'source': 'journal_export'}) if 'runtime_snapshot' in globals() else {}
    dispatcher = UPDATE_DISPATCHER.stats() if 'UPDATE_DISPATCHER' in globals() else {}
    thread_rows = []
    try:
        for t in threading.enumerate():
            thread_rows.append({'name': t.name, 'ident': t.ident, 'daemon': bool(t.daemon), 'alive': bool(t.is_alive())})
    except Exception:
        pass
    os_diag = {}
    try:
        os_diag['loadavg'] = list(os.getloadavg()) if hasattr(os, 'getloadavg') else None
    except Exception:
        os_diag['loadavg'] = None
    try:
        os_diag['cpu_count'] = os.cpu_count()
    except Exception:
        pass
    try:
        os_diag['open_fd_count'] = len(os.listdir('/proc/self/fd')) if os.path.isdir('/proc/self/fd') else None
    except Exception:
        os_diag['open_fd_count'] = None
    try:
        os_diag['python_gc_count'] = list(__import__('gc').get_count())
    except Exception:
        pass
    return {'generated_at': _journal_ts(), 'version': VERSION, 'behavior_profile': active_bot_behavior_profile() if 'active_bot_behavior_profile' in globals() else '', 'render_safe_env': _journal_render_safe_env(), 'runtime': snap, 'dispatcher': dispatcher, 'keep_alive': dict(KEEP_ALIVE_STATE) if 'KEEP_ALIVE_STATE' in globals() else {}, 'delayed_scheduler': DELAYED_SCHEDULER.stats() if 'DELAYED_SCHEDULER' in globals() else {}, 'mega_tasks': mega_task_registry_stats() if 'mega_task_registry_stats' in globals() else {}, 'durable_journal': journal_durable_stats() if 'journal_durable_stats' in globals() else {}, 'window_diagnostics': window_diagnostic_snapshot() if callable(globals().get('window_diagnostic_snapshot')) else {}, 'priority': {'order': ['finance', 'forward', 'other'], 'forward_finance_priority_max_wait_seconds': globals().get('FORWARD_FINANCE_PRIORITY_MAX_WAIT_SECONDS')}, 'threads': thread_rows, 'os': os_diag}

def _journal_write_export_row(fh, r: dict):
    q = f"Q content={r.get('webhook_pending', 0)}/{r.get('webhook_active', 0)} ui={r.get('ui_pending', 0)}/{r.get('ui_active', 0)} fin={r.get('finance_pending', 0)}/{r.get('finance_active', 0)} fwd={r.get('forward_pending', 0)}/{r.get('forward_active', 0)} delta={r.get('delta_pending', 0)} backup={r.get('backup_pending', 0)}"
    fh.write(f"{r.get('ts', '')} | {r.get('level', '')} | {r.get('action', '')} | chat={r.get('chat_name') or r.get('chat_id')} | thread={r.get('thread', '')} | phase={r.get('runtime_phase', '')} ready={r.get('runtime_ready', '')} | {q} | {r.get('detail', '')}\n")

def _journal_stream_mega_rows_to_file(fh, limit: int=3000, bot_version_filter: str | None=None, since_ts: str | None=None) -> int:
    """Stream durable journal chunks without loading the whole history into RAM.

    bot_version_filter is exact for v124+ rows. since_ts is a compatibility fallback for
    older rows that did not yet carry bot_version. Existing full-journal callers use no filter.
    """
    if not BOT_JOURNAL_DURABLE_ENABLED or not mega_is_configured():
        return 0
    remote_dir = _journal_durable_remote_dir()
    max_files = min(BOT_JOURNAL_DURABLE_RESTORE_FILES, max(8, int(max(1, limit) / max(1, BOT_JOURNAL_DURABLE_FLUSH_ROWS)) + 16))
    try:
        files = _mega_find_remote_files(remote_dir, 'journal_*', max_files)
    except Exception:
        return 0
    count = 0
    seen = set()
    wanted_version = str(bot_version_filter or '').strip()
    since_ts = str(since_ts or '').strip()
    for remote in reversed(files):
        local = None
        try:
            local = _mega_download_remote_path(remote)
            rows = _journal_load_chunk_rows(local) if local else []
            if not rows:
                continue
            for r in rows:
                if not isinstance(r, dict):
                    continue
                if wanted_version:
                    row_version = str(r.get('bot_version') or '').strip()
                    if row_version:
                        if row_version != wanted_version:
                            continue
                    elif since_ts and str(r.get('ts') or '') < since_ts:
                        continue
                key = _journal_row_key(r)
                if key in seen:
                    continue
                seen.add(key)
                _journal_write_export_row(fh, r)
                count += 1
                if count >= int(limit):
                    return count
        except Exception as e:
            fh.write(f'[journal chunk read error] {remote}: {e}\n')
        finally:
            try:
                if local:
                    shutil.rmtree(os.path.dirname(local), ignore_errors=True)
            except Exception:
                pass
    return count
_INTERACTIVE_FILE_JOB_KEY = 'interactive-file-global'
_FILE_JOB_LOCK = threading.RLock()
_FILE_JOB_STATE = {}
_FILE_JOB_CONTEXT = threading.local()

def _file_job_elapsed_text(seconds: float) -> str:
    try:
        total = max(0, int(seconds or 0))
    except Exception:
        total = 0
    h, rem = divmod(total, 3600)
    m, sec = divmod(rem, 60)
    return f'{h:d}:{m:02d}:{sec:02d}' if h else f'{m:d}:{sec:02d}'

def _file_job_current() -> dict:
    try:
        return getattr(_FILE_JOB_CONTEXT, 'value', None) or {}
    except Exception:
        return {}

def _file_job_busy_info() -> dict:
    with _FILE_JOB_LOCK:
        st = dict(_FILE_JOB_STATE.get(_INTERACTIVE_FILE_JOB_KEY) or {})
    if not st:
        return {}
    started = float(st.get('started_monotonic') or st.get('queued_monotonic') or time.monotonic())
    st['elapsed'] = max(0.0, time.monotonic() - started)
    return st

def build_all_processes_toast(chat_id=None) -> str:
    """Compact snapshot of every active business lane for Telegram callback toast."""
    parts = []
    try:
        busy = _file_job_busy_info()
        if busy:
            elapsed = _file_job_elapsed_text(float(busy.get('elapsed') or 0))
            phase = str(busy.get('phase') or 'работаю')
            parts.append(f"📄 {busy.get('label', 'файл')} {elapsed} · {phase}")
    except Exception:
        pass
    pools = (('Сообщ', WEBHOOK_TASK_POOL), ('UI', UI_TASK_POOL), ('Фин', FINANCE_TASK_POOL), ('ФинПерес', FIN_FORWARD_TASK_POOL), ('Перес', FORWARD_TASK_POOL), ('Восст', RECOVERY_TASK_POOL), ('Напом', REMINDER_TASK_POOL), ('Бэкап', BACKUP_TASK_POOL), ('MEGAΔ', DELTA_TASK_POOL), ('Экспорт', EXPORT_TASK_POOL), ('Общие', GENERAL_TASK_POOL), ('Сервис', MAINTENANCE_TASK_POOL), ('Журнал', JOURNAL_TASK_POOL), ('Таймер', DELAYED_TASK_POOL), ('Дозвон', DOZVON_TASK_POOL))
    for label, pool in pools:
        try:
            st = pool.stats()
            active = int(st.get('active', 0) or 0)
            pending = int(st.get('pending', 0) or 0)
            if active or pending:
                parts.append(f'{label} {active}/{pending}')
        except Exception:
            pass
    try:
        mt = globals().get('mega_task_stats')
        if callable(mt):
            st = mt() or {}
            pending = int(st.get('pending', 0) or 0)
            running = int(st.get('running', 0) or 0) + int(st.get('processing', 0) or 0)
            if pending or running:
                parts.append(f'Защита {running}/{pending}')
    except Exception:
        pass
    if not parts:
        return '✅ Активных процессов нет'
    text = '⚙️ ' + ' · '.join(parts)
    return text[:190]

def _v177_legacy_0009_file_job_progress(phase: str, current=None, total=None, force: bool=False):
    """Update one temporary Telegram status message at a throttled rate."""
    ctx = _file_job_current()
    if not ctx:
        return
    key = str(ctx.get('key') or _INTERACTIVE_FILE_JOB_KEY)
    now_m = time.monotonic()
    with _FILE_JOB_LOCK:
        st = _FILE_JOB_STATE.get(key)
        if not isinstance(st, dict):
            return
        st['phase'] = str(phase or 'работаю')
        if current is not None:
            st['current'] = current
        if total is not None:
            st['total'] = total
        last = float(st.get('last_ui_monotonic') or 0.0)
        if not force and now_m - last < 8.0:
            return
        st['last_ui_monotonic'] = now_m
        chat_id = int(st.get('chat_id'))
        msg_id = st.get('status_msg_id')
        label = str(st.get('label') or 'Файл')
        started = float(st.get('started_monotonic') or st.get('queued_monotonic') or now_m)
        elapsed = _file_job_elapsed_text(now_m - started)
        cur = st.get('current')
        tot = st.get('total')
    progress = ''
    if cur is not None and tot is not None:
        progress = f'\nПрогресс: {cur}/{tot}'
    text = f'⏳ {label}\nВремя: {elapsed}\nЭтап: {phase}{progress}\nПовторные нажатия не ставятся в очередь.'
    try:
        if msg_id:
            bot.edit_message_text(text, chat_id=chat_id, message_id=int(msg_id))
    except Exception:
        pass
try:
    _v177_legacy_0009_file_job_progress.__name__ = '_file_job_progress'
except Exception:
    pass

def _v177_legacy_0010_file_job_tick(key: str):
    """Keep elapsed time moving even when the builder is inside one long blocking call."""
    key = str(key)
    with _FILE_JOB_LOCK:
        st = _FILE_JOB_STATE.get(key)
        if not isinstance(st, dict):
            return
        chat_id = int(st.get('chat_id'))
        msg_id = st.get('status_msg_id')
        label = str(st.get('label') or 'Файл')
        phase = str(st.get('phase') or 'работаю')
        started = float(st.get('started_monotonic') or st.get('queued_monotonic') or time.monotonic())
        elapsed = _file_job_elapsed_text(time.monotonic() - started)
        cur = st.get('current')
        tot = st.get('total')
    progress = f'\nПрогресс: {cur}/{tot}' if cur is not None and tot is not None else ''
    try:
        if msg_id:
            bot.edit_message_text(f'⏳ {label}\nВремя: {elapsed}\nЭтап: {phase}{progress}\nПовторные нажатия не ставятся в очередь.', chat_id=chat_id, message_id=int(msg_id))
    except Exception:
        pass
    with _FILE_JOB_LOCK:
        alive = isinstance(_FILE_JOB_STATE.get(key), dict)
    if alive:
        DELAYED_SCHEDULER.schedule(f'file-job-tick:{key}', internal_timer_seconds('process_status_refresh', 10.0), _file_job_tick, key)
try:
    _v177_legacy_0010_file_job_tick.__name__ = '_file_job_tick'
except Exception:
    pass

def _v177_legacy_0012_interactive_file_job_runner(job_meta: dict, func, args, kwargs):
    key = str(job_meta.get('key') or _INTERACTIVE_FILE_JOB_KEY)
    previous = getattr(_FILE_JOB_CONTEXT, 'value', None)
    _FILE_JOB_CONTEXT.value = {'key': key}
    ok = False
    error_text = ''
    try:
        with _FILE_JOB_LOCK:
            st = _FILE_JOB_STATE.get(key)
            if isinstance(st, dict):
                st['started_monotonic'] = time.monotonic()
                st['phase'] = 'запуск'
        _file_job_progress('запуск', force=True)
        mem_ctx = globals().get('memory_operation')
        if callable(mem_ctx):
            with mem_ctx(f"file:{job_meta.get('kind') or 'export'}", {'chat_id': job_meta.get('chat_id'), 'label': job_meta.get('label')}, heavy=True):
                result = func(*args, **kwargs)
        else:
            result = func(*args, **kwargs)
        ok = result is not False
        if not ok:
            error_text = 'операция завершилась без подтверждения'
    except Exception as exc:
        error_text = str(exc)[:300]
        try:
            log_error(f"INTERACTIVE FILE JOB {job_meta.get('kind')}: {exc}")
        except Exception:
            pass
    finally:
        now_m = time.monotonic()
        with _FILE_JOB_LOCK:
            st = _FILE_JOB_STATE.get(key)
            if isinstance(st, dict):
                chat_id = int(st.get('chat_id'))
                msg_id = st.get('status_msg_id')
                label = str(st.get('label') or 'Файл')
                started = float(st.get('started_monotonic') or st.get('queued_monotonic') or now_m)
                elapsed = _file_job_elapsed_text(now_m - started)
            else:
                chat_id = int(job_meta.get('chat_id') or 0)
                msg_id = None
                label = str(job_meta.get('label') or 'Файл')
                elapsed = '0:00'
        try:
            if msg_id:
                final = f'✅ {label}\nГотово за {elapsed}.' if ok else f"⚠️ {label}\nЗавершено за {elapsed}.\n{error_text or 'Telegram не подтвердил отправку.'}"
                bot.edit_message_text(final, chat_id=chat_id, message_id=int(msg_id))
                delete_message_later(chat_id, int(msg_id), 90 if ok else 180)
        except Exception:
            pass
        try:
            bot_journal('file_job_done' if ok else 'file_job_uncertain', chat_id, f"kind={job_meta.get('kind')} elapsed={elapsed} error={error_text}")
        except Exception:
            pass
        try:
            DELAYED_SCHEDULER.cancel(f'file-job-tick:{key}')
        except Exception:
            pass
        with _FILE_JOB_LOCK:
            _FILE_JOB_STATE.pop(key, None)
        if previous is None:
            try:
                delattr(_FILE_JOB_CONTEXT, 'value')
            except Exception:
                pass
        else:
            _FILE_JOB_CONTEXT.value = previous
try:
    _v177_legacy_0012_interactive_file_job_runner.__name__ = '_interactive_file_job_runner'
except Exception:
    pass

def _v177_legacy_0016_submit_interactive_file_job(chat_id: int, kind: str, label: str, func, *args, **kwargs) -> tuple[bool, str]:
    """Start one heavy user-requested file job; duplicate taps are coalesced."""
    chat_id = int(chat_id)
    gate = globals().get('memory_heavy_allowed')
    if callable(gate):
        try:
            allowed, reason = gate(str(kind or 'export'))
        except Exception:
            allowed, reason = (True, '')
        if not allowed:
            try:
                send_and_auto_delete(chat_id, f'🧠 {reason}', 15)
            except Exception:
                pass
            return (False, reason or 'сервер временно разгружает память')
    key = _INTERACTIVE_FILE_JOB_KEY
    with _FILE_JOB_LOCK:
        existing = _FILE_JOB_STATE.get(key)
        if isinstance(existing, dict):
            started = float(existing.get('started_monotonic') or existing.get('queued_monotonic') or time.monotonic())
            elapsed = _file_job_elapsed_text(time.monotonic() - started)
            return (False, build_all_processes_toast(chat_id))
        meta = {'key': key, 'chat_id': chat_id, 'kind': str(kind), 'label': str(label), 'queued_monotonic': time.monotonic(), 'started_monotonic': 0.0, 'phase': 'в очереди', 'status_msg_id': None, 'last_ui_monotonic': 0.0}
        _FILE_JOB_STATE[key] = meta
    try:
        status = bot.send_message(chat_id, f'⏳ {label}\nВремя: 0:00\nЭтап: в очереди\nПовторные нажатия не ставятся в очередь.')
        with _FILE_JOB_LOCK:
            if isinstance(_FILE_JOB_STATE.get(key), dict):
                _FILE_JOB_STATE[key]['status_msg_id'] = int(getattr(status, 'message_id', 0) or 0) or None
    except Exception:
        pass
    ok = EXPORT_TASK_POOL.submit_unique(key, _interactive_file_job_runner, dict(meta), func, args, kwargs)
    if not ok:
        with _FILE_JOB_LOCK:
            _FILE_JOB_STATE.pop(key, None)
        return (False, build_all_processes_toast(chat_id))
    try:
        DELAYED_SCHEDULER.cancel(f'file-job-tick:{key}')
        DELAYED_SCHEDULER.schedule(f'file-job-tick:{key}', internal_timer_seconds('process_status_refresh', 10.0), _file_job_tick, key)
    except Exception:
        pass
    try:
        bot_journal('file_job_queued', chat_id, f'kind={kind} label={label}')
    except Exception:
        pass
    return (True, 'Запущено')
try:
    _v177_legacy_0016_submit_interactive_file_job.__name__ = 'submit_interactive_file_job'
except Exception:
    pass
_JOURNAL_FILENAME_WAIT = {}
_JOURNAL_FILENAME_WAIT_LOCK = threading.RLock()

def _sanitize_journal_download_base(value: str) -> str:
    raw = str(value or '').strip()
    raw = re.sub('\\.txt$', '', raw, flags=re.I).strip()
    raw = re.sub('[\\\\/:*?\\"<>|]+', ' ', raw)
    raw = re.sub('\\s+', '_', raw).strip('._- ')
    raw = re.sub('_+', '_', raw)
    return raw[:64] or 'Журнал_бота'

def _v228_prev_journal_download_base_name() -> str:
    try:
        value = str((data.get('_global_settings', {}) or {}).get('journal_download_base_name') or '')
    except Exception:
        value = ''
    return _sanitize_journal_download_base(value or 'Журнал_бота')

def _v228_prev_set_journal_download_base_name(value: str | None) -> str:
    base = _sanitize_journal_download_base(value or 'Журнал_бота')
    gs = data.setdefault('_global_settings', {})
    gs['journal_download_base_name'] = base
    try:
        save_data(data, full=True)
    except Exception:
        pass
    try:
        if mega_is_configured() and (not RESTORE_GUARD_ACTIVE):
            schedule_config_backup_for_chats(int(OWNER_ID or 0), delay=0.5)
    except Exception:
        pass
    return base

def _v228_prev_journal_download_filename(kind: str='full') -> str:
    base = journal_download_base_name()
    stamp = now_local().strftime('%Y-%m-%d_%H-%M-%S')
    suffix = 'текущая_версия' if str(kind) == 'current' else 'полный'
    return f'{base}_{suffix}_{stamp}.txt'

def _v228_prev_journal_filename_begin_wait(chat_id: int, seconds: float=120.0) -> float:
    deadline = time.monotonic() + max(15.0, float(seconds))
    with _JOURNAL_FILENAME_WAIT_LOCK:
        _JOURNAL_FILENAME_WAIT[int(chat_id)] = deadline
    return deadline

def journal_filename_cancel_wait(chat_id: int) -> None:
    with _JOURNAL_FILENAME_WAIT_LOCK:
        _JOURNAL_FILENAME_WAIT.pop(int(chat_id), None)

def _v228_prev_handle_journal_filename_input(msg) -> bool:
    try:
        chat_id = int(msg.chat.id)
        if not is_owner_chat(chat_id) or str(getattr(msg, 'content_type', '')) != 'text':
            return False
        with _JOURNAL_FILENAME_WAIT_LOCK:
            deadline = float(_JOURNAL_FILENAME_WAIT.get(chat_id) or 0.0)
            if not deadline:
                return False
            if time.monotonic() > deadline:
                _JOURNAL_FILENAME_WAIT.pop(chat_id, None)
                return False
            _JOURNAL_FILENAME_WAIT.pop(chat_id, None)
        raw = str(getattr(msg, 'text', '') or '').strip()
        if raw.upper() in {'СБРОС', 'RESET', 'АВТО', 'ПО УМОЛЧАНИЮ'}:
            base = set_journal_download_base_name('Журнал_бота')
        else:
            base = set_journal_download_base_name(raw)
        try:
            bot.delete_message(chat_id, int(msg.message_id))
        except Exception:
            pass
        send_and_auto_delete(chat_id, f"✅ Имя скачиваемых журналов: {base}\nПример: {journal_download_filename('current')}", 15)
        try:
            bot_journal('journal_download_name_changed_v189', chat_id, f'base={base}')
        except Exception:
            pass
        return True
    except Exception as exc:
        try:
            log_error(f'journal filename input: {exc}')
        except Exception:
            pass
        return False

def _send_journal_file_to_owner_sync(chat_id: int, limit: int=3000):
    """Build/send diagnostics inside EXPORT_TASK_POOL; never block Telegram webhook workers."""
    if not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, '📓 Журнал доступен только владельцу.', HELPER_DELETE_DELAY)
        return
    bot_journal('journal_export_requested', chat_id, f'limit={limit}; streaming=1; memory_guard=1')
    _file_job_progress('фиксирую свежий журнал в MEGA', force=True)
    try:
        journal_flush_to_mega(True)
    except Exception:
        pass
    pressure = _runtime_memory_pressure()
    if str(pressure.get('level')) in {'high', 'critical'}:
        _runtime_emergency_trim('journal_export')
    diag = _journal_diagnostic_snapshot()
    tmp_path = None
    try:
        os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
        tmp_path = os.path.join(MEGA_LOCAL_TMP_DIR, journal_download_filename('full'))
        with open(tmp_path, 'w', encoding='utf-8') as fh:
            fh.write('📓 МАКСИМАЛЬНЫЙ ДИАГНОСТИЧЕСКИЙ ЖУРНАЛ БОТА\n')
            fh.write(f'Создан: {_journal_ts()}\nВерсия: {VERSION}\n')
            fh.write('ВАЖНО: время старта Python != время начала Render deploy.\n')
            fh.write('v123: edit consistency + 💰Перес clean insert + exact edit witnesses; LOW-RAM remains active.\n\n')
            fh.write('==================== CURRENT DIAGNOSTIC SNAPSHOT (JSON) ====================\n')
            json.dump(diag, fh, ensure_ascii=False, indent=2, default=str)
            fh.write('\n\n==================== TRAFFIC AUDIT SNAPSHOT ====================\n')
            try:
                traffic_snap_fn = globals().get('traffic_audit_snapshot')
                traffic_text_fn = globals().get('traffic_audit_text')
                traffic_snap = traffic_snap_fn() if callable(traffic_snap_fn) else {'available': False}
                json.dump(traffic_snap, fh, ensure_ascii=False, indent=2, default=str)
                if callable(traffic_text_fn):
                    for _scope in ('process', 'today', 'month'):
                        fh.write(f'\n\n--- {_scope.upper()} ---\n')
                        fh.write(str(traffic_text_fn(_scope) or ''))
            except Exception as e:
                fh.write(f'traffic audit snapshot unavailable: {e}')
            fh.write('\n\n==================== DURABLE JOURNAL ====================\n')
            json.dump(journal_durable_stats(), fh, ensure_ascii=False, indent=2, default=str)
            fh.write('\n\n==================== MEMORY FORENSICS ====================\n')
            try:
                mem_fn = globals().get('memory_forensics_snapshot')
                json.dump(mem_fn(deep=True) if callable(mem_fn) else {'available': False}, fh, ensure_ascii=False, indent=2, default=str)
            except Exception as e:
                fh.write(f'memory forensics unavailable: {e}')
            fh.write('\n\n==================== RUNTIME EVENTS ====================\n')
            try:
                with _RUNTIME_LOCK:
                    runtime_rows = list(_RUNTIME_EVENTS)
                for r in runtime_rows:
                    fh.write(f"{r.get('ts', '')} | {r.get('level', '')} | {r.get('event', '')} | {r.get('detail', '')}\n")
            except Exception as e:
                fh.write(f'runtime events unavailable: {e}\n')
            fh.write('\n==================== WINDOW MUTATION TRACE ====================\n')
            try:
                window_tail_fn = globals().get('window_diagnostic_tail')
                window_rows = window_tail_fn(800) if callable(window_tail_fn) else []
                for row in window_rows:
                    fh.write(json.dumps(row, ensure_ascii=False, default=str, separators=(',', ':')) + '\n')
                fh.write(f'[window trace rows: {len(window_rows)}]\n')
            except Exception as e:
                fh.write(f'window diagnostics unavailable: {e}\n')
            fh.write('\n==================== ACTION JOURNAL (MEGA STREAM) ====================\n')
            _file_job_progress('читаю журнал из MEGA', force=True)
            remote_count = _journal_stream_mega_rows_to_file(fh, int(limit))
            fh.write(f'\n[MEGA rows streamed: {remote_count}]\n')
            fh.write('\n==================== CURRENT PROCESS TAIL ====================\n')
            seen_tail = set()
            for r in _journal_merge_rows(_journal_read_file_rows(300), get_recent_journal(300), limit=600):
                key = _journal_row_key(r)
                if key in seen_tail:
                    continue
                seen_tail.add(key)
                _journal_write_export_row(fh, r)
            fh.write('\n==================== INTERPRETATION KEYS ====================\n')
            fh.write('deploy_new_commit_*: Git commit changed — strong deploy evidence.\n')
            fh.write('planned_restart_same_commit: same commit + graceful SIGTERM.\n')
            fh.write('probable_render_idle_*: probable sleep/wake estimate.\n')
            fh.write('probable_memory_oom_or_hard_kill: cgroup OOM event or memory peak >=90% of limit.\n')
            fh.write('process_restart_or_unknown + high RAM/no SIGTERM: suspect OOM/hard kill.\n')
            fh.write('v125: fast 3-day 💰Перес repaint, global Excel mode and verified Notes/Comments separation.\n')
            fh.write('window_stale_edit_apply: отложенное старое обновление применилось после другого изменения этого сообщения.\n')
            fh.write('window_recreated: старое окно не удалось изменить, поэтому бот создал новое сообщение.\n')
            fh.write('window_duplicate_marker_candidate: одновременно обнаружены два окна с одинаковой меткой.\n')
            fh.write('window_registry_changed: реестр переименовал/переклассифицировал то же Telegram-сообщение.\n')
        _file_job_progress('отправляю файл в Telegram', force=True)
        with open(tmp_path, 'rb') as fh:
            _tg_call_retry(bot.send_document, chat_id, fh, caption='📓 Максимальный журнал: Render + бот + очереди + MEGA (single-flight v120)', timeout=120, purpose='journal_send_document')
        return True
    finally:
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
O9_SECRET_CLICK_WINDOW_SECONDS = 3.0
O9_SECRET_WAIT_SECONDS = 90
O9_SECRET_WAIT_COUNTDOWN_STEP_SECONDS = 30
_o9_secret_clicks = {}
_o9_secret_click_lock = threading.RLock()
_o9_secret_action_timers = {}
_o9_secret_wait_timers = {}

def send_journal_file_to_owner(chat_id: int, limit: int=3000):
    """Queue one maximum journal export; repeated taps are coalesced."""
    if not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, '📓 Журнал доступен только владельцу.', HELPER_DELETE_DELAY)
        return False
    ok, info = submit_interactive_file_job(int(chat_id), 'journal', 'Диагностический журнал', _send_journal_file_to_owner_sync, int(chat_id), int(limit))
    if not ok:
        try:
            send_and_auto_delete(chat_id, f'⏳ {info}. Новая копия в очередь не добавлена.', 10)
        except Exception:
            pass
        return False
    return True
BOT_SOURCE_ARCHIVE_DIR = os.getenv('MEGA_BOT_SOURCE_ARCHIVE_DIR', f"{MEGA_BACKUP_DIR.rstrip('/')}/runtime/bot_versions").strip() or f"{MEGA_BACKUP_DIR.rstrip('/')}/runtime/bot_versions"

def _current_version_journal_since_ts() -> str:
    """Compatibility lower bound for v123-and-older rows without a bot_version field."""
    try:
        return str((globals().get('_RUNTIME_STATE') or {}).get('started_at') or '')[:23].replace('T', ' ')
    except Exception:
        return ''

def _send_current_version_journal_to_owner_sync(chat_id: int, limit: int=5000):
    tmp_path = None
    try:
        _file_job_progress('фиксирую последние строки журнала', force=True)
        try:
            journal_flush_to_mega(True)
        except Exception:
            pass
        os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
        tmp_path = os.path.join(MEGA_LOCAL_TMP_DIR, journal_download_filename('current'))
        with open(tmp_path, 'w', encoding='utf-8') as fh:
            fh.write('📓 ЖУРНАЛ ТЕКУЩЕЙ ВЕРСИИ БОТА\n')
            fh.write(f'Версия: {VERSION}\n')
            fh.write(f"Commit: {str(os.getenv('RENDER_GIT_COMMIT', '') or '—')}\n")
            fh.write(f"Текущий Python start: {str((globals().get('_RUNTIME_STATE') or {}).get('started_at') or '—')}\n")
            fh.write(f"Создан: {now_local().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}\n")
            fh.write('=' * 78 + '\n')
            _file_job_progress('читаю журнал этой версии из MEGA', force=True)
            count = _journal_stream_mega_rows_to_file(fh, int(limit), bot_version_filter=str(VERSION), since_ts=_current_version_journal_since_ts())
            fh.write('=' * 78 + '\n')
            fh.write(f'Строк: {count}\n')
            fh.write('\nRUNTIME EVENTS CURRENT PROCESS\n')
            for ev in list(globals().get('_RUNTIME_EVENTS') or []):
                if isinstance(ev, dict):
                    fh.write(f"{ev.get('ts', '')} | {ev.get('level', '')} | {ev.get('event', '')} | {ev.get('detail', '')}\n")
        _file_job_progress('отправляю журнал текущей версии', force=True)
        with open(tmp_path, 'rb') as fh:
            _tg_call_retry(bot.send_document, int(chat_id), fh, caption=f'📓 Журнал текущей версии: {VERSION}', timeout=120, purpose='current_version_journal_send')
        return True
    finally:
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

def send_current_version_journal_to_owner(chat_id: int, limit: int=5000):
    if not is_owner_chat(chat_id):
        return False
    ok, info = submit_interactive_file_job(int(chat_id), 'journal_current', 'Журнал текущей версии', _send_current_version_journal_to_owner_sync, int(chat_id), int(limit))
    if not ok:
        send_and_auto_delete(int(chat_id), f'⏳ {info}. Новая копия в очередь не добавлена.', 10)
    return bool(ok)

def _send_current_bot_source_sync(chat_id: int):
    path = _current_source_path()
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    _file_job_progress('читаю исходник текущего деплоя', force=True)
    fobj = file_bytesio_named(path, f'{VERSION}.py')
    if not fobj:
        raise RuntimeError('Не удалось открыть исходник текущего деплоя')
    _file_job_progress('отправляю исходник текущего деплоя', force=True)
    _tg_call_retry(bot.send_document, int(chat_id), fobj, caption=f"🤖 Исходник текущего деплоя\n{VERSION}\ncommit {str(os.getenv('RENDER_GIT_COMMIT', '') or '—')[:12]}", timeout=120, purpose='current_bot_source_send')
    return True

def send_current_bot_source_to_owner(chat_id: int):
    if not is_owner_chat(chat_id):
        return False
    ok, info = submit_interactive_file_job(int(chat_id), 'bot_source', 'Исходник текущего деплоя', _send_current_bot_source_sync, int(chat_id))
    if not ok:
        send_and_auto_delete(int(chat_id), f'⏳ {info}. Новая копия в очередь не добавлена.', 10)
    return bool(ok)

def archive_current_bot_source_to_mega() -> bool:
    """One immutable name per VERSION+commit, so the running source survives later deploys."""
    if not mega_is_configured():
        return False
    path = _current_source_path()
    if not os.path.exists(path):
        return False
    commit = re.sub('[^0-9A-Za-z]+', '', str(os.getenv('RENDER_GIT_COMMIT', '') or ''))[:16] or 'no_commit'
    safe_ver = re.sub('[^0-9A-Za-z_\\-]+', '_', str(VERSION))[:90]
    remote_name = f'{safe_ver}__{commit}.py'
    ok = mega_put_replace(path, BOT_SOURCE_ARCHIVE_DIR, remote_name, archive_previous=False)
    try:
        bot_journal('bot_source_archive', None, f'ok={ok} file={remote_name}')
    except Exception:
        pass
    return bool(ok)

def _secret_notes_list() -> list:
    try:
        arr = data.setdefault('_secret_notes', [])
        if not isinstance(arr, list):
            data['_secret_notes'] = []
            arr = data['_secret_notes']
        return arr
    except Exception:
        return []

def _secret_notes_local_path() -> str:
    try:
        os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
        return os.path.join(MEGA_LOCAL_TMP_DIR, 'secret_notes_owner.json')
    except Exception:
        return 'secret_notes_owner.json'

def _save_secret_notes_plain_to_file() -> str | None:
    try:
        payload = {'kind': 'owner_secret_notes_plain_text', 'version': VERSION, 'created_at': now_local().isoformat(timespec='seconds'), 'warning': 'plain text, not encrypted', 'notes': _secret_notes_list()}
        path = _secret_notes_local_path()
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return path
    except Exception as e:
        log_error(f'_save_secret_notes_plain_to_file: {e}')
        return None

def upload_secret_notes_to_mega() -> bool:
    """Совместимость: секреты О9 теперь идут в единый файл чата владельца."""
    try:
        return bool(OWNER_ID and upload_chat_secrets_to_mega(int(OWNER_ID)))
    except Exception as e:
        log_error(f'upload_secret_notes_to_mega: {e}')
        return False

def _is_o9_owner_call(call) -> bool:
    try:
        chat_id = int(call.message.chat.id)
        if not is_owner_chat(chat_id):
            return False
        msg_id = int(call.message.message_id)
        store = get_chat_store(chat_id)
        if int(store.get('info_msg_id') or 0) == msg_id:
            return True
        text = getattr(call.message, 'text', None) or getattr(call.message, 'caption', None) or ''
        return bool(re.search('(?:^|\\s)о9\\s*$', str(text or '')[-160:]))
    except Exception:
        return False

def _o9_action_scheduler_key(key) -> str:
    try:
        return f'o9-action:{int(key[0])}:{int(key[1])}:{str(key[2])}'
    except Exception:
        return f'o9-action:{str(key)}'

def _cancel_o9_secret_timer(key):
    try:
        _o9_secret_action_timers.pop(key, None)
        DELAYED_SCHEDULER.cancel(_o9_action_scheduler_key(key))
    except Exception:
        pass

def _format_mmss(seconds: int) -> str:
    seconds = max(0, int(seconds))
    return f'{seconds // 60:02d}:{seconds % 60:02d}'

def _secret_wait_keyboard(chat_id: int, remaining: int=O9_SECRET_WAIT_SECONDS):
    kb = types.InlineKeyboardMarkup()
    store = get_chat_store(chat_id)
    kb.row(IB(f'❌ Закрыть {_format_mmss(remaining)}', callback_data='secret_cancel'), IB('⬅️ Назад осн. окно', callback_data=f"d:{store.get('current_view_day', today_key())}:back_main"))
    return kb

def _secret_wait_prompt_text(remaining: int | None=None) -> str:
    tail = ''
    if remaining is not None:
        tail = f'\n\n⏳ Осталось: {_format_mmss(remaining)}'
    return wm_common('🔐 Секретные данные\n\nОтправь одним сообщением текст, который нужно сохранить.\nБот удалит твоё сообщение после сохранения.\n\nВажно: сейчас хранение обычным текстом, без шифрования.' + tail, 9)

def _cancel_o9_secret_wait_timer(chat_id: int):
    key = int(chat_id)
    with _o9_secret_click_lock:
        item = _o9_secret_wait_timers.get(key)
        if isinstance(item, dict):
            item['cancelled'] = True
        _o9_secret_wait_timers.pop(key, None)
    try:
        DELAYED_SCHEDULER.cancel(f'o9-secret-wait:{key}')
    except Exception:
        pass

def schedule_o9_secret_wait_timeout(chat_id: int, prompt_message_id: int, delay: int=O9_SECRET_WAIT_SECONDS):
    """Автоотмена ожидания секрета без частого редактирования таймера."""
    key = int(chat_id)
    with _o9_secret_click_lock:
        prev = _o9_secret_wait_timers.get(key)
        if isinstance(prev, dict):
            prev['cancelled'] = True
        generation = int(time.time() * 1000)
        token = {'generation': generation, 'cancelled': False}
        _o9_secret_wait_timers[key] = token

    def _job():
        try:
            with _o9_secret_click_lock:
                current = _o9_secret_wait_timers.get(key)
                if current is not token or token.get('cancelled'):
                    return
                _o9_secret_wait_timers.pop(key, None)
            _clear_secret_wait(chat_id, delete_prompt=True)
            send_and_auto_delete(chat_id, '⌛ Время принятия секретных данных истекло.', 8)
        except Exception as e:
            log_error(f'schedule_o9_secret_wait_timeout({chat_id},{prompt_message_id}): {e}')
    DELAYED_SCHEDULER.schedule(f'o9-secret-wait:{key}', int(delay), _job)

def _o9_delayed_close(chat_id: int, message_id: int, key):
    try:
        with _o9_secret_click_lock:
            item = _o9_secret_clicks.get(key) or {}
            if int(item.get('count', 0) or 0) >= 3:
                return
            _o9_secret_clicks.pop(key, None)
            _o9_secret_action_timers.pop(key, None)
        try:
            bot.delete_message(chat_id, message_id)
        except Exception:
            pass
        try:
            _clear_secret_wait(chat_id, delete_prompt=False)
        except Exception:
            pass
        try:
            _clear_stored_window(chat_id, 'info_msg_id', message_id)
        except Exception:
            pass
    except Exception as e:
        log_error(f'_o9_delayed_close: {e}')

def _o9_delayed_back_main(chat_id: int, message_id: int, day_key: str, key):
    try:
        with _o9_secret_click_lock:
            item = _o9_secret_clicks.get(key) or {}
            if int(item.get('count', 0) or 0) >= 3:
                return
            _o9_secret_clicks.pop(key, None)
            _o9_secret_action_timers.pop(key, None)
        try:
            cancel_pending_window_commands(chat_id, delete_prompt=False)
        except Exception:
            pass
        try:
            day_key = day_key or get_chat_store(chat_id).get('current_view_day') or today_key()
            txt, _ = render_day_window(chat_id, day_key)
            bot.edit_message_text(txt, chat_id=chat_id, message_id=message_id, reply_markup=build_main_keyboard(day_key, chat_id), parse_mode='HTML')
            try:
                set_active_window_id(chat_id, day_key, message_id)
            except Exception:
                pass
            try:
                _clear_stored_window(chat_id, 'info_msg_id', message_id)
            except Exception:
                pass
        except Exception as e:
            log_error(f'_o9_delayed_back_main edit failed: {e}')
            try:
                txt, _ = render_day_window(chat_id, day_key)
                sent = _tg_call_retry(bot.send_message, chat_id, txt, reply_markup=build_main_keyboard(day_key, chat_id), parse_mode='HTML', purpose='o9_secret_back_send_main')
                try:
                    set_active_window_id(chat_id, day_key, sent.message_id)
                except Exception:
                    pass
            except Exception as e2:
                log_error(f'_o9_delayed_back_main send main failed: {e2}')
    except Exception as e:
        log_error(f'_o9_delayed_back_main: {e}')

def _start_secret_wait(chat_id: int, message_id: int | None=None):
    try:
        store = get_chat_store(chat_id)
        store['secret_wait'] = {'type': 'secret_note_add', 'started_at': now_local().isoformat(timespec='seconds'), 'window_msg_id': int(message_id or 0)}
        save_data(data)
        kb = _secret_wait_keyboard(chat_id, O9_SECRET_WAIT_SECONDS)
        text = _secret_wait_prompt_text(O9_SECRET_WAIT_SECONDS)
        if message_id:
            try:
                bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
                store['secret_wait']['prompt_msg_id'] = int(message_id)
                save_data(data)
                schedule_o9_secret_wait_timeout(chat_id, int(message_id), O9_SECRET_WAIT_SECONDS)
                return
            except Exception:
                pass
        sent = _tg_call_retry(bot.send_message, chat_id, text, reply_markup=kb, purpose='secret_prompt')
        store['secret_wait']['prompt_msg_id'] = sent.message_id
        save_data(data)
        schedule_o9_secret_wait_timeout(chat_id, sent.message_id, O9_SECRET_WAIT_SECONDS)
    except Exception as e:
        log_error(f'_start_secret_wait({chat_id}): {e}')

def _format_secret_notes_text() -> str:
    notes = _secret_notes_list()
    if not notes:
        return '🔐 Секретные данные\n\nПока пусто.'
    lines = ['🔐 Секретные данные', '']
    for i, item in enumerate(notes, start=1):
        ts = str((item or {}).get('ts') or '')
        body = str((item or {}).get('text') or '')
        lines.append(f'{i}. {ts}\n{body}')
        lines.append('')
    text = '\n'.join(lines).strip()
    if len(text) > 3900:
        text = text[-3900:]
        text = '🔐 Секретные данные (последняя часть)\n\n' + text
    return text

def _send_secret_notes_to_owner(chat_id: int, message_id: int | None=None):
    try:
        open_secret_day_window(chat_id, chat_id, message_id=message_id)
    except Exception as e:
        log_error(f'_send_secret_notes_to_owner({chat_id}): {e}')

def _clear_secret_wait(chat_id: int, delete_prompt: bool=False):
    try:
        _cancel_o9_secret_wait_timer(chat_id)
        store = get_chat_store(chat_id)
        wait = store.get('secret_wait') or {}
        msg_id = int(wait.get('prompt_msg_id') or wait.get('window_msg_id') or 0)
        store['secret_wait'] = None
        save_data(data)
        if delete_prompt and msg_id:
            try:
                bot.delete_message(chat_id, msg_id)
            except Exception:
                pass
            try:
                _clear_stored_window(chat_id, 'info_msg_id', msg_id)
            except Exception:
                pass
    except Exception as e:
        log_error(f'_clear_secret_wait({chat_id}): {e}')

def handle_secret_note_message(msg) -> bool:
    """Сохраняет секретное сообщение владельца и удаляет исходный текст."""
    try:
        if getattr(msg, 'content_type', None) != 'text':
            return False
        chat_id = int(msg.chat.id)
        if not is_owner_chat(chat_id):
            return False
        store = get_chat_store(chat_id)
        wait = store.get('secret_wait')
        if not wait or wait.get('type') != 'secret_note_add':
            return False
        text = (msg.text or '').strip()
        if not text:
            return True
        save_secret_message(chat_id, msg, cleaned_text=text)
        delete_secret_source_message(msg)
        _clear_secret_wait(chat_id, delete_prompt=True)
        status = '✅ Секрет сохранён в единый файл чата и поставлен в очередь MEGA.'
        sent = _tg_call_retry(bot.send_message, chat_id, status, purpose='secret_saved_notice')
        try:
            delete_message_later(chat_id, sent.message_id, 12)
        except Exception:
            pass
        return True
    except Exception as e:
        log_error(f'handle_secret_note_message: {e}')
        return True

def _v177_legacy_0020_handle_o9_secret_triple_click(call, data_str: str) -> bool:
    """Перехватывает О9: Закрыть ×3 = ввод секрета, Назад ×3 = показать секреты."""
    try:
        if not _is_o9_owner_call(call):
            return False
        chat_id = int(call.message.chat.id)
        msg_id = int(call.message.message_id)
        kind = None
        day_key = get_chat_store(chat_id).get('current_view_day', today_key())
        if data_str == 'info_close':
            kind = 'close'
        elif str(data_str or '').startswith('d:'):
            parts = str(data_str).split(':', 2)
            action = parts[2] if len(parts) >= 3 else ''
            if action == 'back_main':
                kind = 'back'
                day_key = parts[1] or day_key
        if not kind:
            return False
        key = (chat_id, msg_id, kind)
        now_ts = time.time()
        with _o9_secret_click_lock:
            item = _o9_secret_clicks.get(key) or {'count': 0, 'ts': 0}
            if now_ts - float(item.get('ts', 0) or 0) > O9_SECRET_CLICK_WINDOW_SECONDS:
                item = {'count': 0, 'ts': 0}
            item['count'] = int(item.get('count', 0) or 0) + 1
            item['ts'] = now_ts
            _o9_secret_clicks[key] = item
            _cancel_o9_secret_timer(key)
            count = int(item['count'])
            if count < 3:
                scheduler_key = _o9_action_scheduler_key(key)
                if kind == 'close':
                    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, O9_SECRET_CLICK_WINDOW_SECONDS + 0.2, _o9_delayed_close, chat_id, msg_id, key)
                else:
                    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, O9_SECRET_CLICK_WINDOW_SECONDS + 0.2, _o9_delayed_back_main, chat_id, msg_id, day_key, key)
                _o9_secret_action_timers[key] = deadline
        if count >= 3:
            _cancel_o9_secret_timer(key)
            with _o9_secret_click_lock:
                _o9_secret_clicks.pop(key, None)
            if kind == 'close':
                _start_secret_wait(chat_id, msg_id)
                try:
                    bot.answer_callback_query(call.id, '🔐 Секретные данные')
                except Exception:
                    pass
            else:
                _send_secret_notes_to_owner(chat_id, msg_id)
                try:
                    bot.answer_callback_query(call.id, '🔐 Отправил секретные данные')
                except Exception:
                    pass
            return True
        try:
            bot.answer_callback_query(call.id, f'Секрет: {count}/3', show_alert=False)
        except Exception:
            pass
        return True
    except Exception as e:
        log_error(f'handle_o9_secret_triple_click: {e}')
        return False
try:
    _v177_legacy_0020_handle_o9_secret_triple_click.__name__ = 'handle_o9_secret_triple_click'
except Exception:
    pass

def format_error_for_owner(raw) -> str:
    """Для /errors: по возможности заменяет известные chat_id на имена чатов/пользователей."""
    text = str(raw or '')
    try:
        ids = set()
        for cid in (data.get('chats', {}) or {}).keys():
            try:
                ids.add(str(int(cid)))
            except Exception:
                pass
        if OWNER_ID:
            try:
                ids.add(str(int(OWNER_ID)))
            except Exception:
                pass
        if BACKUP_CHAT_ID:
            try:
                ids.add(str(int(BACKUP_CHAT_ID)))
            except Exception:
                pass
        for cid_s in sorted(ids, key=len, reverse=True):
            try:
                name = get_chat_display_name(int(cid_s))
            except Exception:
                continue
            if not name or name == f'Чат {cid_s}':
                continue
            text = re.sub(f'(?<!\\\\d){re.escape(cid_s)}(?!\\\\d)', name, text)
    except Exception:
        pass
    return text

def get_tz():
    """Return local timezone, with fallback to UTC-3."""
    try:
        return ZoneInfo(DEFAULT_TZ)
    except Exception:
        return timezone(timedelta(hours=-3))

def now_local():
    return datetime.now(get_tz())

def today_key() -> str:
    return now_local().strftime('%Y-%m-%d')
WINDOW_MARK_RE = re.compile('(?:^|\\s)([СФП]\\d{1,6}|[ов]\\d{1,3})(?:-\\(W[A-Z0-9]{6,12}\\))?(?:\\s*[⏳⏰])?\\s*$', re.IGNORECASE)
_WINDOW_MARK_GROUPS = ('С', 'Ф', 'П')
WINDOW_MARKER_CONSTANTS = {'br:open': 'Ф54', 'br:p:*': 'Ф54', 'br:c:*': 'Ф54', 'br:t:*': 'Ф54', 'br:changes': 'Ф54', 'br:dl': 'Ф233', 'v153:json:menu': 'Ф168', 'v153:json:global': 'Ф233', 'v153:json:tenant:*': 'Ф233', 'v237:storage:*': 'Ф54', 'v238:storage:*': 'Ф54', 'v240:modes:*': 'Ф54', 'v176:back_info': 'Ф54', 'v255:constitution:loss_toggle': 'Ф233', 'forward_menu_style_toggle': 'П1', 'fw_': 'П2', 'fw_back_root': 'П3', 'fw_back_src': 'П4', 'fw_back_tgt:*': 'П5', 'fw_finpair:*': 'П6', 'fw_finpair:*:ab': 'П7', 'fw_finpair:*:ba': 'П8', 'fw_mode:*': 'П9', 'fw_mode:*:del': 'П10', 'fw_mode:*:from': 'П11', 'fw_mode:*:to': 'П12', 'fw_mode:*:two': 'П13', 'fw_new_back_src': 'П14', 'fw_new_clear:*': 'П15', 'fw_new_fin:*': 'П16', 'fw_new_fin:*:ab': 'П17', 'fw_new_fin:*:ba': 'П18', 'fw_new_mode:*': 'П19', 'fw_new_mode:*:from': 'П20', 'fw_new_mode:*:to': 'П21', 'fw_new_mode:*:two': 'П22', 'fw_new_pair:*': 'П23', 'fw_new_src:*': 'П24', 'fw_new_tgt:*': 'П25', 'fw_open': 'П26', 'fw_probe_all': 'П27', 'fw_probe_one:*': 'П28', 'fw_removed_list': 'П29', 'fw_src:*': 'П30', 'fw_tgt:*': 'П31', 'secbacklist': 'С1', 'secchatcal:*': 'С2', 'secclose': 'С3', 'secday:*': 'С4', 'secdel:*': 'С5', 'secdelgo:*': 'С6', 'secdelt:*': 'С7', 'secedit:*': 'С8', 'secedfull:*': 'С8', 'secedselected:*': 'С9', 'secedtoggle:*': 'С10', 'seclist:*': 'С11', 'secmclose': 'С12', 'secmedia:*': 'С13', 'secmon:*': 'С14', 'secmonthlist:*': 'С15', 'secmwait': 'С16', 'secret_cancel': 'С17', 'sectoggle:*': 'С18', 'secview:*': 'С19', 'total_secret_mask_toggle': 'С20', 'additional_owners': 'Ф1', 'addown:*': 'Ф2', 'v168:owners_circle:*': 'Ф2', 'articles_desc': 'Ф3', 'aux_close': 'Ф4', 'bp:collapse': 'Ф5', 'bp:open': 'Ф6', 'buttons_current_toggle': 'Ф7', 'c:*': 'Ф8', 'cat_': 'Ф9', 'cat_add': 'Ф10', 'cat_add_cancel': 'Ф11', 'cat_close': 'Ф12', 'cat_del_menu': 'Ф13', 'cat_del_selected': 'Ф14', 'cat_del_toggle:*': 'Ф15', 'cat_desc': 'Ф16', 'cat_edit_menu': 'Ф17', 'cat_edit_pick:*': 'Ф18', 'cat_m:*': 'Ф19', 'cat_months': 'Ф20', 'cat_months_y:*': 'Ф21', 'cat_pick_end2:*': 'Ф22', 'cat_pick_end:*': 'Ф23', 'cat_pick_set_end2:*': 'Ф24', 'cat_pick_set_end:*': 'Ф25', 'cat_pick_set_start:*': 'Ф26', 'cat_pick_start:*': 'Ф27', 'cat_range_custom2:*': 'Ф28', 'cat_range_custom:*': 'Ф29', 'cat_rng:*': 'Ф30', 'cat_show:*': 'Ф31', 'cat_show_wk:*': 'Ф32', 'cat_show_wthu:*': 'Ф33', 'cat_today': 'Ф34', 'cat_wk:*': 'Ф35', 'cat_wthu:*': 'Ф36', 'catx:*': 'Ф37', 'cbx:*': 'Ф38', 'd:*': 'Ф39', 'd:*:back_main': 'Ф40', 'd:*:backup_menu': 'Ф41', 'd:*:bk_channel': 'Ф42', 'd:*:bk_chat': 'Ф43', 'd:*:bk_mega': 'Ф44', 'd:*:calendar': 'Ф45', 'd:*:cancel_edit': 'Ф46', 'd:*:csv_all': 'Ф47', 'd:*:del_selected': 'Ф48', 'd:*:edit_list': 'Ф49', 'd:*:edit_menu': 'Ф50', 'd:*:fin_windows_menu': 'Ф51', 'd:*:forward_finmode_menu': 'Ф52', 'd:*:forward_menu': 'Ф53', 'd:*:info': 'Ф54', 'd:*:next': 'Ф55', 'd:*:open': 'Ф56', 'd:*:prev': 'Ф57', 'd:*:report': 'Ф59', 'd:*:usd_month': 'Ф177', 'd:*:today': 'Ф60', 'd:*:total': 'Ф61', 'dzv:*': 'Ф62', 'dzv:close': 'Ф63', 'fc:*': 'Ф64', 'finance:plain_window': 'Ф65', 'finance_day5_toggle': 'Ф66', 'fv:*': 'Ф67', 'fv:*:bk_channel:*': 'Ф68', 'fv:*:bk_chat:*': 'Ф69', 'fv:*:bk_mega:*': 'Ф70', 'fv:*:calendar:*': 'Ф71', 'fv:*:cancel_edit:*': 'Ф72', 'fv:*:clear_delete_back:*': 'Ф73', 'fv:*:csv_menu:*': 'Ф74', 'fv:*:del_selected:*': 'Ф75', 'fv:*:edit_list:*': 'Ф76', 'fv:*:info:*': 'Ф77', 'fv:*:open:*': 'Ф78', 'fv:*:report:*': 'Ф79', 'fv:*:usd_month:*': 'Ф178', 'fv:*:reset:*': 'Ф80', 'fv:*:total:*': 'Ф81', 'fvcat_': 'Ф82', 'fvcatx:*': 'Ф83', 'icon_buttons_toggle': 'Ф84', 'restore_guard_toggle': 'Ф165', 'mega_manual_restore': 'Ф166', 'v234:config:*': 'Ф233', 'v242:mdb:*': 'Ф233', 'main_close:*': 'Ф167', 'runtime_watcher': 'Ф168', 'runtime_events': 'Ф169', 'runtime_snapshot_now': 'Ф170', 'excel_style_toggle': 'Ф171', 'excel_style_menu': 'Ф171', 'excel_style_set:*': 'Ф171', 'runtime_export': 'Ф172', 'info_close': 'Ф85', 'info_finance_off': 'Ф86', 'journal_back': 'Ф87', 'journal_file': 'Ф88', 'journal_current_file': 'Ф173', 'journal_bot_source': 'Ф174', 'fwdcopy_edit_copy': 'Ф175', 'itxt:*': 'Ф176', 'journal_open': 'Ф89', 'journal_compact_toggle': 'Ф89', 'journal_compact_interval_menu': 'Ф89', 'journal_compact_interval:*': 'Ф89', 'journal_verbose_toggle': 'Ф89', 'journal_name_edit': 'Ф89', 'journal_name_cancel': 'Ф89', 'journal_name_reset': 'Ф89', 'traffic_audit': 'Ф9998', 'traffic_audit:*': 'Ф9998', 'journal_toggle': 'Ф90', 'legacy_common:*': 'Ф91', 'legacy_owner:*': 'Ф92', 'markup:plain': 'Ф93', 'ncb:*': 'Ф94', 'ncb:*:no': 'Ф95', 'ncb:*:yes': 'Ф96', 'none': 'Ф97', 'ojr:*': 'Ф98', 'ojr:*:no': 'Ф99', 'ojr:*:yes': 'Ф100', 'rep:*': 'Ф101', 'rep_close': 'Ф102', 'rep_today': 'Ф103', 'cat_pick_start_record:*': 'Ф104', 'cat_pick_end3:*': 'Ф105', 'cat_pick_set_end3:*': 'Ф106', 'cat_pick_end_record:*': 'Ф107', 'cat_range_records:*': 'Ф110', 'cat_show_records:*': 'Ф109', 'cat_back_records:*': 'Ф149', 'exp_pick_start:*': 'Ф111', 'exp_pick_set_start:*': 'Ф112', 'exp_pick_start_record:*': 'Ф113', 'exp_pick_end:*': 'Ф114', 'exp_pick_set_end:*': 'Ф115', 'exp_pick_end_record:*': 'Ф116', 'exp_style_period:*': 'Ф179', 'exp_send_period_style:*': 'Ф180', 'exp_style_exact:*': 'Ф181', 'exp_send_exact_style:*': 'Ф182', 'internal_timers': 'Ф183', 'itmr_pick:*': 'Ф184', 'itmr_digit:*': 'Ф185', 'itmr_unit:*': 'Ф186', 'itmr_backspace': 'Ф187', 'itmr_clear': 'Ф188', 'itmr_apply': 'Ф189', 'itmr_back_info': 'Ф190', 'exp_new_period_toggle:*': 'Ф179', 'exp_new_period_send:*': 'Ф180', 'exp_new_exact_toggle:*': 'Ф181', 'exp_new_exact_send:*': 'Ф182', 'exp_send:*:csv:*': 'Ф117', 'exp_send:*:xlsx:*': 'Ф118', 'd:*:backup_mass_chat': 'Ф119', 'd:*:backup_mass_channel': 'Ф120', 'd:*:backup_mass_mega': 'Ф121', 'cat_prompt_back': 'Ф122', 'info_instruction': 'Ф123', 'info_queues': 'Ф124', 'mega_priority_toggle': 'Ф125', 'journal_chats_open': 'Ф126', 'journal_chats_open:*': 'Ф127', 'journal_chat_toggle:*': 'Ф128', 'journal_chats_back': 'Ф129', 'main_articles_toggle': 'Ф130', 'cat_main_edit:*': 'Ф131', 'version_menu': 'Ф132', 'version_page:*': 'Ф132', 'version_select:*': 'Ф133', 'version_back': 'Ф134', 'main_financial_values_toggle': 'Ф135', 'keepalive_status': 'Ф136', 'gomonk_open': 'Ф137', 'gomonk_open:*': 'Ф137', 'gomonk_toggle': 'Ф138', 'gomonk_toggle:*': 'Ф138', 'gomonk_back': 'Ф139', 'gomonk_back:*': 'Ф139', 'remaining_open:*': 'Ф140', 'remaining_toggle:*': 'Ф141', 'cat_pick_today_start': 'Ф142', 'cat_usd_toggle_records:*': 'Ф143', 'usd_display_toggle': 'Ф144', 'currency_menu': 'Ф145', 'currency_select:*': 'Ф146', 'currency_back': 'Ф147', 'info_delta_status': 'Ф148', 'cat_usd_toggle_period:*': 'Ф150', 'cat_order_open_sum:*': 'Ф151', 'cat_order_select_sum:*': 'Ф151', 'cat_order_position_sum:*': 'Ф151', 'cat_order_open_exact:*': 'Ф152', 'cat_order_select_exact:*': 'Ф152', 'cat_order_position_exact:*': 'Ф152', 'cat_order_move_sum:*': 'Ф153', 'cat_order_move_exact:*': 'Ф154', 'cat_other_sort:*': 'Ф155', 'cat_other_sort_toggle:*': 'Ф156', 'cat_other_sort_choose:*': 'Ф157', 'cat_other_sort_target:*': 'Ф158', 'cat_pick_today_end:*': 'Ф159', 'exp_send:*:xlsxstat:*': 'Ф160', 'forward_copy_edit_mode_toggle': 'Ф161', 'fwdcopy_edit': 'Ф162', 'fwdcopy_edit_cancel': 'Ф163', 'd:*:usd_tx_toggle': 'Ф164', 'rem:*': 'Ф191', 'mega_tasks_check': 'Ф192', 'mega_tasks_recover': 'Ф193', 'mega_tasks_retry_failed': 'Ф194', 'nav_prev': 'Ф195', 'chat_desc_menu:*': 'Ф196', 'chat_desc_open:*': 'Ф197', 'chat_desc_page:*': 'Ф197', 'expense_shortcut_info': 'Ф198', 'expense_shortcut_pick': 'Ф199', 'expense_shortcut_target:*': 'Ф200', 'expense_shortcut_regenerate': 'Ф201', 'expense_shortcut_test': 'Ф202', 'expense_shortcut_send_url': 'Ф203', 'process_center': 'Ф204', 'problem_tasks': 'Ф205', 'safety_profile_toggle': 'Ф206', 'safety_profile_open': 'Ф206', 'integrity_status': 'Ф207', 'expense_inbox_open': 'Ф208', 'expense_draft_open:*': 'Ф209', 'expense_draft_resolved:*': 'Ф210', 'expense_draft_dismiss:*': 'Ф210', 'expense_evening_toggle': 'Ф211', 'expense_evening_now': 'Ф212', 'expense_evening_done': 'Ф213', 'security_roles:*': 'Ф214', 'security_role_user:*': 'Ф215', 'security_role_set:*': 'Ф216', 'careful_restore_toggle': 'Ф251', 'v215:mode:*': 'Ф256', 'v215:mode:back': 'Ф255', 'info': 'Ф54', 'edit_list': 'Ф49', 'csv_menu': 'Ф74', 'command_window_id': 'Ф40', 'о1': 'О1'}
WINDOW_MARKER_UNKNOWN = {'С': 'С9998', 'Ф': 'Ф9998', 'П': 'П9998'}
_WINDOW_MARKER_UNKNOWN_LOGGED = {}
_WINDOW_MARKER_UNKNOWN_LOG_TTL = 600.0

def has_window_mark(text: str) -> bool:
    try:
        tail = str(text or '')[-160:]
        tail = re.sub('<[^>]+>', '', tail)
        return bool(WINDOW_MARK_RE.search(tail))
    except Exception:
        return False

def strip_window_mark(text: str) -> str:
    try:
        text = str(text or '')
        text = re.sub('\\n\\s*<i>(?:[СФП]\\d{1,6}|[ов]\\d{1,3})(?:-\\(W[A-Z0-9]{6,12}\\))?(?:\\s*[⏳⏰])?</i>\\s*$', '', text, flags=re.IGNORECASE)
        text = re.sub('\\n\\s*(?:[СФП]\\d{1,6}|[ов]\\d{1,3})(?:-\\(W[A-Z0-9]{6,12}\\))?(?:\\s*[⏳⏰])?\\s*$', '', text, flags=re.IGNORECASE)
        return text.rstrip()
    except Exception:
        return str(text or '')

def _window_text_has_active_timer(text: str) -> bool:
    """Показывает ⏳ рядом с маркером только у окон с активным auto-close/cancel/return."""
    body = str(text or '')
    return bool(re.search('(?:⏳|⌛|до закрытия|осталось\\s*[:：]|автоматическ[^\\n]{0,48}(?:закры|отмен|возврат)|через\\s+\\d+\\s*(?:сек|мин)|режим редактирования будет автоматически)', body, flags=re.IGNORECASE))
WINDOW_MARKER_CLOCK_CODES = {'Ф40'}

def window_mark(text: str, code: str, html_mode: bool=False) -> str:
    """Добавляет служебный маркер: ⏳ — auto-close/cancel/return, ⏰ — внутренний refresh."""
    try:
        text = strip_window_mark(str(text or ''))
        code = str(code or '').strip()
        if not code:
            return text
        if _window_text_has_active_timer(text):
            suffix = code + ' ⏳'
        elif code in WINDOW_MARKER_CLOCK_CODES:
            suffix = code + ' ⏰'
        else:
            suffix = code
        return text + '\n\n' + suffix
    except Exception:
        return str(text or '')

def _normalize_window_action(data_str: str) -> str:
    d = str(data_str or '').strip()
    try:
        d = resolve_short_callback(d) or d
    except Exception:
        pass
    if not d:
        return 'finance:unknown'
    d = d.replace(' ', '_')
    parts = d.split(':')
    norm = []
    for idx, part in enumerate(parts):
        low = str(part or '').strip().casefold()
        if idx == 0:
            norm.append(low or 'unknown')
            continue
        if re.fullmatch('[a-zа-яё_][a-zа-яё0-9_\\-]{0,48}', low, flags=re.IGNORECASE):
            norm.append(low)
        else:
            norm.append('*')
    compact = []
    for item in norm:
        if item == '*' and compact and (compact[-1] == '*'):
            continue
        compact.append(item)
    return ':'.join(compact)

def _window_group_for_action(action_key: str) -> str:
    head = str(action_key or '').casefold().split(':', 1)[0]
    if head.startswith('sec') or head.startswith('secret') or head.startswith('total_secret'):
        return 'С'
    if head.startswith('fw') or head.startswith('forward'):
        return 'П'
    return 'Ф'

def _marker_constant_pattern_matches(pattern: str, key: str) -> bool:
    """Сопоставляет статический ключ с константным шаблоном.

    Звёздочка внутри шаблона соответствует одному сегменту callback, а
    последняя звёздочка — всему оставшемуся хвосту. Это не создаёт маркеры
    динамически: номера по-прежнему берутся только из таблицы констант.
    """
    p_parts = str(pattern or '').split(':')
    k_parts = str(key or '').split(':')
    for idx, part in enumerate(p_parts):
        if idx >= len(k_parts):
            return False
        if part == '*':
            if idx == len(p_parts) - 1:
                return True
            continue
        if part != k_parts[idx]:
            return False
    return len(k_parts) == len(p_parts)

def window_marker_is_declared(action_key: str) -> bool:
    """Return True when an action has a statically declared marker.

    Ф9998 is a legitimate marker for Traffic Audit, so checking the numeric
    suffix alone cannot distinguish a valid F9998 window from the unknown
    fallback.  Keep the decision tied to the constants table instead.
    """
    raw_key = str(action_key or '').strip()
    if re.fullmatch('[СФПОВсов]\\d{1,6}', raw_key, flags=re.IGNORECASE):
        return True
    key = _normalize_window_action(raw_key)
    if key.startswith('stored:'):
        parts = key.split(':', 2)
        if len(parts) == 3 and parts[2]:
            key = parts[2]
    if key in WINDOW_MARKER_CONSTANTS:
        return True
    for pattern in WINDOW_MARKER_CONSTANTS:
        if '*' in pattern and _marker_constant_pattern_matches(pattern, key):
            return True
    return False

def _window_marker_code(action_key: str, forced_group: str | None=None) -> str:
    raw_key = str(action_key or '').strip()
    if re.fullmatch('[СФПОВсов]\\d{1,6}', raw_key, flags=re.IGNORECASE):
        return raw_key.upper().replace('В', 'В').replace('О', 'О')
    key = _normalize_window_action(raw_key)
    if key.startswith('stored:'):
        parts = key.split(':', 2)
        if len(parts) == 3 and parts[2]:
            inner_key = parts[2]
            direct = WINDOW_MARKER_CONSTANTS.get(inner_key)
            if direct:
                return direct
            for pattern, marker in sorted(WINDOW_MARKER_CONSTANTS.items(), key=lambda item: (item[0].count('*'), -len(item[0]))):
                if '*' in pattern and _marker_constant_pattern_matches(pattern, inner_key):
                    return marker
    code = WINDOW_MARKER_CONSTANTS.get(key)
    if code:
        return code
    candidates = sorted(WINDOW_MARKER_CONSTANTS.items(), key=lambda item: (item[0].count('*'), -len(item[0])))
    for pattern, marker in candidates:
        if '*' in pattern and _marker_constant_pattern_matches(pattern, key):
            return marker
    group = str(forced_group or _window_group_for_action(key)).upper()
    if group not in _WINDOW_MARK_GROUPS:
        group = 'Ф'
    try:
        now_m = time.monotonic()
        last = float(_WINDOW_MARKER_UNKNOWN_LOGGED.get(key, 0.0) or 0.0)
        if now_m - last >= _WINDOW_MARKER_UNKNOWN_LOG_TTL:
            _WINDOW_MARKER_UNKNOWN_LOGGED[key] = now_m
            log_error(f'WINDOW_MARKER_NOT_DECLARED: raw={raw_key!r} normalized={key!r} group={group}')
    except Exception:
        pass
    return WINDOW_MARKER_UNKNOWN[group]

def window_code_for_callback(data_str: str, owner_chat: bool=False) -> str:
    return _window_marker_code(str(data_str or ''))

def _window_key_from_markup(reply_markup) -> str:
    """Определяет фиксированный маркер окна по его кнопкам.

    Ф93 оставлен только за окном выбора месяцев. Остальные окна получают
    собственный заранее объявленный маркер по первой содержательной кнопке,
    поэтому один и тот же Ф93 больше не повторяется во всех окнах статей.
    """
    try:
        rows = getattr(reply_markup, 'keyboard', None) or []
        values = []
        for row in rows:
            for btn in row:
                cb = getattr(btn, 'callback_data', None)
                if cb:
                    values.append(_normalize_window_action(str(cb)))
        if values:
            if any((v.startswith('cat_m:') for v in values)) and any((v.startswith('cat_months_y:') for v in values)):
                return 'markup:plain'
            for value in values:
                if value == 'none':
                    continue
                if value in WINDOW_MARKER_CONSTANTS:
                    return value
            for value in values:
                if value != 'none':
                    return value
    except Exception:
        pass
    return 'finance:plain_window'

def auto_window_mark(text: str, data_str: str='', owner_chat: bool=False, html_mode: bool=False) -> str:
    return window_mark(text, window_code_for_callback(data_str, owner_chat=owner_chat), html_mode=html_mode)

def wm_common(text: str, n: int, html_mode: bool=False) -> str:
    body = strip_window_mark(str(text or ''))
    return window_mark(body, _window_marker_code(f'legacy_common:{int(n)}', 'Ф'), html_mode=html_mode)

def wm_owner(text: str, n: int, html_mode: bool=False) -> str:
    body = strip_window_mark(str(text or ''))
    return window_mark(body, _window_marker_code(f'legacy_owner:{int(n)}', 'Ф'), html_mode=html_mode)

def audit_window_marker_registry() -> dict:
    """Проверяет статическую таблицу констант на повторы."""
    values = list(WINDOW_MARKER_CONSTANTS.values())
    duplicates = sorted({v for v in values if values.count(v) > 1})
    return {'fixed': 0, 'duplicates': duplicates, 'groups': {g: sum((1 for v in values if v.startswith(g))) for g in _WINDOW_MARK_GROUPS}, 'constant': True}
_v98_auto_close_timers = {}
_v98_auto_close_lock = threading.RLock()

def _v98_scheduler_key(chat_id: int, message_id: int) -> str:
    return f'v98-close:{int(chat_id)}:{int(message_id)}'

def _cancel_v98_auto_close(chat_id: int, message_id: int):
    key = (int(chat_id), int(message_id))
    with _v98_auto_close_lock:
        _v98_auto_close_timers.pop(key, None)
    DELAYED_SCHEDULER.cancel(_v98_scheduler_key(chat_id, message_id))

def _schedule_v98_auto_close(chat_id: int, message_id: int, delay: int | float | None=None):
    """Обычные o98/v98 окна по таймеру возвращаются в основное окно; секретные режимы сюда не входят."""
    chat_id = int(chat_id)
    message_id = int(message_id)
    if delay is None:
        delay = internal_timer_seconds('window_auto_return', 120)
    _cancel_v98_auto_close(chat_id, message_id)

    def _job():
        with _v98_auto_close_lock:
            _v98_auto_close_timers.pop((chat_id, message_id), None)
        try:
            day_key = get_chat_store(chat_id).get('current_view_day') or today_key()
            return_to_main_window_closing_previous(chat_id, day_key, message_id)
        except Exception as e:
            log_error(f'v98 auto return({chat_id},{message_id}): {e}')
    deadline = DELAYED_SCHEDULER.schedule(_v98_scheduler_key(chat_id, message_id), float(delay), _job)
    with _v98_auto_close_lock:
        _v98_auto_close_timers[chat_id, message_id] = deadline

def _touch_v98_auto_close_for_callback(chat_id: int, message_id: int, data_str: str):
    """Любой клик внутри уже открытого авто-окна начинает его таймер заново.

    Раньше неизвестная callback-кнопка отменяла таймер целиком, из-за чего окно могло
    закрыться/вернуться в основное прямо во время работы пользователя.
    """
    try:
        chat_id = int(chat_id)
        message_id = int(message_id)
        raw = str(data_str or '')
        key = (chat_id, message_id)
        closeish = raw == 'aux_close' or raw.endswith(':back_main') or raw in {'close', 'secclose', 'secmclose'}
        if closeish:
            _cancel_v98_auto_close(chat_id, message_id)
            return
        with _v98_auto_close_lock:
            already_active = key in _v98_auto_close_timers
        code = window_code_for_callback(raw, owner_chat=is_owner_chat(chat_id))
        if already_active or code in {'о98', 'в98', 'Ф9998'}:
            _schedule_v98_auto_close(chat_id, message_id, None)
    except Exception:
        pass
DAY_WINDOW_MAX_RECORDS = 35
DAY_WINDOW_MAX_CHARS = 3500
BALANCE_PANEL_REFRESH_DELAY = 5.0
BALANCE_PANEL_COLLAPSE_DELAY = 90.0
COMMAND_DELETE_DELAY = 30
HELPER_DELETE_DELAY = 25
DOZVON_INTERVAL_SECONDS = 0.5
DOZVON_BURST_SECONDS = 10
DOZVON_PAUSE_SECONDS = 5
OWNER_TOTAL_WINDOW_DELETE_DELAY = 60
AUX_WINDOW_DELETE_DELAY = 120
INTERNAL_TIMER_DEFS = {'input_wait': {'label': '✏️ Ожидание ввода / редактирования', 'default': 40, 'min': 5, 'max': 3600}, 'window_auto_return': {'label': '🪟 Автовозврат обычных окон', 'default': 120, 'min': 5, 'max': 7200}, 'command_cleanup': {'label': '🧹 Удаление команд пользователя', 'default': 30, 'min': 1, 'max': 3600}, 'balance_collapse': {'label': '🏦 Сворачивание быстрого остатка', 'default': 90, 'min': 5, 'max': 3600}, 'main_window_refresh': {'label': '⏰ Обновление главного окна Ф40', 'default': 5, 'min': 1, 'max': 300}, 'process_status_refresh': {'label': '⚙️ Обновление статуса процессов', 'default': 10, 'min': 2, 'max': 300}, 'careful_restore_idle': {'label': '⏰ Аккуратное восстановление · авто-ВЫКЛ', 'default': 120, 'min': 30, 'max': 900}}
_timer_input_sessions = {}
_timer_input_lock = threading.RLock()
_careful_restore_sessions = {}
_careful_restore_lock = threading.RLock()

def _careful_restore_scheduler_key(chat_id: int) -> str:
    return f'careful-restore:{int(chat_id)}'

def is_forwarded_telegram_message(msg) -> bool:
    """True only for Telegram-forwarded input; ordinary fresh finance text is untouched."""
    if msg is None:
        return False
    for attr in ('forward_origin', 'forward_date', 'forward_from', 'forward_from_chat', 'forward_sender_name'):
        try:
            if getattr(msg, attr, None):
                return True
        except Exception:
            pass
    return False

def _careful_restore_expire(chat_id: int, expected_deadline: float | None=None) -> bool:
    chat_id = int(chat_id)
    with _careful_restore_lock:
        sess = _careful_restore_sessions.get(chat_id)
        if not isinstance(sess, dict):
            return False
        deadline = float(sess.get('deadline', 0.0) or 0.0)
        if expected_deadline is not None and abs(deadline - float(expected_deadline)) > 0.001:
            return False
        now_m = time.monotonic()
        if deadline > now_m:
            remaining = max(0.05, deadline - now_m)
            DELAYED_SCHEDULER.schedule(_careful_restore_scheduler_key(chat_id), remaining, lambda cid=chat_id, dl=deadline: _careful_restore_expire(cid, dl))
            return False
        _careful_restore_sessions.pop(chat_id, None)
    try:
        bot_journal('careful_restore_auto_off', chat_id, 'idle timeout', 'INFO')
    except Exception:
        pass

    def _notice():
        try:
            fn = globals().get('send_and_auto_delete')
            if callable(fn):
                fn(chat_id, f"⏰ Аккуратное восстановление отключено: {_format_duration_short(internal_timer_seconds('careful_restore_idle', 120))} не было принятых финансовых значений.", 10)
            elif globals().get('bot'):
                bot.send_message(chat_id, '⏰ Аккуратное восстановление отключено по таймеру.')
        except Exception:
            pass
    try:
        pool = globals().get('UI_TASK_POOL') or globals().get('BACKGROUND_TASK_POOL')
        if pool is not None:
            pool.submit(_notice, key=f'careful-restore-notice:{chat_id}')
    except Exception:
        pass
    return True

def careful_restore_status(chat_id: int) -> dict:
    chat_id = int(chat_id)
    with _careful_restore_lock:
        sess = _careful_restore_sessions.get(chat_id)
        if not isinstance(sess, dict):
            return {'active': False, 'remaining': 0, 'day_key': ''}
        deadline = float(sess.get('deadline', 0.0) or 0.0)
    if deadline <= time.monotonic():
        _careful_restore_expire(chat_id, deadline)
        return {'active': False, 'remaining': 0, 'day_key': ''}
    try:
        fn = globals().get('canonical_main_day')
        day_key = str(fn(chat_id) if callable(fn) else get_chat_store(chat_id).get('current_view_day') or finance_today_key(chat_id))[:10]
    except Exception:
        day_key = today_key()
    return {'active': True, 'remaining': max(0, int(round(deadline - time.monotonic()))), 'day_key': day_key, 'enabled_at': sess.get('enabled_at', ''), 'last_value_at': sess.get('last_value_at', ''), 'accepted': int(sess.get('accepted', 0) or 0)}

def careful_restore_active(chat_id: int) -> bool:
    return bool(careful_restore_status(int(chat_id)).get('active'))

def careful_restore_set(chat_id: int, enabled: bool, reason: str='manual') -> bool:
    chat_id = int(chat_id)
    DELAYED_SCHEDULER.cancel(_careful_restore_scheduler_key(chat_id))
    if not enabled:
        with _careful_restore_lock:
            existed = bool(_careful_restore_sessions.pop(chat_id, None))
        try:
            bot_journal('careful_restore_off', chat_id, f'reason={reason}; existed={int(existed)}', 'INFO')
        except Exception:
            pass
        return False
    delay = float(internal_timer_seconds('careful_restore_idle', 120))
    deadline = time.monotonic() + delay
    try:
        fn = globals().get('canonical_main_day')
        day_key = str(fn(chat_id) if callable(fn) else get_chat_store(chat_id).get('current_view_day') or finance_today_key(chat_id))[:10]
    except Exception:
        day_key = today_key()
    with _careful_restore_lock:
        _careful_restore_sessions[chat_id] = {'deadline': deadline, 'enabled_at': now_local().isoformat(timespec='seconds'), 'last_value_at': '', 'accepted': 0}
    DELAYED_SCHEDULER.schedule(_careful_restore_scheduler_key(chat_id), delay, lambda cid=chat_id, dl=deadline: _careful_restore_expire(cid, dl))
    try:
        bot_journal('careful_restore_on', chat_id, f'day={day_key}; idle={delay:.0f}s', 'INFO')
    except Exception:
        pass
    return True

def careful_restore_toggle(chat_id: int) -> bool:
    chat_id = int(chat_id)
    return careful_restore_set(chat_id, not careful_restore_active(chat_id), 'toggle')

def careful_restore_touch_value(chat_id: int, day_key: str, msg=None, record=None) -> bool:
    """Refresh inactivity timer only after a finance value was actually accepted."""
    chat_id = int(chat_id)
    if not careful_restore_active(chat_id):
        return False
    delay = float(internal_timer_seconds('careful_restore_idle', 120))
    deadline = time.monotonic() + delay
    with _careful_restore_lock:
        sess = _careful_restore_sessions.get(chat_id)
        if not isinstance(sess, dict):
            return False
        sess['deadline'] = deadline
        sess['last_value_at'] = now_local().isoformat(timespec='seconds')
        sess['accepted'] = int(sess.get('accepted', 0) or 0) + 1
        accepted = int(sess['accepted'])
    DELAYED_SCHEDULER.cancel(_careful_restore_scheduler_key(chat_id))
    DELAYED_SCHEDULER.schedule(_careful_restore_scheduler_key(chat_id), delay, lambda cid=chat_id, dl=deadline: _careful_restore_expire(cid, dl))
    try:
        mid = int(getattr(msg, 'message_id', 0) or 0) if msg is not None else 0
        rid = int((record or {}).get('id', 0) or 0) if isinstance(record, dict) else 0
        bot_journal('careful_restore_value', chat_id, f'day={day_key}; msg={mid}; record={rid}; accepted={accepted}; idle_reset={delay:.0f}s', 'INFO')
    except Exception:
        pass
    return True

def careful_restore_button_label(chat_id: int) -> str:
    st = careful_restore_status(int(chat_id))
    if not st.get('active'):
        return '🩹 Аккуратное восстановление: ВЫКЛ'
    dk = str(st.get('day_key') or '')
    try:
        dk = datetime.strptime(dk, '%Y-%m-%d').strftime('%d.%m')
    except Exception:
        pass
    return f'🩹 Аккуратное восстановление: ВКЛ · {dk}'

def _format_duration_short(seconds: int | float) -> str:
    seconds = max(0, int(round(float(seconds or 0))))
    minutes, sec = divmod(seconds, 60)
    if minutes and sec:
        return f'{minutes}м {sec}с'
    if minutes:
        return f'{minutes}м'
    return f'{sec}с'

def _v177_legacy_0021_internal_timer_seconds(key: str, fallback=None) -> float:
    cfg = INTERNAL_TIMER_DEFS.get(str(key)) or {}
    default = float(cfg.get('default', fallback if fallback is not None else 30) or 30)
    try:
        gs = data.setdefault('_global_settings', {})
        values = gs.setdefault('internal_timers', {})
        value = float(values.get(str(key), default) or default)
    except Exception:
        value = default
    low = float(cfg.get('min', 1) or 1)
    high = float(cfg.get('max', 86400) or 86400)
    return max(low, min(high, value))
try:
    _v177_legacy_0021_internal_timer_seconds.__name__ = 'internal_timer_seconds'
except Exception:
    pass

def _v177_legacy_0022_set_internal_timer_seconds(key: str, seconds: int | float) -> float:
    key = str(key)
    cfg = INTERNAL_TIMER_DEFS.get(key)
    if not cfg:
        raise KeyError(key)
    value = max(float(cfg.get('min', 1)), min(float(cfg.get('max', 86400)), float(seconds)))
    data.setdefault('_global_settings', {}).setdefault('internal_timers', {})[key] = value
    save_data(data, root_only=True)
    try:
        if OWNER_ID:
            schedule_quick_backup(int(OWNER_ID), 0.5)
        _mark_global_snapshot_pending()
    except Exception as e:
        try:
            log_error(f'set_internal_timer_seconds backup: {e}')
        except Exception:
            pass
    return value
try:
    _v177_legacy_0022_set_internal_timer_seconds.__name__ = 'set_internal_timer_seconds'
except Exception:
    pass

def _v177_legacy_0023_build_internal_timers_text() -> str:
    lines = ['⏱ Внутренние таймеры', '', 'Настройки общие для всех обычных режимов бота.', 'Секретный режим имеет собственные таймеры и здесь не меняется.', 'Ф40 и строка процессов теперь также управляются из этого меню.', '']
    for key, cfg in INTERNAL_TIMER_DEFS.items():
        lines.append(f"{cfg['label']}: {_format_duration_short(internal_timer_seconds(key))}")
    lines.extend(['', 'Выберите таймер для изменения.'])
    return wm_owner('\n'.join(lines), 9)
try:
    _v177_legacy_0023_build_internal_timers_text.__name__ = 'build_internal_timers_text'
except Exception:
    pass

def build_internal_timers_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup(row_width=1)
    for key, cfg in INTERNAL_TIMER_DEFS.items():
        kb.row(IB(f"{cfg['label']} — {_format_duration_short(internal_timer_seconds(key))}", callback_data=f'itmr_pick:{key}'))
    day = get_chat_store(chat_id).get('current_view_day') or today_key()
    kb.row(IB('🔙 Назад в Инфо', callback_data='itmr_back_info'))
    kb.row(IB('⬅️ Назад осн. окно', callback_data=f'd:{day}:back_main'), IB('❌ Закрыть', callback_data='info_close'))
    return kb

def _timer_input_session(chat_id: int):
    with _timer_input_lock:
        return _timer_input_sessions.setdefault(int(chat_id), {'key': None, 'buffer': '', 'minutes': None, 'seconds': None})

def _reset_timer_input_session(chat_id: int, key: str | None=None):
    with _timer_input_lock:
        _timer_input_sessions[int(chat_id)] = {'key': key, 'buffer': '', 'minutes': None, 'seconds': None}
        return _timer_input_sessions[int(chat_id)]

def _timer_input_total_preview(session: dict) -> int:
    minutes = int(session.get('minutes') or 0)
    seconds = int(session.get('seconds') or 0)
    buf = str(session.get('buffer') or '')
    if buf:
        if session.get('minutes') is not None:
            seconds = int(buf)
        elif session.get('seconds') is not None:
            seconds = int(buf)
        else:
            seconds = int(buf)
    return minutes * 60 + seconds

def _v177_legacy_0024_build_internal_timer_input_text(chat_id: int) -> str:
    session = _timer_input_session(chat_id)
    key = session.get('key')
    cfg = INTERNAL_TIMER_DEFS.get(str(key)) or {'label': 'Таймер'}
    buf = str(session.get('buffer') or '') or '—'
    mins = '—' if session.get('minutes') is None else str(session.get('minutes'))
    secs = '—' if session.get('seconds') is None else str(session.get('seconds'))
    preview = _timer_input_total_preview(session)
    return wm_owner(f"⏱ {cfg.get('label')}\n\nСейчас: {_format_duration_short(internal_timer_seconds(str(key)))}\nМинуты: {mins}\nСекунды: {secs}\nНабор: {buf}\nИтого сейчас: {_format_duration_short(preview)}\n\nНаберите число и нажмите «м» или «с». Можно задать, например: 1 → м → 30 → с. Если единицу не нажимать, число считается секундами. Затем нажмите «✅ Выбрать».", 9)
try:
    _v177_legacy_0024_build_internal_timer_input_text.__name__ = 'build_internal_timer_input_text'
except Exception:
    pass

def _canon_build_internal_timer_input_keyboard__001(chat_id: int):
    kb = types.InlineKeyboardMarkup(row_width=3)
    for row in (('1', '2', '3'), ('4', '5', '6'), ('7', '8', '9'), ('м', '0', 'с')):
        buttons = []
        for value in row:
            if value == 'м':
                buttons.append(IB('м', callback_data='itmr_unit:m'))
            elif value == 'с':
                buttons.append(IB('с', callback_data='itmr_unit:s'))
            else:
                buttons.append(IB(value, callback_data=f'itmr_digit:{value}'))
        kb.row(*buttons)
    kb.row(IB('⌫', callback_data='itmr_backspace'), IB('🧹 Очистить', callback_data='itmr_clear'))
    kb.row(IB('✅ Выбрать', callback_data='itmr_apply'))
    day = get_chat_store(chat_id).get('current_view_day') or today_key()
    kb.row(IB('🔙 К списку таймеров', callback_data='internal_timers'))
    kb.row(IB('ℹ️ Инфо', callback_data='itmr_back_info'), IB('⬅️ Осн. окно', callback_data=f'd:{day}:back_main'), IB('❌ Закрыть', callback_data='info_close'))
    return kb
try:
    BACKUP_MIN_DELAY_SECONDS = max(300.0, float(os.getenv('BACKUP_MIN_DELAY_SECONDS', '1800') or '1800'))
except Exception:
    BACKUP_MIN_DELAY_SECONDS = 1800.0
try:
    BACKUP_BUSY_RETRY_SECONDS = max(15.0, float(os.getenv('BACKUP_BUSY_RETRY_SECONDS', '60') or '60'))
except Exception:
    BACKUP_BUSY_RETRY_SECONDS = 60.0
_dozvon_sessions = {}
_dozvon_target_index = defaultdict(set)

def day_key_from_message(msg=None) -> str:
    try:
        if msg and getattr(msg, 'date', None):
            return datetime.fromtimestamp(msg.date, tz=get_tz()).strftime('%Y-%m-%d')
    except Exception:
        pass
    return today_key()

def finance_day_start_5am_enabled(chat_id: int | None=None) -> bool:
    """Режим финансовых суток хранится отдельно в owner scope."""
    return bool(_owner_setting_value('finance_day_start_5am', False, chat_id))

def toggle_finance_day_start_5am(chat_id: int | None=None) -> bool:
    new_value = not finance_day_start_5am_enabled(chat_id)
    _set_owner_setting_value('finance_day_start_5am', new_value, chat_id)
    return new_value

def finance_day_key_from_datetime(dt: datetime, chat_id: int | None=None) -> str:
    try:
        if finance_day_start_5am_enabled(chat_id):
            dt = dt - timedelta(hours=5)
        else:
            try:
                minute = int(_owner_setting_value('finance_day_start_minute', 5, chat_id) or 5)
            except Exception:
                minute = 5
            dt = dt - timedelta(minutes=max(0, min(59, minute)))
        return dt.strftime('%Y-%m-%d')
    except Exception:
        return today_key()

def finance_day_key_from_message(msg=None) -> str:
    try:
        if msg and getattr(msg, 'date', None):
            dt = datetime.fromtimestamp(int(msg.date), tz=get_tz())
        else:
            dt = now_local()
        cid = getattr(getattr(msg, 'chat', None), 'id', None) if msg is not None else current_state_chat_id()
        return finance_day_key_from_datetime(dt, cid)
    except Exception:
        return day_key_from_message(msg)

def finance_today_key(chat_id: int | None=None) -> str:
    return finance_day_key_from_datetime(now_local(), chat_id if chat_id is not None else current_state_chat_id())

def finance_day_start_label(chat_id: int | None=None) -> str:
    if finance_day_start_5am_enabled(chat_id):
        return '05:00'
    try:
        minute = int(_owner_setting_value('finance_day_start_minute', 5, chat_id) or 5)
    except Exception:
        minute = 5
    return f'00:{max(0, min(59, minute)):02d}'
RU_MONTH_NAMES = ('Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь')

def russian_month_name(month: int) -> str:
    try:
        return RU_MONTH_NAMES[int(month) - 1]
    except Exception:
        return str(month)

def calendar_window_text(center_day: datetime, marker: bool=True) -> str:
    text = f'📅 Выберите день:\n{russian_month_name(center_day.month)} {center_day.year}'
    return wm_common(text, 2) if marker else text

def fmt_date_ddmmyy(day_key: str) -> str:
    """YYYY-MM-DD -> DD.MM.YY"""
    try:
        d = datetime.strptime(day_key, '%Y-%m-%d')
        return d.strftime('%d.%m.%y')
    except Exception:
        return str(day_key)

def fmt_date_backup(day_key: str) -> str:
    """Формат даты для backup-файлов: DD:MM:YY. Внутренний day_key YYYY-MM-DD сохраняем отдельно."""
    try:
        d = datetime.strptime(str(day_key)[:10], '%Y-%m-%d')
        return d.strftime('%d:%m:%y')
    except Exception:
        return str(day_key)

def fmt_date_table(day_key: str) -> str:
    """Формат дат в пользовательских CSV/Excel: DD.MM.YY."""
    try:
        d = datetime.strptime(str(day_key)[:10], '%Y-%m-%d')
        return d.strftime('%d.%m.%y')
    except Exception:
        raw = str(day_key or '')
        return raw.replace(':', '.')

def insert_blank_rows_between_days(rows: list[list], header_rows: int=1, date_col: int=0) -> list[list]:
    """Добавляет пустую строку между разными днями в Excel-таблицах."""
    rows = list(rows or [])
    head = rows[:max(0, int(header_rows))]
    body = rows[max(0, int(header_rows)):]
    out = list(head)
    prev_day = None
    for row in body:
        row = list(row or [])
        day = str(row[date_col]).strip() if len(row) > date_col else ''
        if day and prev_day is not None and (day != prev_day):
            out.append([])
        out.append(row)
        if day:
            prev_day = day
    return out

def backup_record_copy(rec: dict) -> dict:
    """Копия записи для JSON-бэкапа: добавляем date в формате DD:MM:YY, не ломая day_key для восстановления."""
    try:
        rr = json.loads(json.dumps(rec or {}, ensure_ascii=False, default=str))
    except Exception:
        rr = dict(rec or {})
    dk = rr.get('day_key') or _record_day_key(rr) if isinstance(rr, dict) else today_key()
    rr['date'] = fmt_date_backup(dk)
    return rr

def backup_records_list(records) -> list:
    return [backup_record_copy(r) for r in records or [] if isinstance(r, dict)]

def backup_daily_records(daily: dict) -> dict:
    """JSON-friendly daily_records с прежними ключами YYYY-MM-DD и дополнительными date в записях."""
    out = {}
    for dk in sorted((daily or {}).keys()):
        out[str(dk)] = backup_records_list((daily or {}).get(dk, []))
    return out

def message_timestamp_iso(source_msg=None) -> str:
    """Для хронологии берём Telegram msg.date, а не время обработки потока."""
    try:
        msg_date = getattr(source_msg, 'date', None)
        if msg_date:
            return datetime.fromtimestamp(int(msg_date), tz=get_tz()).isoformat(timespec='seconds')
    except Exception:
        pass
    return now_local().isoformat(timespec='seconds')

def record_sort_key(rec: dict):
    """Устойчивая сортировка: дата → время Telegram → исходный message_id → внутренний id."""
    try:
        order_msg = int(rec.get('source_order_msg_id') or rec.get('source_msg_id') or rec.get('origin_msg_id') or rec.get('msg_id') or 0)
    except Exception:
        order_msg = 0
    try:
        rid = int(rec.get('id', 0) or 0)
    except Exception:
        rid = 0
    return (str(rec.get('day_key', '')), str(rec.get('timestamp', '')), order_msg, rid)

def compose_edit_input_value(amount, note: str='') -> str:
    """Готовая строка для ручного редактирования записи."""
    try:
        amount = float(amount or 0)
    except Exception:
        amount = 0.0
    note = (note or '').strip()
    if amount > 0:
        base = '+' + fmt_num_compact(amount)
    elif amount < 0:
        base = fmt_num_compact(abs(amount))
    else:
        base = '0'
    return (base + (' ' + note if note else '')).strip()

def fmt_num_compact(v) -> str:
    """
    Число без .0, с минусом при необходимости.
    """
    try:
        v = float(v)
        if v.is_integer():
            return str(int(v))
        s = f'{v:.2f}'.rstrip('0').rstrip('.')
        return s
    except Exception:
        return str(v)

def fmt_csv_amount(v) -> str:
    """CSV-представление суммы без минуса; доход с префиксом «+»."""
    try:
        v = float(v or 0)
    except Exception:
        return str(v)
    body = fmt_num_compact(abs(v))
    if v > 0:
        return f'+ {body}'
    return body

def parse_csv_amount(raw) -> float:
    """Понимает новый CSV-формат и старые +/- значения.

    ВАЖНО: fmt_csv_amount() пишет приход как "+ 123".
    Раньше здесь было s[5:], из-за чего для "+ 123" получалась пустая строка
    и Excel-экспорт по периодам падал с ошибкой: could not convert string to float: ''.
    """
    s = str(raw or '').strip()
    if not s:
        return 0.0
    s = s.replace('➕', '+').replace('➖', '-').strip()
    if s.startswith('+'):
        num = s[1:].strip()
        if not num:
            return 0.0
        return abs(parse_amount('+' + num))
    if s.startswith(('-', '–')):
        return parse_amount(s)
    return -abs(parse_amount(s))

def write_csv_rows_with_day_gaps(writer, rows, width: int | None=None):
    prev_day = None
    for row in rows:
        row = list(row)
        day = str(row[0]) if row else ''
        if prev_day is not None and day != prev_day:
            writer.writerow([''] * (width or len(row)))
        writer.writerow(row)
        prev_day = day

def center_text(text: str, width: int) -> str:
    """
    Центрирование строки в фиксированной ширине.
    Если строка длиннее width — возвращаем как есть.
    """
    text = str(text)
    if len(text) >= width:
        return text
    pad = width - len(text)
    left = pad // 2
    right = pad - left
    return ' ' * left + text + ' ' * right

def report_cell(value, width: int=7) -> str:
    """Числовая ячейка отчёта фиксированной ширины."""
    s = fmt_num_compact(value)
    return s.rjust(width) if len(s) < width else s

def report_header_cell(label: str, width: int=7) -> str:
    """Заголовок ячейки отчёта фиксированной ширины."""
    return center_text(label, width)

def get_chat_display_name(chat_id: int) -> str:
    """Canonical human-visible chat name. Never substitutes owner-role icons for Telegram identity."""
    try:
        store = get_chat_store(int(chat_id))
        info = store.get('info', {}) or {}
        title = str(info.get('title') or '').strip()
        username = str(info.get('username') or '').strip()
        first = str(info.get('first_name') or '').strip()
        last = str(info.get('last_name') or '').strip()
        if title and title != f'Чат {chat_id}':
            return title
        full = (first + ' ' + last).strip()
        if full:
            return full
        if username:
            return f"@{username.lstrip('@')}"
        if title:
            return title
    except Exception:
        pass
    try:
        key = str(int(chat_id))
        for st in (data.get('chats', {}) or {}).values():
            if not isinstance(st, dict):
                continue
            row = (st.get('known_chats') or {}).get(key)
            if not isinstance(row, dict):
                continue
            title = str(row.get('title') or '').strip()
            username = str(row.get('username') or '').strip()
            if title and title != f'Чат {chat_id}':
                return title
            if username:
                return f"@{username.lstrip('@')}"
    except Exception:
        pass
    return f'Чат {chat_id}'

def _chat_title_from_message(msg, previous_title: str='') -> str:
    """Canonical Telegram identity: group/channel title; private chat = real user name/username."""
    try:
        chat_id = int(msg.chat.id)
        chat_title = getattr(msg.chat, 'title', None)
        if chat_title:
            return str(chat_title).strip()
        chat_first = str(getattr(msg.chat, 'first_name', None) or '').strip()
        chat_last = str(getattr(msg.chat, 'last_name', None) or '').strip()
        chat_full = (chat_first + ' ' + chat_last).strip()
        if chat_full:
            return chat_full
        chat_username = str(getattr(msg.chat, 'username', None) or '').strip()
        if chat_username:
            return f"@{chat_username.lstrip('@')}"
        user = getattr(msg, 'from_user', None)
        if user is not None:
            if getattr(user, 'is_bot', False):
                if previous_title and (not str(previous_title).startswith('Чат ')):
                    return str(previous_title)
            else:
                first = str(getattr(user, 'first_name', None) or '').strip()
                last = str(getattr(user, 'last_name', None) or '').strip()
                full = (first + ' ' + last).strip()
                if full:
                    return full
                username = str(getattr(user, 'username', None) or '').strip()
                if username:
                    return f"@{username.lstrip('@')}"
        if previous_title and (not str(previous_title).startswith('Чат ')):
            return str(previous_title)
    except Exception:
        pass
    return f"Чат {getattr(getattr(msg, 'chat', None), 'id', '')}".strip()

def _chat_username_from_message(msg):
    try:
        username = getattr(msg.chat, 'username', None)
        if username:
            return str(username).lstrip('@')
        user = getattr(msg, 'from_user', None)
        if user is not None and (not getattr(user, 'is_bot', False)) and getattr(user, 'username', None):
            return str(user.username).lstrip('@')
    except Exception:
        pass
    return None

def format_finance_mode_label(chat_id: int) -> str:
    return '✅ ВКЛ' if is_finance_mode(chat_id) else '⬜ ВЫКЛ'

def info_finance_toggle_label(chat_id: int) -> str:
    return '✅ Фин режим ВКЛ' if is_finance_mode(chat_id) else '⬜ Фин режим ВЫКЛ'

def is_quick_balance_enabled(chat_id: int) -> bool:
    store = get_chat_store(chat_id)
    settings = store.setdefault('settings', {})
    return bool(settings.get('quick_balance_enabled', False))

def get_quick_balance_behavior(chat_id: int) -> str:
    store = get_chat_store(chat_id)
    settings = store.setdefault('settings', {})
    behavior = (settings.get('quick_balance_behavior') or 'normal').strip().lower()
    if behavior in {'normal', 'mini', 'open', 'first'}:
        return behavior
    return 'normal'

def _infer_legacy_finance_window_mode(chat_id: int) -> str:
    """Migration from v107: hidden-only means no visible auto-window; otherwise preserve old visible mode."""
    try:
        if not is_finance_mode(chat_id):
            return 'off'
        if is_quick_balance_enabled(chat_id):
            behavior = get_quick_balance_behavior(chat_id)
            if behavior in {'open', 'first'}:
                return behavior
        if is_hidden_finance_mode(chat_id):
            return 'off'
        return 'normal'
    except Exception:
        return 'off'

def _finance_window_state(chat_id: int) -> dict:
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    state = store.get('finance_window_state')
    if not isinstance(state, dict):
        try:
            active = dict((data.get('active_messages', {}) or {}).get(str(chat_id), {}) or {})
        except Exception:
            active = {}
        mode = _infer_legacy_finance_window_mode(chat_id)
        state = {'mode': mode, 'main_windows': {str(k): int(v) for k, v in active.items() if v}, 'balance_panel_id': int(store.get('balance_panel_id')) if store.get('balance_panel_id') else None, 'balance_panel_mode': str(store.get('balance_panel_mode') or 'mini'), 'current_view_day': str(store.get('current_view_day') or today_key()), 'auto_reopen_on_boot': bool(mode != 'off' and (active or store.get('balance_panel_id') or (not is_hidden_finance_mode(chat_id)))), 'updated_at': now_local().isoformat(timespec='seconds')}
        store['finance_window_state'] = state
    state.setdefault('mode', _infer_legacy_finance_window_mode(chat_id))
    if state.get('mode') not in {'off', 'normal', 'open', 'first'}:
        state['mode'] = 'off'
    state.setdefault('main_windows', {})
    state.setdefault('balance_panel_id', None)
    state.setdefault('balance_panel_mode', 'mini')
    state.setdefault('current_view_day', str(store.get('current_view_day') or today_key()))
    state.setdefault('auto_reopen_on_boot', bool(state.get('mode') != 'off'))
    state.setdefault('updated_at', now_local().isoformat(timespec='seconds'))
    return state

def finance_window_mode(chat_id: int) -> str:
    if not is_finance_mode(chat_id):
        return 'off'
    try:
        return str(_finance_window_state(chat_id).get('mode') or 'off')
    except Exception:
        return 'off'

def finance_window_mode_enabled(chat_id: int, mode: str | None=None) -> bool:
    current = finance_window_mode(chat_id)
    if mode is None:
        return current in {'normal', 'open', 'first'}
    return current == str(mode)

def _sync_finance_window_state_from_runtime(chat_id: int, *, schedule_delta: bool=False):
    """Compact UI state intentionally survives deploy without putting full open_window_registry into delta."""
    try:
        chat_id = int(chat_id)
        store = get_chat_store(chat_id)
        state = _finance_window_state(chat_id)
        try:
            active = dict((data.get('active_messages', {}) or {}).get(str(chat_id), {}) or {})
        except Exception:
            active = {}
        state['main_windows'] = {str(k): int(v) for k, v in active.items() if v}
        state['balance_panel_id'] = int(store.get('balance_panel_id')) if store.get('balance_panel_id') else None
        state['balance_panel_mode'] = str(store.get('balance_panel_mode') or state.get('balance_panel_mode') or 'mini')
        state['current_view_day'] = str(store.get('current_view_day') or state.get('current_view_day') or today_key())
        state['updated_at'] = now_local().isoformat(timespec='seconds')
        store['finance_window_state'] = state
        save_data(data, chat_ids=[chat_id])
        if schedule_delta and mega_is_configured() and (not RESTORE_GUARD_ACTIVE):
            schedule_quick_backup(chat_id, 0.5)
    except Exception as e:
        log_error(f'_sync_finance_window_state_from_runtime({chat_id}): {e}')

def restore_finance_window_runtime_state():
    """Rehydrate volatile Telegram message ids from compact chat metadata after MEGA/global+delta restore."""
    try:
        for cid_s, store in (data.get('chats', {}) or {}).items():
            try:
                cid = int(cid_s)
            except Exception:
                continue
            state = store.get('finance_window_state')
            if not isinstance(state, dict):
                _finance_window_state(cid)
                state = store.get('finance_window_state') or {}
            mode = str(state.get('mode') or 'off')
            settings = store.setdefault('settings', {})
            if mode == 'normal':
                settings['quick_balance_enabled'] = False
                settings['quick_balance_behavior'] = 'normal'
                settings['quick_balance_user_selected'] = True
            elif mode in {'open', 'first'}:
                settings['quick_balance_enabled'] = True
                settings['quick_balance_behavior'] = mode
                settings['quick_balance_user_selected'] = True
            else:
                settings['quick_balance_enabled'] = False
                settings['quick_balance_behavior'] = 'normal'
                settings['quick_balance_user_selected'] = True
            main_windows = {str(k): int(v) for k, v in (state.get('main_windows') or {}).items() if v}
            selected_day = str(state.get('current_view_day') or store.get('current_view_day') or today_key())[:10]
            selected_mid = int(main_windows.get(selected_day) or 0)
            if not selected_mid and main_windows:
                try:
                    selected_day, selected_mid = list(main_windows.items())[-1]
                    selected_day = str(selected_day)[:10]
                    selected_mid = int(selected_mid)
                except Exception:
                    selected_mid = 0
            data.setdefault('active_messages', {})[str(cid)] = {selected_day: selected_mid} if selected_mid else {}
            store['primary_main_window_id'] = selected_mid or None
            store['primary_main_window_day'] = selected_day
            store['current_view_day'] = selected_day
            state['main_windows'] = {selected_day: selected_mid} if selected_mid else {}
            state['current_view_day'] = selected_day
            store['balance_panel_id'] = int(state.get('balance_panel_id')) if state.get('balance_panel_id') else None
            store['balance_panel_mode'] = str(state.get('balance_panel_mode') or 'mini')
    except Exception as e:
        log_error(f'restore_finance_window_runtime_state: {e}')

def _persist_finance_window_mode_critical(chat_id: int) -> bool:
    """Persist window choice + callback idempotency marker before a critical callback may be acknowledged."""
    try:
        chat_id = int(chat_id)
        _sync_finance_window_state_from_runtime(chat_id, schedule_delta=False)
        if mega_is_configured() and (not RESTORE_GUARD_ACTIVE):
            ctx = _current_telegram_update_context()
            update_id = ctx.get('update_id')
            if update_id is not None and str(ctx.get('update_type') or '') == 'callback_query':
                mark_durable_update_processed(update_id, chat_id, 'callback_query')
            return bool(persist_critical_delta_now(chat_id))
    except Exception as e:
        log_error(f'_persist_finance_window_mode_critical({chat_id}): {e}')
    return False

def set_finance_window_mode(chat_id: int, mode: str, *, persist_now: bool=False):
    chat_id = int(chat_id)
    mode = str(mode or 'off').lower().strip()
    if mode not in {'off', 'normal', 'open', 'first'}:
        mode = 'off'
    store = get_chat_store(chat_id)
    settings = store.setdefault('settings', {})
    state = _finance_window_state(chat_id)
    state['mode'] = mode
    state['auto_reopen_on_boot'] = bool(mode != 'off')
    state['updated_at'] = now_local().isoformat(timespec='seconds')
    if mode == 'normal':
        settings['quick_balance_enabled'] = False
        settings['quick_balance_behavior'] = 'normal'
        settings['quick_balance_user_selected'] = True
    elif mode in {'open', 'first'}:
        settings['quick_balance_enabled'] = True
        settings['quick_balance_behavior'] = mode
        settings['quick_balance_user_selected'] = True
    else:
        settings['quick_balance_enabled'] = False
        settings['quick_balance_behavior'] = 'normal'
        settings['quick_balance_user_selected'] = True
    store['finance_window_state'] = state
    save_data(data, chat_ids=[chat_id])
    if persist_now:
        _persist_finance_window_mode_critical(chat_id)
    else:
        schedule_config_backup_for_chats(chat_id)

def _v177_legacy_0025_delete_auto_finance_windows_for_chat(chat_id: int, *, persist_now: bool=False) -> int:
    """Delete only automatic finance windows controlled by the three F39 modes, not manual reports/F91/category views."""
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    ids = set()
    try:
        ids.update((int(v) for v in (get_or_create_active_windows(chat_id) or {}).values() if v))
    except Exception:
        pass
    try:
        if store.get('balance_panel_id'):
            ids.add(int(store.get('balance_panel_id')))
    except Exception:
        pass
    removed = 0
    for mid in sorted(ids):
        try:
            bot.delete_message(chat_id, mid)
            removed += 1
        except Exception:
            pass
        try:
            unregister_open_window(chat_id, mid)
        except Exception:
            pass
    data.setdefault('active_messages', {})[str(chat_id)] = {}
    store['balance_panel_id'] = None
    store['balance_panel_mode'] = 'mini'
    store['main_window_msg_count'] = 0
    store['balance_panel_msg_count'] = 0
    state = _finance_window_state(chat_id)
    state['main_windows'] = {}
    state['balance_panel_id'] = None
    state['balance_panel_mode'] = 'mini'
    state['auto_reopen_on_boot'] = False if finance_window_mode(chat_id) == 'off' else state.get('auto_reopen_on_boot', True)
    state['updated_at'] = now_local().isoformat(timespec='seconds')
    save_data(data, chat_ids=[chat_id])
    if persist_now:
        _persist_finance_window_mode_critical(chat_id)
    else:
        try:
            schedule_quick_backup(chat_id, 0.5)
        except Exception:
            pass
    return removed
try:
    _v177_legacy_0025_delete_auto_finance_windows_for_chat.__name__ = 'delete_auto_finance_windows_for_chat'
except Exception:
    pass

def set_quick_balance_behavior(chat_id: int, behavior: str):
    store = get_chat_store(chat_id)
    settings = store.setdefault('settings', {})
    behavior = str(behavior or 'normal').strip().lower()
    if behavior not in {'normal', 'mini', 'open', 'first'}:
        behavior = 'normal'
    settings['quick_balance_behavior'] = behavior
    settings['quick_balance_user_selected'] = True
    save_data(data)
    schedule_config_backup_for_chats(chat_id)
    if behavior == 'first':
        schedule_quick_balance_first_recreate(chat_id)

def set_quick_balance_enabled(chat_id: int, enabled: bool):
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    settings = store.setdefault('settings', {})
    enabled = bool(enabled)
    settings['quick_balance_enabled'] = enabled
    if enabled:
        set_finance_mode(chat_id, True)
        if store.get('balance_panel_mode') not in {'mini', 'open'}:
            store['balance_panel_mode'] = 'mini'
        save_data(data)
        schedule_config_backup_for_chats(chat_id)
        schedule_balance_panel_refresh(chat_id, 0.1)
        return
    panel_id = store.get('balance_panel_id')
    if panel_id:
        try:
            bot.delete_message(chat_id, panel_id)
        except Exception:
            pass
    store['balance_panel_id'] = None
    store['balance_panel_mode'] = 'normal'
    settings['quick_balance_behavior'] = 'normal'
    save_data(data)
    schedule_config_backup_for_chats(chat_id)

def is_hidden_finance_mode(chat_id: int) -> bool:
    try:
        store = get_chat_store(chat_id)
        return bool(store.setdefault('settings', {}).get('hidden_finance', False))
    except Exception:
        return False

def is_finance_output_suppressed(chat_id: int) -> bool:
    """Скрытый финрежим: учёт остаётся, но в самом чате ничего финансового не выводим."""
    try:
        return bool(is_hidden_finance_mode(chat_id) and (not is_owner_chat(chat_id)))
    except Exception:
        return False

def mega_backup_priority_enabled(chat_id: int | None=None) -> bool:
    """Приоритет MEGA — настройка owner scope; без контекста сохраняется legacy fallback."""
    return bool(_owner_setting_value('mega_backup_priority', False, chat_id))

def set_mega_backup_priority_enabled(enabled: bool, chat_id: int | None=None):
    _set_owner_setting_value('mega_backup_priority', bool(enabled), chat_id)
    if mega_is_configured():
        _schedule_global_mega_snapshot(1.0)

def toggle_mega_backup_priority(chat_id: int | None=None) -> bool:
    new_value = not mega_backup_priority_enabled(chat_id)
    set_mega_backup_priority_enabled(new_value, chat_id)
    return new_value

def mega_backup_priority_label(chat_id: int | None=None) -> str:
    return '☁️ Сразу в MEGA' if mega_backup_priority_enabled(chat_id) else '🕓 MEGA как обычно'

def _v177_legacy_0026_backup_excel_all_enabled() -> bool:
    try:
        return bool((data or {}).setdefault('_global_settings', {}).get('backup_excel_all_enabled', True))
    except Exception:
        return True
try:
    _v177_legacy_0026_backup_excel_all_enabled.__name__ = 'backup_excel_all_enabled'
except Exception:
    pass

def _v177_legacy_0027_set_backup_excel_all_enabled(enabled: bool):
    data.setdefault('_global_settings', {})['backup_excel_all_enabled'] = bool(enabled)
    save_data(data, full=True)
try:
    _v177_legacy_0027_set_backup_excel_all_enabled.__name__ = 'set_backup_excel_all_enabled'
except Exception:
    pass

def toggle_backup_excel_all_enabled() -> bool:
    new_value = not backup_excel_all_enabled()
    set_backup_excel_all_enabled(new_value)
    return new_value

def backup_excel_all_label() -> str:
    return '✅ ВКЛ' if backup_excel_all_enabled() else '⬜ ВЫКЛ'

def _normalize_excel_table_style(value) -> str:
    raw = str(value or '').strip().lower()
    aliases = {'new': 'new_notes', 'notes': 'new_notes', 'note': 'new_notes', 'comments': 'new_comments', 'comment': 'new_comments', 'plain': 'new_plain', 'new_plain': 'new_plain', 'google': 'google_notes', 'sheets': 'google_notes', 'google_sheets': 'google_notes', 'google_notes': 'google_notes'}
    mode = aliases.get(raw, raw)
    return mode if mode in {'old', 'new_plain', 'new_comments', 'new_notes', 'google_notes'} else ''

def _v177_legacy_0028_excel_interface_mode(chat_id: int | None=None) -> str:
    """INFO switch: old interface (v136 chooser) or new checkbox recipe."""
    gs = data.setdefault('_global_settings', {})
    mode = str(gs.get('excel_interface_mode') or 'old').strip().lower()
    if mode not in {'old', 'new'}:
        mode = 'old'
        gs['excel_interface_mode'] = mode
    return mode
try:
    _v177_legacy_0028_excel_interface_mode.__name__ = 'excel_interface_mode'
except Exception:
    pass

def _v177_legacy_0029_set_excel_interface_mode(mode: str) -> str:
    mode = 'new' if str(mode or '').strip().lower() == 'new' else 'old'
    data.setdefault('_global_settings', {})['excel_interface_mode'] = mode
    save_data(data, root_only=True)
    try:
        if OWNER_ID:
            schedule_config_backup_for_chats(int(OWNER_ID), delay=1.0)
    except Exception:
        pass
    return mode
try:
    _v177_legacy_0029_set_excel_interface_mode.__name__ = 'set_excel_interface_mode'
except Exception:
    pass

def toggle_excel_interface_mode(chat_id: int | None=None) -> str:
    return set_excel_interface_mode('new' if excel_interface_mode(chat_id) == 'old' else 'old')

def _v177_legacy_0030_excel_new_export_options() -> dict:
    gs = data.setdefault('_global_settings', {})
    raw = gs.get('excel_new_export_options')
    if not isinstance(raw, dict):
        raw = {}
    options = {'old_table': bool(raw.get('old_table', False)), 'comments': bool(raw.get('comments', False)), 'notes': bool(raw.get('notes', True)), 'description_column': bool(raw.get('description_column', False))}
    if options['old_table']:
        options.update({'comments': False, 'notes': False, 'description_column': True})
    elif options['comments'] and options['notes']:
        options['comments'] = False
    gs['excel_new_export_options'] = dict(options)
    return options
try:
    _v177_legacy_0030_excel_new_export_options.__name__ = 'excel_new_export_options'
except Exception:
    pass

def _v177_legacy_0031_toggle_excel_new_export_option(option: str) -> dict:
    option = str(option or '').strip().lower()
    options = excel_new_export_options()
    if option == 'old_table':
        enabled = not options['old_table']
        options['old_table'] = enabled
        if enabled:
            options.update({'comments': False, 'notes': False, 'description_column': True})
    elif option == 'comments':
        enabled = not options['comments']
        options.update({'old_table': False, 'comments': enabled})
        if enabled:
            options['notes'] = False
    elif option == 'notes':
        enabled = not options['notes']
        options.update({'old_table': False, 'notes': enabled})
        if enabled:
            options['comments'] = False
    elif option == 'description_column':
        enabled = not options['description_column']
        options['description_column'] = enabled
        if not enabled:
            options['old_table'] = False
    data.setdefault('_global_settings', {})['excel_new_export_options'] = dict(options)
    save_data(data, root_only=True)
    try:
        if OWNER_ID:
            schedule_config_backup_for_chats(int(OWNER_ID), delay=1.0)
    except Exception:
        pass
    return dict(options)
try:
    _v177_legacy_0031_toggle_excel_new_export_option.__name__ = 'toggle_excel_new_export_option'
except Exception:
    pass

def normalize_excel_export_options(value: dict | None=None) -> dict:
    src = dict(value or excel_new_export_options())
    out = {'old_table': bool(src.get('old_table', False)), 'comments': bool(src.get('comments', False)), 'notes': bool(src.get('notes', False)), 'description_column': bool(src.get('description_column', False))}
    if out['old_table']:
        out.update({'comments': False, 'notes': False, 'description_column': True})
    elif out['comments'] and out['notes']:
        out['comments'] = False
    return out

def excel_export_options_style(options: dict | None=None) -> str:
    opts = normalize_excel_export_options(options)
    if opts['old_table']:
        return 'old'
    if opts['comments']:
        return 'new_comments'
    if opts['notes']:
        return 'new_notes'
    return 'new_plain'

def _v177_legacy_0032_excel_table_style(chat_id: int) -> str:
    gs = data.setdefault('_global_settings', {})
    mode = _normalize_excel_table_style(gs.get('excel_table_style_global'))
    if not mode:
        candidates = [gs.get('excel_table_style')]
        try:
            if OWNER_ID:
                candidates.append(get_chat_store(int(OWNER_ID)).setdefault('settings', {}).get('excel_table_style'))
        except Exception:
            pass
        try:
            candidates.append(get_chat_store(int(chat_id)).setdefault('settings', {}).get('excel_table_style'))
        except Exception:
            pass
        mode = next((_normalize_excel_table_style(v) for v in candidates if _normalize_excel_table_style(v)), 'new_notes')
        gs['excel_table_style_global'] = mode
        gs['excel_table_style'] = mode
    return mode
try:
    _v177_legacy_0032_excel_table_style.__name__ = 'excel_table_style'
except Exception:
    pass

def _v177_legacy_0033_set_excel_table_style(chat_id: int, mode: str) -> str:
    chat_id = int(chat_id)
    mode = _normalize_excel_table_style(mode) or 'new_notes'
    gs = data.setdefault('_global_settings', {})
    gs['excel_table_style_global'] = mode
    gs['excel_table_style'] = mode
    touched = []
    for cid in (chat_id, int(OWNER_ID or 0)):
        if not cid or cid in touched:
            continue
        try:
            get_chat_store(cid).setdefault('settings', {})['excel_table_style'] = mode
            touched.append(cid)
        except Exception:
            pass
    save_data(data, chat_ids=touched or None, root_only=not bool(touched))
    try:
        schedule_config_backup_for_chats(*(touched or [chat_id]), delay=1.0)
    except Exception:
        pass
    return mode
try:
    _v177_legacy_0033_set_excel_table_style.__name__ = 'set_excel_table_style'
except Exception:
    pass

def toggle_excel_table_style(chat_id: int) -> str:
    order = ['old', 'new_comments', 'new_notes', 'google_notes']
    current = excel_table_style(chat_id)
    try:
        next_mode = order[(order.index(current) + 1) % len(order)]
    except Exception:
        next_mode = 'new_notes'
    return set_excel_table_style(chat_id, next_mode)

def excel_table_style_caption(chat_id: int) -> str:
    if excel_interface_mode(chat_id) == 'new':
        return 'ПО-НОВОМУ'
    return 'ПО-СТАРОМУ'

def excel_annotation_mode(chat_id: int) -> str | None:
    mode = excel_table_style(chat_id)
    if mode == 'new_comments':
        return 'comments'
    if mode in {'new_notes', 'google_notes'}:
        return 'notes'
    return None

def excel_table_style_label(chat_id: int) -> str:
    return '📊 Excel: по новому' if excel_interface_mode(chat_id) == 'new' else '📊 Excel: по старому'

def build_excel_style_text(chat_id: int) -> str:
    return wm_owner(f'📊 Excel\\n\\nКнопка в INFO теперь только переключает интерфейс экспорта.\\n• По старому — меню выбора формата v136.\\n• По новому — настройки с галочками в Ф179/Ф181.\\n\\nСейчас: {excel_table_style_caption(chat_id)}', 9)

def build_excel_style_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.row(IB(excel_table_style_label(chat_id), callback_data='excel_style_toggle'))
    kb.row(IB('🔙 Назад в Инфо', callback_data='journal_back'))
    return kb

def _backup_target_all_state(target: str) -> tuple[int, int]:
    ids = [int(cid) for cid, _ in _collect_backup_menu_items()]
    if target == 'chat':
        ids = [cid for cid in ids if is_owner_chat(cid)]
    enabled = sum((1 for cid in ids if is_backup_target_enabled(cid, target)))
    return (enabled, len(ids))

def set_backup_target_for_all(target: str, enabled: bool) -> int:
    count = 0
    for cid, _title in _collect_backup_menu_items():
        cid = int(cid)
        if target == 'chat' and (not is_owner_chat(cid)):
            continue
        settings = _ensure_backup_settings(cid)
        settings[_backup_target_setting_key(target)] = bool(enabled)
        settings['auto_backup_enabled'] = any((bool(settings.get('auto_backup_to_chat_enabled', True)), bool(settings.get('auto_backup_to_channel_enabled', True)), bool(settings.get('auto_backup_to_mega_enabled', True))))
        count += 1
    save_data(data, full=True)
    for cid, _title in _collect_backup_menu_items():
        schedule_backup_flush(int(cid), BACKUP_MIN_DELAY_SECONDS)
    return count

def _backup_target_setting_key(target: str) -> str:
    target = str(target or '').strip().lower()
    if target in {'chat', 'owner', 'self'}:
        return 'auto_backup_to_chat_enabled'
    if target in {'channel', 'backup_channel'}:
        return 'auto_backup_to_channel_enabled'
    if target in {'mega', 'cloud'}:
        return 'auto_backup_to_mega_enabled'
    return 'auto_backup_enabled'

def _ensure_backup_settings(chat_id: int) -> dict:
    store = get_chat_store(chat_id)
    settings = store.setdefault('settings', {})
    legacy = bool(settings.get('auto_backup_enabled', True))
    settings.setdefault('auto_backup_enabled', legacy)
    settings.setdefault('auto_backup_to_chat_enabled', legacy)
    settings.setdefault('auto_backup_to_channel_enabled', legacy)
    settings.setdefault('auto_backup_to_mega_enabled', legacy)
    return settings

def is_backup_target_enabled(chat_id: int, target: str) -> bool:
    try:
        settings = _ensure_backup_settings(chat_id)
        return bool(settings.get(_backup_target_setting_key(target), True))
    except Exception:
        return True

def is_backup_to_chat_enabled(chat_id: int) -> bool:
    return is_backup_target_enabled(chat_id, 'chat')

def is_backup_to_channel_enabled(chat_id: int) -> bool:
    return is_backup_target_enabled(chat_id, 'channel')

def is_backup_to_mega_enabled(chat_id: int) -> bool:
    return is_backup_target_enabled(chat_id, 'mega')

def is_auto_backup_enabled(chat_id: int) -> bool:
    """Legacy master: True если включён хотя бы один тип авто-бэкапа."""
    try:
        return any((is_backup_to_chat_enabled(chat_id), is_backup_to_channel_enabled(chat_id), is_backup_to_mega_enabled(chat_id)))
    except Exception:
        return True

def set_backup_target_enabled(chat_id: int, target: str, enabled: bool):
    settings = _ensure_backup_settings(chat_id)
    settings[_backup_target_setting_key(target)] = bool(enabled)
    settings['auto_backup_enabled'] = any((bool(settings.get('auto_backup_to_chat_enabled', True)), bool(settings.get('auto_backup_to_channel_enabled', True)), bool(settings.get('auto_backup_to_mega_enabled', True))))
    save_data(data)
    schedule_config_backup_for_chats(chat_id)

def set_auto_backup_enabled(chat_id: int, enabled: bool):
    """Совместимость: старое включение/выключение теперь меняет все три бэкапа сразу."""
    settings = _ensure_backup_settings(chat_id)
    enabled = bool(enabled)
    settings['auto_backup_enabled'] = enabled
    settings['auto_backup_to_chat_enabled'] = enabled
    settings['auto_backup_to_channel_enabled'] = enabled
    settings['auto_backup_to_mega_enabled'] = enabled
    save_data(data)
    schedule_config_backup_for_chats(chat_id)

def _v177_legacy_0034_is_bot_removed_error(err) -> bool:
    text = str(err or '').lower()
    needles = ('bot was kicked', 'bot was blocked', 'user is deactivated', 'chat not found', 'forbidden', 'not enough rights', 'have no rights')
    return any((n in text for n in needles))
try:
    _v177_legacy_0034_is_bot_removed_error.__name__ = '_is_bot_removed_error'
except Exception:
    pass

def _v177_legacy_0035_set_chat_bot_removed(chat_id: int, removed: bool=True, reason: str='', *, persist: bool=True, schedule_backup: bool=True):
    try:
        chat_id = int(chat_id)
        store = get_chat_store(chat_id)
        settings = store.setdefault('settings', {})
        current = bool(settings.get('bot_removed', False))
        new_reason = str(reason or 'bot removed')[:300]
        if current == bool(removed):
            if not removed:
                return False
            if str(settings.get('bot_removed_reason') or '') == new_reason:
                return False
        settings['bot_removed'] = bool(removed)
        if removed:
            settings['bot_removed_reason'] = new_reason
            settings['bot_removed_at'] = now_local().isoformat(timespec='seconds')
        else:
            settings.pop('bot_removed_reason', None)
            settings.pop('bot_removed_at', None)
        if persist:
            save_data(data)
        if schedule_backup:
            try:
                ids_for_backup = [chat_id]
                if OWNER_ID and str(chat_id) != str(OWNER_ID):
                    ids_for_backup.append(int(OWNER_ID))
                schedule_config_backup_for_chats(*ids_for_backup, delay=1.0)
            except Exception:
                pass
        try:
            bot_journal('bot_removed_state', chat_id, f'removed={removed} {reason}')
        except Exception:
            pass
        return True
    except Exception as e:
        log_error(f'set_chat_bot_removed({chat_id}): {e}')
        return False
try:
    _v177_legacy_0035_set_chat_bot_removed.__name__ = 'set_chat_bot_removed'
except Exception:
    pass

def _v177_legacy_0036_is_chat_bot_removed(chat_id: int) -> bool:
    try:
        store = get_chat_store(int(chat_id))
        return bool(store.setdefault('settings', {}).get('bot_removed', False))
    except Exception:
        return False
try:
    _v177_legacy_0036_is_chat_bot_removed.__name__ = 'is_chat_bot_removed'
except Exception:
    pass

def _v177_legacy_0037_chat_button_title(chat_id: int, title: str | None=None) -> str:
    title = title or get_chat_display_name(chat_id)
    return ('➖ ' if is_chat_bot_removed(chat_id) else '') + str(title)
try:
    _v177_legacy_0037_chat_button_title.__name__ = 'chat_button_title'
except Exception:
    pass

def answer_removed_chat(call, target_chat_id: int) -> bool:
    if not is_chat_bot_removed(target_chat_id):
        return False
    txt = f'➖ Бот удалён из чата: {get_chat_display_name(target_chat_id)}'
    try:
        bot.answer_callback_query(call.id, txt, show_alert=True)
    except Exception:
        pass
    try:
        send_and_auto_delete(call.message.chat.id, txt, 12)
    except Exception:
        pass
    return True

def _v177_legacy_0038_collect_all_known_chat_ids(include_owner: bool=True) -> list[int]:
    """Все известные чаты из памяти/пересылок/финрежима для проверки наличия бота."""
    ids = set()
    try:
        for cid in (data.get('chats', {}) or {}).keys():
            ids.add(int(cid))
    except Exception:
        pass
    try:
        for cid in (collect_forward_menu_chats() or {}).keys():
            ids.add(int(cid))
    except Exception:
        pass
    try:
        fr = data.get('forward_rules', {}) or {}
        for src, dsts in fr.items():
            ids.add(int(src))
            for dst in (dsts or {}).keys():
                ids.add(int(dst))
    except Exception:
        pass
    if OWNER_ID and include_owner:
        try:
            ids.add(int(OWNER_ID))
        except Exception:
            pass
    return sorted(ids, key=lambda cid: get_chat_display_name(cid).lower())
try:
    _v177_legacy_0038_collect_all_known_chat_ids.__name__ = 'collect_all_known_chat_ids'
except Exception:
    pass

def _v197_json_safe(value, depth: int=0):
    if depth > 6:
        return str(value)[:500]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _v197_json_safe(v, depth + 1) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_v197_json_safe(v, depth + 1) for v in value]
    try:
        fn = getattr(value, 'to_dict', None)
        if callable(fn):
            return _v197_json_safe(fn(), depth + 1)
    except Exception:
        pass
    return str(value)[:1000]

def _v197_chat_object_snapshot(chat_obj) -> dict:
    """Whitelisted Telegram metadata visible to the bot; safe to persist and diff on explicit chat probe."""
    fields = ('type', 'username', 'first_name', 'last_name', 'description', 'bio', 'invite_link', 'linked_chat_id', 'message_auto_delete_time', 'slow_mode_delay', 'is_forum', 'has_protected_content', 'join_to_send_messages', 'join_by_request', 'has_visible_history', 'accent_color_id', 'background_custom_emoji_id', 'profile_accent_color_id', 'profile_background_custom_emoji_id', 'emoji_status_custom_emoji_id', 'emoji_status_expiration_date')
    out = {}
    for name in fields:
        try:
            value = getattr(chat_obj, name, None)
        except Exception:
            value = None
        if value is not None:
            if name == 'username':
                value = str(value).strip().lstrip('@') or None
            out[name] = value
    try:
        title = str(getattr(chat_obj, 'title', None) or '').strip()
    except Exception:
        title = ''
    if not title:
        first = str(out.get('first_name') or '').strip()
        last = str(out.get('last_name') or '').strip()
        title = (first + ' ' + last).strip()
        if not title and out.get('username'):
            title = f"@{out['username']}"
    if title:
        out['title'] = title
    try:
        raw_fn = getattr(chat_obj, 'to_dict', None)
        raw = raw_fn() if callable(raw_fn) else {}
        if isinstance(raw, dict):
            raw = dict(raw)
            raw.pop('pinned_message', None)
            out['telegram_profile_v197'] = _v197_json_safe(raw)
    except Exception:
        pass
    return out

def _v177_legacy_0039_update_chat_info_from_chat_object(chat_obj, *, persist: bool=True, schedule_backup: bool=True) -> bool:
    """Refresh canonical chat identity and Telegram-visible metadata from getChat()."""
    try:
        chat_id = int(getattr(chat_obj, 'id'))
    except Exception:
        return False
    store = get_chat_store(chat_id)
    info = store.setdefault('info', {})
    snapshot = _v197_chat_object_snapshot(chat_obj)
    prev_title = str(info.get('title') or '')
    if not snapshot.get('title'):
        snapshot['title'] = prev_title or f'Чат {chat_id}'
    changed = False
    for key, value in snapshot.items():
        if info.get(key) != value:
            info[key] = value
            changed = True
    info['last_probe_at'] = now_local().isoformat(timespec='seconds')
    info['last_probe_ok'] = True
    if OWNER_ID and str(chat_id) != str(OWNER_ID):
        owner_store = get_chat_store(int(OWNER_ID))
        kc = owner_store.setdefault('known_chats', {})
        new_known = {'title': info.get('title') or f'Чат {chat_id}', 'username': info.get('username'), 'type': info.get('type')}
        new_identity = _chat_identity_key(chat_id, new_known)
        for old_cid, old_info in list(kc.items()):
            try:
                old_id_int = int(old_cid)
            except Exception:
                kc.pop(old_cid, None)
                changed = True
                continue
            if str(old_cid) != str(chat_id) and _chat_identity_key(old_id_int, old_info if isinstance(old_info, dict) else {}) == new_identity:
                kc.pop(old_cid, None)
                changed = True
        if kc.get(str(chat_id)) != new_known:
            kc[str(chat_id)] = new_known
            changed = True
    if changed and persist:
        save_data(data)
    if changed and schedule_backup:
        try:
            ids_for_backup = [chat_id]
            if OWNER_ID and str(chat_id) != str(OWNER_ID):
                ids_for_backup.append(int(OWNER_ID))
            schedule_config_backup_for_chats(*ids_for_backup, delay=2.0)
        except Exception as e:
            log_error(f'update_chat_info_from_chat_object backup {chat_id}: {e}')
    return changed
try:
    _v177_legacy_0039_update_chat_info_from_chat_object.__name__ = 'update_chat_info_from_chat_object'
except Exception:
    pass
_V197_BOT_USER_ID_CACHE = None

def _v197_bot_user_id() -> int:
    global _V197_BOT_USER_ID_CACHE
    if _V197_BOT_USER_ID_CACHE:
        return int(_V197_BOT_USER_ID_CACHE)
    try:
        me = _tg_call_retry(bot.get_me, attempts=2, purpose='probe_get_me')
        _V197_BOT_USER_ID_CACHE = int(getattr(me, 'id', 0) or 0)
    except Exception:
        _V197_BOT_USER_ID_CACHE = 0
    return int(_V197_BOT_USER_ID_CACHE or 0)

def _v199_extract_migration_target(err) -> int | None:
    try:
        fn = globals().get('_telegram_migrate_to_chat_id')
        if callable(fn):
            value = fn(err)
            if value is not None:
                return int(value)
    except Exception:
        pass
    try:
        result_json = getattr(err, 'result_json', None) or {}
        value = (result_json.get('parameters') or {}).get('migrate_to_chat_id')
        if value is not None:
            return int(value)
    except Exception:
        pass
    return None

def _v199_normalized_chat_title(value: str) -> str:
    return re.sub('\\\\s+', ' ', str(value or '').strip()).casefold()

def collect_probe_chat_ids_v200(include_owner: bool=True) -> list[int]:
    """Global probe inventory, deliberately independent from thread-local tenant scope.

    The owner action «Проверить чаты» must inspect every chat the bot knows, even
    when it runs in a background worker where tenant_current_id() has no UI actor.
    """
    ids = set()
    try:
        for raw in (data.get('chats', {}) or {}).keys():
            try:
                ids.add(int(raw))
            except Exception:
                pass
    except Exception:
        pass
    for root_key in ('forward_rules', 'forward_finance'):
        try:
            root = data.get(root_key, {}) or {}
            for src, dsts in root.items():
                try:
                    ids.add(int(src))
                except Exception:
                    pass
                if isinstance(dsts, dict):
                    for dst in dsts.keys():
                        try:
                            ids.add(int(dst))
                        except Exception:
                            pass
        except Exception:
            pass
    try:
        gs = data.get('_global_settings', {}) or {}
        tenants_root = gs.get('tenants_v148') or {}
        mapping = tenants_root.get('chat_to_tenant') or {}
        for raw in mapping.keys():
            try:
                ids.add(int(raw))
            except Exception:
                pass
        for row in (tenants_root.get('tenants') or {}).values():
            if not isinstance(row, dict):
                continue
            for raw in row.get('chat_ids') or []:
                try:
                    ids.add(int(raw))
                except Exception:
                    pass
            try:
                if row.get('root_chat_id') is not None:
                    ids.add(int(row.get('root_chat_id')))
            except Exception:
                pass
        reminders = (gs.get('reminders_v2') or {}).get('items') or {}
        for cfg in reminders.values():
            if not isinstance(cfg, dict):
                continue
            for raw in cfg.get('chat_ids') or []:
                try:
                    ids.add(int(raw))
                except Exception:
                    pass
    except Exception:
        pass
    try:
        for raw in (data.get('_task_settings_v172') or {}).keys():
            try:
                ids.add(int(raw))
            except Exception:
                pass
        for row in (data.get('_tasks_v172') or {}).values():
            if not isinstance(row, dict):
                continue
            for key in ('chat_id', 'source_chat_id', 'target_chat_id'):
                try:
                    if row.get(key) is not None:
                        ids.add(int(row.get(key)))
                except Exception:
                    pass
    except Exception:
        pass
    if include_owner and OWNER_ID:
        try:
            ids.add(int(OWNER_ID))
        except Exception:
            pass
    resolver = globals().get('resolve_canonical_chat_id_v199')
    out = []
    for raw in ids:
        try:
            cid = int(resolver(int(raw))) if callable(resolver) else int(raw)
        except Exception:
            continue
        if not include_owner and OWNER_ID and (cid == int(OWNER_ID)):
            continue
        if cid not in out:
            out.append(cid)
    return sorted(out, key=lambda cid: (str(get_chat_display_name(cid) or '').casefold(), int(cid)))

def _v199_unique_local_supergroup_successor(old_chat_id: int, chat_obj=None) -> int | None:
    """Fallback only when Telegram says 'upgraded' but the client library hides ResponseParameters.
    Require one exact-title supergroup candidate in the same tenant; never guess among multiples.
    """
    old_chat_id = int(old_chat_id)
    title = ''
    try:
        title = str(getattr(chat_obj, 'title', None) or '')
    except Exception:
        pass
    if not title:
        try:
            title = str((get_chat_store(old_chat_id).get('info') or {}).get('title') or '')
        except Exception:
            pass
    norm = _v199_normalized_chat_title(title)
    if not norm:
        return None
    try:
        tenant_fn = globals().get('tenant_id_for_chat')
        old_tid = str(tenant_fn(old_chat_id, create=False)) if callable(tenant_fn) else ''
    except Exception:
        old_tid = ''
    candidates = []
    same_tenant = []
    try:
        collector = globals().get('collect_probe_chat_ids_v200') or globals().get('collect_all_known_chat_ids')
        ids = list(collector(include_owner=True)) if callable(collector) else []
    except Exception:
        ids = []
    for cid in ids:
        try:
            cid = int(cid)
            if cid == old_chat_id:
                continue
            info = get_chat_store(cid).get('info') or {}
            if str(info.get('type') or '') != 'supergroup':
                continue
            if _v199_normalized_chat_title(info.get('title') or get_chat_display_name(cid)) != norm:
                continue
            candidates.append(cid)
            if old_tid and callable(globals().get('tenant_id_for_chat')):
                try:
                    if str(globals()['tenant_id_for_chat'](cid, create=False)) == old_tid:
                        same_tenant.append(cid)
                except Exception:
                    pass
        except Exception:
            continue
    candidates = sorted(set(candidates))
    same_tenant = sorted(set(same_tenant))
    if len(same_tenant) == 1:
        return same_tenant[0]
    if len(candidates) == 1:
        return candidates[0]
    return None

def _v199_migration_target_from_probe_error(old_chat_id: int, err, chat_obj=None) -> int | None:
    target = _v199_extract_migration_target(err)
    if target is not None:
        return int(target)
    if 'group chat was upgraded to a supergroup chat' not in str(err or '').casefold():
        return None
    return _v199_unique_local_supergroup_successor(int(old_chat_id), chat_obj)

def _v197_refresh_chat_probe_facts(chat_id: int, chat_obj=None) -> bool:
    """Refresh facts that can change without a message: member count, bot role/rights and administrator fingerprint."""
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    info = store.setdefault('info', {})
    changed = False
    warnings = []
    fn = getattr(bot, 'get_chat_member_count', None) or getattr(bot, 'get_chat_members_count', None)
    if callable(fn):
        try:
            count = int(_tg_call_retry(fn, chat_id, attempts=2, purpose='probe_member_count'))
            if info.get('member_count') != count:
                info['member_count'] = count
                changed = True
        except Exception as exc:
            migrated = _v199_migration_target_from_probe_error(chat_id, exc, chat_obj)
            if migrated is not None:
                info['_probe_migrate_to_v199'] = int(migrated)
                changed = True
                return changed
            warnings.append('member_count:' + str(exc)[:120])
    bot_uid = _v197_bot_user_id()
    if bot_uid and hasattr(bot, 'get_chat_member'):
        try:
            member = _tg_call_retry(bot.get_chat_member, chat_id, bot_uid, attempts=2, purpose='probe_bot_member')
            row = {'status': str(getattr(member, 'status', '') or '')}
            for key in ('can_manage_chat', 'can_delete_messages', 'can_manage_video_chats', 'can_restrict_members', 'can_promote_members', 'can_change_info', 'can_invite_users', 'can_post_messages', 'can_edit_messages', 'can_pin_messages', 'can_manage_topics'):
                value = getattr(member, key, None)
                if value is not None:
                    row[key] = bool(value)
            if info.get('bot_membership') != row:
                info['bot_membership'] = row
                changed = True
        except Exception as exc:
            migrated = _v199_migration_target_from_probe_error(chat_id, exc, chat_obj)
            if migrated is not None:
                info['_probe_migrate_to_v199'] = int(migrated)
                changed = True
                return changed
            warnings.append('bot_member:' + str(exc)[:120])
    chat_type = str(getattr(chat_obj, 'type', None) or info.get('type') or '')
    if chat_type in {'group', 'supergroup', 'channel'} and hasattr(bot, 'get_chat_administrators'):
        try:
            admins = list(_tg_call_retry(bot.get_chat_administrators, chat_id, attempts=2, purpose='probe_administrators') or [])
            rows = []
            for member in admins:
                user = getattr(member, 'user', None)
                rows.append({'id': int(getattr(user, 'id', 0) or 0), 'name': (str(getattr(user, 'first_name', '') or '') + ' ' + str(getattr(user, 'last_name', '') or '')).strip(), 'username': str(getattr(user, 'username', '') or '').lstrip('@') or None, 'status': str(getattr(member, 'status', '') or ''), 'custom_title': str(getattr(member, 'custom_title', '') or '') or None})
            rows.sort(key=lambda r: r.get('id') or 0)
            if info.get('administrators') != rows:
                info['administrators'] = rows
                changed = True
            if info.get('administrator_count') != len(rows):
                info['administrator_count'] = len(rows)
                changed = True
        except Exception as exc:
            migrated = _v199_migration_target_from_probe_error(chat_id, exc, chat_obj)
            if migrated is not None:
                info['_probe_migrate_to_v199'] = int(migrated)
                changed = True
                return changed
            warnings.append('administrators:' + str(exc)[:120])
    new_warnings = warnings[-5:]
    if info.get('probe_warnings') != new_warnings:
        info['probe_warnings'] = new_warnings
        changed = True
    info['last_full_probe_at'] = now_local().isoformat(timespec='seconds')
    return changed

def _v197_chat_probe_fingerprint(chat_id: int) -> tuple:
    try:
        info = get_chat_store(int(chat_id)).get('info') or {}
        return (str(info.get('title') or ''), str(info.get('username') or ''), str(info.get('type') or ''), str(info.get('description') or ''), str(info.get('bio') or ''), json.dumps(info.get('telegram_profile_v197') or {}, ensure_ascii=False, sort_keys=True, default=str), info.get('member_count'), json.dumps(info.get('bot_membership') or {}, ensure_ascii=False, sort_keys=True, default=str), json.dumps(info.get('administrators') or [], ensure_ascii=False, sort_keys=True, default=str), bool(info.get('is_forum', False)), bool(info.get('has_protected_content', False)), info.get('linked_chat_id'), info.get('message_auto_delete_time'), info.get('slow_mode_delay'), bool(is_chat_bot_removed(int(chat_id))))
    except Exception:
        return tuple()

def _v177_legacy_0040_probe_bot_in_chat(chat_id: int, *, deep: bool=True, persist: bool=True, schedule_backup: bool=True, _migration_retry: bool=False) -> bool:
    """Probe a known chat end-to-end; migrations are canonicalized and dead forwarding can recover."""
    chat_id = int(chat_id)
    try:
        resolver = globals().get('resolve_canonical_chat_id_v199')
        if callable(resolver):
            canonical = int(resolver(chat_id))
            if canonical != chat_id and (not _migration_retry):
                return probe_bot_in_chat(canonical, deep=deep, persist=persist, schedule_backup=schedule_backup, _migration_retry=True)
    except Exception:
        pass
    try:
        chat_obj = _tg_call_retry(bot.get_chat, chat_id, attempts=2, purpose='probe_get_chat')
        changed = update_chat_info_from_chat_object(chat_obj, persist=False, schedule_backup=False)
        if deep:
            changed = bool(_v197_refresh_chat_probe_facts(chat_id, chat_obj)) or bool(changed)
            try:
                migrate_to = int((get_chat_store(chat_id).get('info') or {}).pop('_probe_migrate_to_v199', 0) or 0)
            except Exception:
                migrate_to = 0
            if migrate_to and migrate_to != chat_id:
                mig = globals().get('migrate_chat_id_everywhere')
                if callable(mig) and mig(chat_id, migrate_to, 'deep probe: Telegram group upgraded'):
                    return probe_bot_in_chat(migrate_to, deep=deep, persist=persist, schedule_backup=schedule_backup, _migration_retry=True)
        changed = bool(set_chat_bot_removed(chat_id, False, '', persist=False, schedule_backup=False)) or bool(changed)
        try:
            reactivate = globals().get('reactivate_forward_target_v199')
            if callable(reactivate) and reactivate(chat_id, persist=False):
                changed = True
        except Exception:
            pass
        if changed and persist:
            save_data(data)
        if changed and schedule_backup:
            try:
                ids = [chat_id] + ([int(OWNER_ID)] if OWNER_ID and chat_id != int(OWNER_ID) else [])
                schedule_config_backup_for_chats(*ids, delay=1.0)
            except Exception:
                pass
        return True
    except Exception as e:
        if isinstance(e, (TypeError, NameError, AttributeError)) or 'unexpected keyword argument' in str(e or '').casefold():
            log_error(f'probe_internal_error({get_chat_display_name(chat_id)}): {e}')
            return False
        migrate_to = _v199_migration_target_from_probe_error(chat_id, e, None)
        if migrate_to and (not _migration_retry):
            mig = globals().get('migrate_chat_id_everywhere')
            if callable(mig) and mig(chat_id, int(migrate_to), str(e)[:300]):
                return probe_bot_in_chat(int(migrate_to), deep=deep, persist=persist, schedule_backup=schedule_backup, _migration_retry=True)
        if _is_bot_removed_error(e):
            set_chat_bot_removed(chat_id, True, str(e)[:240], persist=persist, schedule_backup=schedule_backup)
        else:
            try:
                status_fn = globals().get('set_chat_status_v150')
                if callable(status_fn):
                    status_fn(chat_id, 'unreachable', str(e)[:500], source='telegram_probe')
            except Exception:
                pass
            if 'chat not found' in str(e or '').casefold():
                try:
                    suspend = globals().get('suspend_forward_target_v199')
                    if callable(suspend):
                        suspend(chat_id, str(e)[:500], persist=persist)
                except Exception as exc:
                    log_error(f'probe suspend {chat_id}: {exc}')
            log_error(f'probe_bot_in_chat({get_chat_display_name(chat_id)}): {e}')
        return False
try:
    _v177_legacy_0040_probe_bot_in_chat.__name__ = 'probe_bot_in_chat'
except Exception:
    pass
probe_bot_in_chat = _v177_legacy_0040_probe_bot_in_chat

def probe_all_known_chats() -> tuple[int, int]:
    """Full explicit Telegram sync for every known chat, INCLUDING the primary owner chat."""
    try:
        normalize_known_chats_for_owner()
    except Exception:
        pass
    ids_raw = collect_probe_chat_ids_v200(include_owner=True)
    resolver = globals().get('resolve_canonical_chat_id_v199')
    ids = []
    for raw in ids_raw:
        try:
            cid = int(resolver(int(raw))) if callable(resolver) else int(raw)
        except Exception:
            continue
        if cid not in ids:
            ids.append(cid)
    ok = bad = changed = renamed = 0
    processed = set()
    for raw_cid in ids:
        try:
            cid = int(resolver(int(raw_cid))) if callable(resolver) else int(raw_cid)
        except Exception:
            continue
        if cid in processed:
            continue
        before_title = get_chat_display_name(cid)
        before_fp = _v197_chat_probe_fingerprint(cid)
        probe_ok = False
        try:
            probe_ok = bool(probe_bot_in_chat(cid, deep=True, persist=False, schedule_backup=False))
        except Exception as exc:
            log_error(f'probe_all_known_chats({cid}): {exc}')
        try:
            after_cid = int(resolver(cid)) if callable(resolver) else cid
        except Exception:
            after_cid = cid
        processed.add(after_cid)
        if probe_ok:
            ok += 1
        elif is_chat_bot_removed(after_cid):
            bad += 1
        after_fp = _v197_chat_probe_fingerprint(after_cid)
        if cid != after_cid or before_fp != after_fp:
            changed += 1
        if cid != after_cid or before_title != get_chat_display_name(after_cid):
            renamed += 1
    try:
        normalize_known_chats_for_owner()
    except Exception:
        pass
    summary = {'checked': len(processed), 'available': ok, 'unavailable': bad, 'errors': max(0, len(processed) - ok - bad), 'changed': changed, 'renamed': renamed, 'at': now_local().isoformat(timespec='seconds')}
    try:
        data.setdefault('_global_settings', {})['last_chat_probe_summary_v197'] = summary
    except Exception:
        pass
    save_data(data)
    try:
        schedule_config_backup_for_chats(*ids, delay=1.0)
    except Exception:
        try:
            schedule_config_backup_for_chats()
        except Exception:
            pass
    try:
        bot_journal('chat_full_sync_v197', int(OWNER_ID or 0), json.dumps(summary, ensure_ascii=False, separators=(',', ':')))
    except Exception:
        pass
    return (ok, bad)

def build_removed_chats_menu(day_key: str | None=None):
    kb = types.InlineKeyboardMarkup(row_width=2)
    suspended = globals().get('is_forward_target_suspended_v199')
    removed = [cid for cid in collect_all_known_chat_ids(include_owner=False) if is_chat_bot_removed(cid) or (callable(suspended) and suspended(cid))]
    if removed:
        buttons = [IB(chat_button_title(cid, get_chat_display_name(cid)), callback_data=f'fw_probe_one:{cid}') for cid in removed]
        add_buttons_in_rows(kb, buttons, 2)
    else:
        kb.row(IB('Удалённых нет', callback_data='none'))
    kb.row(IB('📡 Проверить все', callback_data='fw_probe_all'))
    kb.row(IB('🔙 Назад', callback_data='fw_back_src' if day_key is None else f'd:{day_key}:forward_menu'))
    return kb

def set_hidden_finance_mode(chat_id: int, enabled: bool):
    """v108: hidden finance is independent from the three automatic finance-window modes."""
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    settings = store.setdefault('settings', {})
    settings['hidden_finance'] = bool(enabled)
    if enabled:
        set_finance_mode(chat_id, True)
    save_data(data, chat_ids=[chat_id])
    schedule_config_backup_for_chats(chat_id)

def force_recreate_balance_panel(chat_id: int):
    """Пересоздаёт быстрый остаток, чтобы он снова стал последним окном в чате."""
    if finance_window_mode(chat_id) not in {'open', 'first'}:
        return
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return
    store = get_chat_store(chat_id)
    panel_id = store.get('balance_panel_id')
    if panel_id:
        try:
            bot.delete_message(chat_id, int(panel_id))
        except Exception:
            pass
    store['balance_panel_id'] = None
    store['balance_panel_mode'] = 'mini'
    store['balance_panel_msg_count'] = 0
    save_data(data)
    send_minimized_balance_panel(chat_id)

def is_normal_finance_window_mode(chat_id: int) -> bool:
    """Как обычно: отдельный выбранный режим; hidden finance does not disable it."""
    try:
        return bool(is_finance_mode(chat_id) and finance_window_mode(chat_id) == 'normal')
    except Exception:
        return False

def schedule_main_window_recreate_after_quiet(chat_id: int, delay: float=4.0):
    try:
        chat_id = int(chat_id)
    except Exception:
        return
    if not is_finance_mode(chat_id) or finance_window_mode(chat_id) != 'normal':
        return

    def _job():
        try:
            with locked_chat(chat_id):
                store = get_chat_store(chat_id)
                if int(store.get('main_window_msg_count', 0) or 0) < 10:
                    return
                store['main_window_msg_count'] = 0
                day_key = store.get('current_view_day') or today_key()
                save_data(data)
            recreate_main_window_now(chat_id, day_key)
        except Exception as e:
            log_error(f'schedule_main_window_recreate_after_quiet({get_chat_display_name(chat_id)}): {e}')
    scheduler_key = f'main-window-recreate:{chat_id}'
    with timer_lock:
        DELAYED_SCHEDULER.cancel(scheduler_key)
        deadline = DELAYED_SCHEDULER.schedule(scheduler_key, delay, _job)
        _balance_panel_recreate_timers['main', chat_id] = deadline

def bump_quick_balance_recreate_counter(chat_id: int, count: int=1):
    """Сообщения после ввода: обычное окно через 10 сообщений или быстрый остаток по выбранному режиму."""
    try:
        if not is_finance_mode(chat_id) or finance_window_mode(chat_id) == 'off':
            return
        if is_normal_finance_window_mode(chat_id):
            store = get_chat_store(chat_id)
            cur = int(store.get('main_window_msg_count', 0) or 0) + int(count or 1)
            store['main_window_msg_count'] = cur
            save_data(data)
            if cur >= 10:
                schedule_main_window_recreate_after_quiet(chat_id, delay=4.0)
            return
        if not is_quick_balance_enabled(chat_id):
            return
        if get_quick_balance_behavior(chat_id) == 'first':
            schedule_quick_balance_first_recreate(chat_id)
        store = get_chat_store(chat_id)
        cur = int(store.get('balance_panel_msg_count', 0) or 0) + int(count or 1)
        store['balance_panel_msg_count'] = cur
        save_data(data)
        if cur >= 3:
            schedule_quick_balance_recreate_after_quiet(chat_id, delay=4.0)
    except Exception as e:
        log_error(f'bump_quick_balance_recreate_counter({get_chat_display_name(chat_id)}): {e}')

def schedule_quick_balance_first_recreate(chat_id: int, delay: float=60.0):
    """Режим «всегда быть первым»: если минуту нет новых сообщений, пересоздаём быстрый остаток."""
    try:
        chat_id = int(chat_id)
    except Exception:
        return
    if finance_window_mode(chat_id) != 'first':
        return
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return
    if get_quick_balance_behavior(chat_id) != 'first':
        return

    def _job():
        try:
            with locked_chat(chat_id):
                if finance_window_mode(chat_id) != 'first':
                    return
                if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
                    return
                if get_quick_balance_behavior(chat_id) != 'first':
                    return
                force_recreate_balance_panel(chat_id)
        except Exception as e:
            log_error(f'schedule_quick_balance_first_recreate({chat_id}): {e}')
    scheduler_key = f'quick-balance-first:{chat_id}'
    with timer_lock:
        DELAYED_SCHEDULER.cancel(scheduler_key)
        deadline = DELAYED_SCHEDULER.schedule(scheduler_key, delay, _job)
        _balance_panel_first_timers[chat_id] = deadline

def schedule_quick_balance_recreate_after_quiet(chat_id: int, delay: float=4.0):
    """Debounce для быстрого остатка: пересоздать только когда поток сообщений стих."""
    try:
        chat_id = int(chat_id)
    except Exception:
        return
    if finance_window_mode(chat_id) not in {'open', 'first'}:
        return
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return

    def _job():
        try:
            with locked_chat(chat_id):
                store = get_chat_store(chat_id)
                if int(store.get('balance_panel_msg_count', 0) or 0) < 3:
                    return
                store['balance_panel_msg_count'] = 0
                save_data(data)
                force_recreate_balance_panel(chat_id)
        except Exception as e:
            log_error(f'schedule_quick_balance_recreate_after_quiet({get_chat_display_name(chat_id)}): {e}')
    scheduler_key = f'quick-balance-recreate:{chat_id}'
    with timer_lock:
        DELAYED_SCHEDULER.cancel(scheduler_key)
        deadline = DELAYED_SCHEDULER.schedule(scheduler_key, delay, _job)
        _balance_panel_recreate_timers[chat_id] = deadline

def _set_panel_open_state(chat_id: int, message_id: int):
    store = get_chat_store(chat_id)
    store['balance_panel_id'] = message_id
    store['balance_panel_mode'] = 'open'
    store['balance_panel_msg_count'] = 0
    save_data(data)
    _sync_finance_window_state_from_runtime(chat_id, schedule_delta=True)
    schedule_balance_panel_collapse(chat_id)

def schedule_owner_total_window_delete(chat_id: int, message_id: int, delay: int | float | None=None):
    """
    v104 compatibility shim. Окно «Общий итог» больше не удаляется по отдельному
    таймеру: как и остальные обычные окна, по единому глобальному таймеру оно
    возвращается в основное окно. Секретные режимы этой логикой не затрагиваются.
    """
    key = int(chat_id)
    if delay is None:
        delay = internal_timer_seconds('window_auto_return', 120)
    try:
        DELAYED_SCHEDULER.cancel(f'owner-total-delete:{key}')
    except Exception:
        pass
    schedule_stored_window_delete(chat_id, 'total_msg_id', float(delay))
    _total_message_timers[key] = _aux_window_timers.get((key, 'total_msg_id'))
_aux_window_timers = {}

def _clear_stored_window(chat_id: int, store_key: str, message_id: int | None=None):
    try:
        store = get_chat_store(chat_id)
        current = store.get(store_key)
        if not current:
            return
        if message_id is not None and int(current) != int(message_id):
            return
        store[store_key] = None
        if current:
            unregister_open_window(chat_id, int(current))
        save_data(data)
    except Exception as e:
        log_error(f'_clear_stored_window({chat_id},{store_key}): {e}')

def schedule_stored_window_delete(chat_id: int, store_key: str, delay: int | float | None=None):
    key = (int(chat_id), str(store_key))
    if delay is None:
        delay = internal_timer_seconds('window_auto_return', 120)

    def _job():
        try:
            store = get_chat_store(chat_id)
            message_id = store.get(store_key)
            if not message_id:
                return
            if store.get(store_key) == message_id:
                store[store_key] = None
                unregister_open_window(chat_id, int(message_id))
                _aux_window_timers.pop(key, None)
                save_data(data)
            day_key = store.get('current_view_day') or today_key()
            return_to_main_window_closing_previous(chat_id, day_key, int(message_id))
        except Exception as e:
            log_error(f'schedule_stored_window_delete({chat_id},{store_key}): {e}')
    scheduler_key = f'stored-window-delete:{int(chat_id)}:{str(store_key)}'
    DELAYED_SCHEDULER.cancel(scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, float(delay), _job)
    _aux_window_timers[key] = deadline

def default_window_nav_keyboard(chat_id: int):
    """Кнопки для окон, где раньше не было кнопок: закрыть + назад в основное окно."""
    kb = types.InlineKeyboardMarkup()
    day = get_chat_store(chat_id).get('current_view_day') or today_key()
    kb.row(IB('⬅️ Назад осн. окно', callback_data=f'd:{day}:back_main'), IB('❌ Закрыть', callback_data='aux_close'))
    return kb

def ensure_main_back_nav_keyboard(reply_markup, chat_id: int, day_key: str | None=None):
    """Добавляет «Назад осн. окно» к служебным окнам, если такой кнопки ещё нет.

    Основное финансовое окно не меняется. Функция безопасна для уже собранных
    InlineKeyboardMarkup и вызывается центрально перед редактированием окна.
    """
    if reply_markup is None:
        return default_window_nav_keyboard(chat_id)
    try:
        rows = list(getattr(reply_markup, 'keyboard', None) or [])
        callbacks = []
        labels = []
        for row in rows:
            for btn in row or []:
                callbacks.append(str(getattr(btn, 'callback_data', '') or ''))
                labels.append(str(getattr(btn, 'text', '') or ''))
        if any((cb.startswith('main_close:') for cb in callbacks)):
            return reply_markup
        if any(('back_main' in cb for cb in callbacks)) or any(('Назад осн' in t for t in labels)):
            return reply_markup
        day = str(day_key or get_chat_store(int(chat_id)).get('current_view_day') or today_key())[:10]
        reply_markup.row(IB('⬅️ Назад осн. окно', callback_data=f'd:{day}:back_main'))
    except Exception:
        pass
    return reply_markup

def _open_window_registry() -> dict:
    return data.setdefault('open_window_registry', {})

def _v177_legacy_0041_register_open_window(chat_id: int, message_id: int, window_type: str, code: str='', day_key: str | None=None, params: dict | None=None):
    try:
        chat_id = int(chat_id)
        message_id = int(message_id)
        key = f'{owner_scope_id(chat_id)}:{chat_id}:{message_id}'
        params = params or {}
        currency_chat_id = chat_id
        try:
            if params.get('target_chat_id') is not None:
                currency_chat_id = int(params.get('target_chat_id'))
        except Exception:
            currency_chat_id = chat_id
        _open_window_registry()[key] = {'owner_id': owner_scope_id(chat_id), 'chat_id': chat_id, 'message_id': message_id, 'window_type': str(window_type or ''), 'code': str(code or ''), 'currency_mode': currency_mode(currency_chat_id) if 'currency_mode' in globals() else 'ars', 'day_key': day_key, 'params': params, 'updated_at': now_local().isoformat(timespec='seconds')}
        save_data(data, root_only=True)
    except Exception as e:
        log_error(f'register_open_window: {e}')
try:
    _v177_legacy_0041_register_open_window.__name__ = 'register_open_window'
except Exception:
    pass

def _v177_legacy_0042_unregister_open_window(chat_id: int, message_id: int):
    try:
        chat_id = int(chat_id)
        message_id = int(message_id)
        reg = _open_window_registry()
        changed = False
        for key, item in list(reg.items()):
            if int(item.get('chat_id', 0) or 0) == chat_id and int(item.get('message_id', 0) or 0) == message_id:
                reg.pop(key, None)
                changed = True
        if changed:
            save_data(data, root_only=True)
    except Exception:
        pass
try:
    _v177_legacy_0042_unregister_open_window.__name__ = 'unregister_open_window'
except Exception:
    pass

def _v177_legacy_0043_get_registered_open_window(chat_id: int, message_id: int) -> dict | None:
    """Возвращает фактическое последнее состояние конкретного Telegram-сообщения."""
    try:
        chat_id = int(chat_id)
        message_id = int(message_id)
        best = None
        for item in (_open_window_registry() or {}).values():
            if int((item or {}).get('chat_id', 0) or 0) != chat_id:
                continue
            if int((item or {}).get('message_id', 0) or 0) != message_id:
                continue
            best = item
        return best
    except Exception:
        return None
try:
    _v177_legacy_0043_get_registered_open_window.__name__ = 'get_registered_open_window'
except Exception:
    pass

def register_static_open_view(chat_id: int, message_id: int, code: str='', day_key: str | None=None, params: dict | None=None):
    """Помечает открытое меню как фактически открытое, чтобы фин-синхронизация не превращала его обратно в О1."""
    register_open_window(chat_id, message_id, 'static_view', code=code, day_key=day_key, params=params or {})

def _message_missing_error(exc) -> bool:
    text = str(exc or '').lower()
    return any((x in text for x in ('message to edit not found', 'message not found', 'message_id_invalid', "message can't be edited", 'chat not found', 'bot was blocked', 'forbidden')))

def _markup_callback_values(reply_markup) -> list[str]:
    out = []
    try:
        for row in getattr(reply_markup, 'keyboard', None) or getattr(reply_markup, 'inline_keyboard', None) or []:
            for btn in row:
                cb = getattr(btn, 'callback_data', None)
                if cb:
                    out.append(str(cb))
    except Exception:
        pass
    return out

def _refresh_categories_window_from_state(chat_id: int) -> bool:
    """Перерисовывает основные зависимые окна статей по сохранённому состоянию."""
    store = get_chat_store(chat_id)
    mid = store.get('categories_msg_id')
    state = store.get('categories_refresh_state') or {}
    if not mid or not state:
        return False
    marker = str(state.get('marker_action') or '')
    callbacks = [str(x) for x in state.get('callbacks') or []]
    try:
        if marker.startswith('cat_range_records'):
            cb = next((x for x in callbacks if x.startswith('cat_show_records:')), None)
            if cb:
                _, start_key, start_rid, end_key, end_rid, _slug = cb.split(':', 5)
                text, _ = summarize_categories_record_range(store, start_key, int(start_rid), end_key, int(end_rid))
                kb = build_categories_record_summary_keyboard(start_key, int(start_rid), end_key, int(end_rid), store)
                send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=int(mid), marker_action='cat_range_records:*')
                return True
        if marker.startswith(('cat_order_open_sum', 'cat_order_move_sum', 'cat_order_select_sum', 'cat_order_position_sum')):
            cb = next((x for x in callbacks if x.startswith('cat_order_select_sum:')), None)
            if cb:
                _, _slug, mode, start, end = cb.split(':', 4)
                send_or_edit_categories_window(chat_id, build_category_layout_text(store, 'sum'), reply_markup=build_category_layout_keyboard(store, 'sum', (mode, start, end), chat_id=chat_id), preferred_message_id=int(mid), marker_action='cat_order_open_sum:*')
                return True
        if marker.startswith(('cat_order_open_exact', 'cat_order_move_exact', 'cat_order_select_exact', 'cat_order_position_exact')):
            cb = next((x for x in callbacks if x.startswith('cat_order_select_exact:')), None)
            if cb:
                _, _slug, start_key, start_rid, end_key, end_rid = cb.split(':', 5)
                params = (start_key, int(start_rid), end_key, int(end_rid))
                send_or_edit_categories_window(chat_id, build_category_layout_text(store, 'exact'), reply_markup=build_category_layout_keyboard(store, 'exact', params, chat_id=chat_id), preferred_message_id=int(mid), marker_action='cat_order_open_exact:*')
                return True
    except Exception as e:
        if _message_missing_error(e):
            unregister_open_window(chat_id, int(mid))
            store['categories_msg_id'] = None
            store['categories_refresh_state'] = None
        else:
            log_error(f'_refresh_categories_window_from_state({chat_id}): {e}')
    return False

def _refresh_registered_fin_view(item: dict, changed_chat_id: int) -> bool:
    """Перерисовывает окно владельца, которое показывает финансы другого чата."""
    params = item.get('params') or {}
    try:
        target_chat_id = int(params.get('target_chat_id') or 0)
        host_chat_id = int(item.get('chat_id') or 0)
        message_id = int(item.get('message_id') or 0)
    except Exception:
        return False
    if target_chat_id != int(changed_chat_id) or not host_chat_id or (not message_id):
        return False
    view_day = str(item.get('day_key') or params.get('view_day') or get_chat_store(target_chat_id).get('current_view_day') or today_key())
    owner_day_key = str(params.get('owner_day_key') or get_chat_store(host_chat_id).get('current_view_day') or today_key())
    action = str(params.get('view_action') or 'open')
    target_store = get_chat_store(target_chat_id)
    try:
        if action in {'open', 'back_main', 'menu', 'clear_delete_back'}:
            text = render_fin_window_text(target_chat_id, view_day)
            kb = build_fin_window_view_keyboard(target_chat_id, view_day, owner_day_key)
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=kb, parse_mode='HTML')
        elif action in {'edit_list', 'del_toggle'}:
            text = render_fin_window_text(target_chat_id, view_day)
            kb = build_edit_records_keyboard(view_day, target_chat_id, prefix='fv', owner_day_key=owner_day_key)
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=kb, parse_mode='HTML')
        elif action == 'calendar':
            try:
                cdt = datetime.strptime(str(params.get('center_day') or view_day), '%Y-%m-%d')
            except Exception:
                cdt = now_local()
            bot.edit_message_text(f'📅 Календарь: {html.escape(get_chat_display_name(target_chat_id))}', chat_id=host_chat_id, message_id=message_id, reply_markup=build_fin_calendar_keyboard(target_chat_id, cdt, owner_day_key), parse_mode='HTML')
        elif action == 'report':
            try:
                month_key = datetime.strptime(view_day, '%Y-%m-%d').strftime('%Y-%m')
            except Exception:
                month_key = now_local().strftime('%Y-%m')
            report_html, _ = build_month_report_text(target_chat_id, month_key)
            bot.edit_message_text(f'👁 {html.escape(get_chat_display_name(target_chat_id))}\n' + report_html, chat_id=host_chat_id, message_id=message_id, reply_markup=ensure_main_back_nav_keyboard(_one_button_keyboard('🔙 Назад', f'fv:{target_chat_id}:{view_day}:open:{owner_day_key}'), host_chat_id, owner_day_key), parse_mode='HTML')
        elif action == 'usd_month':
            month_html, _ = render_usd_month_window(target_chat_id, view_day)
            bot.edit_message_text(f'👁 {html.escape(get_chat_display_name(target_chat_id))}\n' + month_html, chat_id=host_chat_id, message_id=message_id, reply_markup=build_fin_window_usd_month_keyboard(target_chat_id, view_day, owner_day_key), parse_mode='HTML')
        elif action == 'total':
            text = f"👁 {html.escape(get_chat_display_name(target_chat_id))}\n\n💰 Общий итог по чату: {format_chat_amount(target_chat_id, target_store.get('balance', 0), True)}"
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=build_fin_window_view_keyboard(target_chat_id, view_day, owner_day_key), parse_mode='HTML')
        elif action == 'info':
            text = build_info_text(target_chat_id) + '\n\n' + build_articles_description_text(target_chat_id)
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=build_fin_window_view_keyboard(target_chat_id, view_day, owner_day_key))
        elif action == 'csv_menu':
            text = wm_common(f'📂 CSV / Excel: {html.escape(get_chat_display_name(target_chat_id))}\nВыберите период:', 5)
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=build_fin_window_csv_menu(target_chat_id, view_day, owner_day_key), parse_mode='HTML')
        else:
            return False
        register_open_window(host_chat_id, message_id, 'fin_view', code=f'fv:{action}', day_key=view_day, params={'target_chat_id': target_chat_id, 'owner_day_key': owner_day_key, 'view_action': action})
        return True
    except Exception as e:
        if 'message is not modified' in str(e).lower():
            return True
        if _message_missing_error(e):
            unregister_open_window(host_chat_id, message_id)
            return False
        log_error(f'_refresh_registered_fin_view({host_chat_id},{message_id}->{target_chat_id}): {e}')
        return False

def _build_total_window_text_for_registry(chat_id: int) -> str:
    """Тот же итог, что показывает кнопка «💰 Общий итог», но пригодный для автообновления реестра."""
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    chat_bal = store.get('balance', 0)
    if not is_owner_chat(chat_id):
        return wm_common(f'💰 Общий итог по этому чату: {format_chat_amount(chat_id, chat_bal, True)}', 4)
    lines = ['💰 Общий итог (для владельца)', '', f'• Этот чат ({get_chat_display_name(chat_id)}): {format_chat_amount(chat_id, chat_bal, True)}']
    total_all = 0
    other_lines = []
    for cid, st in (data.get('chats', {}) or {}).items():
        try:
            cid_int = int(cid)
        except Exception:
            continue
        bal = st.get('balance', 0)
        total_all += bal
        if cid_int == chat_id:
            continue
        other_lines.append(f'   • {get_chat_display_name(cid_int)}: {format_chat_amount(chat_id, bal, True)}')
    if other_lines:
        lines.extend(['', '• Другие чаты:'])
        lines.extend(other_lines)
    lines.extend(['', f'• Всего по всем чатам: {format_chat_amount(chat_id, total_all, True)}'])
    return wm_common('\n'.join(lines), 4)

def _refresh_registered_local_fin_view(item: dict, changed_chat_id: int) -> bool:
    """Сохраняет фактически открытый локальный финансовый экран, а не возвращает сообщение принудительно в О1."""
    params = item.get('params') or {}
    try:
        host_chat_id = int(item.get('chat_id') or 0)
        message_id = int(item.get('message_id') or 0)
    except Exception:
        return False
    if not host_chat_id or not message_id:
        return False
    action = str(params.get('view_action') or item.get('code') or '')
    depends_on_all = bool(params.get('depends_on_all'))
    if host_chat_id != int(changed_chat_id) and (not depends_on_all):
        return False
    view_day = str(item.get('day_key') or params.get('view_day') or get_chat_store(host_chat_id).get('current_view_day') or today_key())
    try:
        if action == 'calendar':
            center_s = str(params.get('center_day') or view_day)
            try:
                center_dt = datetime.strptime(center_s, '%Y-%m-%d')
            except Exception:
                center_dt = now_local()
            bot.edit_message_text(calendar_window_text(center_dt), chat_id=host_chat_id, message_id=message_id, reply_markup=build_calendar_keyboard(center_dt, host_chat_id))
        elif action == 'report':
            month_key = str(params.get('month_key') or view_day[:7])
            report_html, _ = build_month_report_text(host_chat_id, month_key)
            bot.edit_message_text(report_html, chat_id=host_chat_id, message_id=message_id, reply_markup=build_report_keyboard(month_key), parse_mode='HTML')
        elif action == 'usd_month':
            month_day = str(params.get('month_day') or view_day)
            month_html, _ = render_usd_month_window(host_chat_id, month_day)
            bot.edit_message_text(month_html, chat_id=host_chat_id, message_id=message_id, reply_markup=build_usd_month_keyboard(month_day), parse_mode='HTML')
        elif action == 'total':
            bot.edit_message_text(_build_total_window_text_for_registry(host_chat_id), chat_id=host_chat_id, message_id=message_id, parse_mode='HTML', reply_markup=default_window_nav_keyboard(host_chat_id))
        elif action == 'info':
            bot.edit_message_text(wm_common(build_info_text(host_chat_id), 9), chat_id=host_chat_id, message_id=message_id, reply_markup=build_info_keyboard(host_chat_id))
        elif action == 'csv_menu':
            txt, _ = render_day_window(host_chat_id, view_day)
            bot.edit_message_text(txt, chat_id=host_chat_id, message_id=message_id, reply_markup=build_csv_menu(view_day, host_chat_id), parse_mode='HTML')
        elif action == 'edit_list':
            txt, _ = render_day_window(host_chat_id, view_day)
            bot.edit_message_text(txt, chat_id=host_chat_id, message_id=message_id, reply_markup=build_edit_records_keyboard(view_day, host_chat_id), parse_mode='HTML')
        else:
            return False
        register_open_window(host_chat_id, message_id, 'local_fin_view', code=action, day_key=view_day, params={**params, 'view_action': action})
        return True
    except Exception as e:
        if 'message is not modified' in str(e).lower():
            return True
        if _message_missing_error(e):
            unregister_open_window(host_chat_id, message_id)
            return False
        log_error(f'_refresh_registered_local_fin_view({host_chat_id},{message_id},{action}): {e}')
        return False

def _refresh_registered_fin_categories_view(item: dict, changed_chat_id: int) -> bool:
    """Автообновление открытых у владельца окон статей чужого/связанного чата."""
    params = item.get('params') or {}
    try:
        host_chat_id = int(item.get('chat_id') or 0)
        message_id = int(item.get('message_id') or 0)
        target_chat_id = int(params.get('target_chat_id') or 0)
    except Exception:
        return False
    if target_chat_id != int(changed_chat_id) or not host_chat_id or (not message_id):
        return False
    action = str(params.get('view_action') or '')
    owner_day_key = str(params.get('owner_day_key') or today_key())
    store = get_chat_store(target_chat_id)
    try:
        if action == 'wthu':
            ref = str(params.get('ref') or today_key())
            start_key = week_start_thursday(ref)
            start, end = week_bounds_thu_wed(start_key)
            label = f'{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)} (Чт–Ср)'
            text, _ = summarize_categories(store, start, end, label)
            text = f'👁 {get_chat_display_name(target_chat_id)}\n' + text
            kb = build_fin_categories_summary_keyboard(target_chat_id, 'wthu', start, end, owner_day_key)
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=kb)
        elif action == 'show':
            start = str(params.get('start') or today_key())
            end = str(params.get('end') or start)
            slug = str(params.get('slug') or '')
            category = get_category_by_slug(slug, store)
            if not category:
                return False
            label = f'{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)}'
            text = f'👁 {get_chat_display_name(target_chat_id)}\n' + build_category_detail_text(store, start, end, category, label)
            kb = build_fin_categories_summary_keyboard(target_chat_id, 'detail', start, end, owner_day_key)
            kb.row(IB('🔙 Назад', callback_data=fvcat_callback(f'fvcat_wthu:{target_chat_id}:{start}:{owner_day_key}')))
            kb.row(IB('🔙 К окну чата', callback_data=f'fv:{target_chat_id}:{start}:open:{owner_day_key}'))
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=kb)
        else:
            return False
        register_open_window(host_chat_id, message_id, 'fin_categories_view', code=f'fvcat:{action}', day_key=item.get('day_key'), params=params)
        return True
    except Exception as e:
        if 'message is not modified' in str(e).lower():
            return True
        if _message_missing_error(e):
            unregister_open_window(host_chat_id, message_id)
            return False
        log_error(f'_refresh_registered_fin_categories_view({host_chat_id},{message_id}->{target_chat_id},{action}): {e}')
        return False

def _refresh_registered_stored_window(item: dict, changed_chat_id: int) -> bool:
    """Перерисовывает известные отдельные окна текущего чата, зависящие от финансов/настроек."""
    try:
        host_chat_id = int(item.get('chat_id') or 0)
        message_id = int(item.get('message_id') or 0)
    except Exception:
        return False
    if host_chat_id != int(changed_chat_id) or not message_id:
        return False
    code = str(item.get('code') or '')
    store = get_chat_store(host_chat_id)
    try:
        if code == 'info_msg_id':
            bot.edit_message_text(build_info_text(host_chat_id), chat_id=host_chat_id, message_id=message_id, reply_markup=build_info_keyboard(host_chat_id))
            return True
        if code == 'report_window_id':
            month_key = str(store.get('report_month') or now_local().strftime('%Y-%m'))
            text, _ = build_month_report_text(host_chat_id, month_key)
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=build_report_keyboard(month_key), parse_mode='HTML')
            return True
    except Exception as e:
        if 'message is not modified' in str(e).lower():
            return True
        if _message_missing_error(e):
            unregister_open_window(host_chat_id, message_id)
            if store.get(code) == message_id:
                store[code] = None
                save_data(data, chat_ids=[host_chat_id])
            return False
        log_error(f'_refresh_registered_stored_window({host_chat_id},{message_id},{code}): {e}')
    return False

def _v177_legacy_0044_refresh_registered_financial_windows(chat_id: int):
    """Обновляет известные открытые окна текущего owner scope после изменения финансов."""
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    for day_key, mid in list((get_or_create_active_windows(chat_id) or {}).items()):
        try:
            actual = get_registered_open_window(chat_id, int(mid))
            if actual and str(actual.get('window_type') or '') not in {'', 'main_day'}:
                continue
            text, _ = render_day_window(chat_id, day_key)
            bot.edit_message_text(text, chat_id=chat_id, message_id=int(mid), reply_markup=build_main_keyboard(day_key, chat_id))
            register_open_window(chat_id, int(mid), 'main_day', code='О1', day_key=day_key)
        except Exception as e:
            if 'message is not modified' in str(e).lower():
                continue
            if _message_missing_error(e):
                clear_active_window_id(chat_id, day_key)
                unregister_open_window(chat_id, int(mid))
    mid = store.get('remaining_msg_id')
    if mid:
        day_key = store.get('current_view_day') or today_key()
        try:
            bot.edit_message_text(build_remaining_text(chat_id, day_key), chat_id=chat_id, message_id=int(mid), reply_markup=build_remaining_keyboard(chat_id, day_key), parse_mode='HTML')
            register_open_window(chat_id, int(mid), 'remaining', code='Ф91', day_key=day_key)
        except Exception as e:
            if _message_missing_error(e):
                store['remaining_msg_id'] = None
                unregister_open_window(chat_id, int(mid))
    _refresh_categories_window_from_state(chat_id)
    for _key, item in list((_open_window_registry() or {}).items()):
        try:
            wtype = str((item or {}).get('window_type') or '')
            if wtype == 'fin_view':
                _refresh_registered_fin_view(item, chat_id)
            elif wtype == 'local_fin_view':
                _refresh_registered_local_fin_view(item, chat_id)
            elif wtype == 'fin_categories_view':
                _refresh_registered_fin_categories_view(item, chat_id)
            elif wtype == 'stored':
                _refresh_registered_stored_window(item, chat_id)
        except Exception as e:
            log_error(f'refresh_registered_financial_windows registry item: {e}')
try:
    _v177_legacy_0044_refresh_registered_financial_windows.__name__ = 'refresh_registered_financial_windows'
except Exception:
    pass

def send_or_edit_stored_window(chat_id: int, store_key: str, text: str, reply_markup=None, parse_mode=None, delay: int | float | None=None):
    store = get_chat_store(chat_id)
    if reply_markup is None:
        try:
            reply_markup = default_window_nav_keyboard(chat_id)
        except Exception:
            pass
    try:
        reply_markup = ensure_main_back_nav_keyboard(reply_markup, chat_id, store.get('current_view_day'))
    except Exception:
        pass
    try:
        marker_key = f'stored:{store_key}:' + _window_key_from_markup(reply_markup)
        text = window_mark(text, _window_marker_code(marker_key), html_mode=str(parse_mode or '').upper() == 'HTML')
    except Exception:
        pass
    message_id = store.get(store_key)
    recreate_from = 0
    recreate_reason = ''
    if message_id and str(store_key) == 'command_window_id':
        fast_edit = globals().get('fast_ui_edit_message_text')
        if callable(fast_edit):
            try:
                result = str(fast_edit(int(chat_id), int(message_id), text, reply_markup=reply_markup, parse_mode=parse_mode, purpose='commander_window_v246') or '')
                if result in {'ok', 'scheduled', 'not_modified'}:
                    register_open_window(chat_id, message_id, 'stored', code=store_key, day_key=store.get('current_view_day'))
                    schedule_stored_window_delete(chat_id, store_key, delay)
                    return message_id
            except Exception:
                pass
    if message_id:
        try:
            bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=reply_markup, parse_mode=parse_mode)
            register_open_window(chat_id, message_id, 'stored', code=store_key, day_key=store.get('current_view_day'))
            schedule_stored_window_delete(chat_id, store_key, delay)
            return message_id
        except Exception as e:
            if 'message is not modified' in str(e).lower():
                register_open_window(chat_id, message_id, 'stored', code=store_key, day_key=store.get('current_view_day'))
                schedule_stored_window_delete(chat_id, store_key, delay)
                return message_id
            try:
                bot.edit_message_caption(chat_id=chat_id, message_id=message_id, caption=text, reply_markup=reply_markup, parse_mode=parse_mode)
                register_open_window(chat_id, message_id, 'stored', code=store_key, day_key=store.get('current_view_day'))
                schedule_stored_window_delete(chat_id, store_key, delay)
                return message_id
            except Exception as e2:
                if 'message is not modified' in str(e2).lower():
                    schedule_stored_window_delete(chat_id, store_key, delay)
                    return message_id
                recreate_from = int(message_id or 0)
                recreate_reason = f'text={str(e)[:180]} | caption={str(e2)[:180]}'
                try:
                    diag_note = globals().get('window_diag_note_recreate')
                    if callable(diag_note):
                        diag_note(chat_id, recreate_from, recreate_reason, f'stored:{store_key}')
                except Exception:
                    pass
                unregister_open_window(chat_id, message_id)
                store[store_key] = None
                save_data(data)
    diag_context = globals().get('window_diag_context')
    if recreate_from and callable(diag_context):
        with diag_context(purpose=f'stored:{store_key}:send_fallback', recreate_from=recreate_from, recreate_reason=recreate_reason, window_force=True):
            sent = bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
    else:
        sent = bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
    store[store_key] = sent.message_id
    register_open_window(chat_id, sent.message_id, 'stored', code=store_key, day_key=store.get('current_view_day'))
    save_data(data)
    schedule_stored_window_delete(chat_id, store_key, delay)
    return sent.message_id

def _v177_legacy_0046_is_primary_owner(chat_id: int) -> bool:
    return bool(OWNER_ID and str(chat_id) == str(OWNER_ID))
try:
    _v177_legacy_0046_is_primary_owner.__name__ = 'is_primary_owner'
except Exception:
    pass

def _v177_legacy_0047_get_additional_owner_ids() -> set[int]:
    try:
        raw = data.setdefault('_global_settings', {}).setdefault('additional_owner_ids', [])
        return {int(x) for x in raw}
    except Exception:
        return set()
try:
    _v177_legacy_0047_get_additional_owner_ids.__name__ = 'get_additional_owner_ids'
except Exception:
    pass

def _v177_legacy_0048_set_additional_owner(user_id: int, enabled: bool):
    user_id = int(user_id)
    owners = get_additional_owner_ids()
    if enabled:
        owners.add(user_id)
        finance_active_chats.add(user_id)
        store = get_chat_store(user_id)
        store.setdefault('settings', {})['owner_scope_id'] = int(user_id)
        store.setdefault('settings', {}).setdefault('owner_scope_settings', {})
    else:
        owners.discard(user_id)
    data.setdefault('_global_settings', {})['additional_owner_ids'] = sorted(owners)
    save_data(data)
    schedule_config_backup_for_chats(user_id)
try:
    _v177_legacy_0048_set_additional_owner.__name__ = 'set_additional_owner'
except Exception:
    pass

def _v177_legacy_0049_is_owner_chat(chat_id: int) -> bool:
    try:
        return is_primary_owner(chat_id) or int(chat_id) in get_additional_owner_ids()
    except Exception:
        return is_primary_owner(chat_id)
try:
    _v177_legacy_0049_is_owner_chat.__name__ = 'is_owner_chat'
except Exception:
    pass

def _v177_legacy_0050_owner_scope_id(chat_id: int | None=None) -> int:
    """Logical owner namespace. Each additional owner keeps an independent settings world."""
    try:
        cid = int(chat_id) if chat_id is not None else int(OWNER_ID or 0)
    except Exception:
        cid = int(OWNER_ID or 0)
    if cid and is_owner_chat(cid):
        return cid
    try:
        store = get_chat_store(cid) if cid else {}
        scoped = int((store.get('settings') or {}).get('owner_scope_id') or 0)
        if scoped and is_owner_chat(scoped):
            return scoped
    except Exception:
        pass
    return int(OWNER_ID or cid or 0)
try:
    _v177_legacy_0050_owner_scope_id.__name__ = 'owner_scope_id'
except Exception:
    pass

def _v177_legacy_0051_owner_scoped_settings(chat_id: int | None=None) -> dict:
    scope = owner_scope_id(chat_id)
    if not scope:
        return data.setdefault('_global_settings', {})
    store = get_chat_store(scope)
    settings = store.setdefault('settings', {})
    return settings.setdefault('owner_scope_settings', {})
try:
    _v177_legacy_0051_owner_scoped_settings.__name__ = 'owner_scoped_settings'
except Exception:
    pass

def _v177_legacy_0052_bind_chat_to_owner_scope(chat_id: int, scope_id: int):
    try:
        get_chat_store(int(chat_id)).setdefault('settings', {})['owner_scope_id'] = int(scope_id)
        save_data(data, chat_ids=[int(chat_id)])
    except Exception as e:
        log_error(f'bind_chat_to_owner_scope({chat_id},{scope_id}): {e}')
try:
    _v177_legacy_0052_bind_chat_to_owner_scope.__name__ = 'bind_chat_to_owner_scope'
except Exception:
    pass

def is_backup_channel_chat(chat_id: int) -> bool:
    """True только для служебного backup-канала, если он задан."""
    return bool(BACKUP_CHAT_ID and str(chat_id) == str(BACKUP_CHAT_ID))

def can_receive_direct_json_backup(chat_id: int) -> bool:
    """JSON прямо в чат отправляем только владельцу или в backup-канал."""
    return is_owner_chat(chat_id) or is_backup_channel_chat(chat_id)

def schedule_command_delete(msg):
    try:
        bot_journal('command_received', msg.chat.id, getattr(msg, 'text', ''))
    except Exception:
        pass
    try:
        delete_message_later(msg.chat.id, msg.message_id, internal_timer_seconds('command_cleanup', COMMAND_DELETE_DELAY))
    except Exception:
        pass

def guard_non_owner_finance_for_command(msg, allowed_commands=None) -> bool:
    allowed = {c.lower().lstrip('/') for c in allowed_commands or []}
    chat_id = msg.chat.id
    if is_owner_chat(chat_id):
        return False
    if is_finance_output_suppressed(chat_id):
        return True
    text = (getattr(msg, 'text', None) or '').strip().lower()
    cmd = text.split()[0].split('@')[0].lstrip('/') if text else ''
    if cmd in allowed:
        return False
    if not is_finance_mode(chat_id):
        send_and_auto_delete(chat_id, '⚙️ Для этого включите финансовый режим командой /ok', HELPER_DELETE_DELAY)
        return True
    return False

def guard_non_owner_finance_for_callback(chat_id: int, data_str: str) -> bool:
    if is_owner_chat(chat_id):
        return False
    if is_finance_output_suppressed(chat_id):
        if finance_window_mode(chat_id) in {'normal', 'open', 'first'}:
            return False
        return True
    if is_finance_mode(chat_id):
        return False
    if data_str in {'info_close', 'main_articles_toggle', 'main_financial_values_toggle'}:
        return False
    if data_str.startswith('d:') and data_str.endswith(':info'):
        return False
    send_and_auto_delete(chat_id, '⚙️ Для этого включите финансовый режим командой /ok', HELPER_DELETE_DELAY)
    return True

def add_buttons_in_rows(kb, buttons, per_row: int=3):
    for i in range(0, len(buttons), per_row):
        kb.row(*buttons[i:i + per_row])
    return kb

def _v177_legacy_0053_build_help_text(chat_id: int) -> str:
    lines = [f'ℹ️ {BOT_DISPLAY_NAME} — версия {VERSION}', '', 'Команды:', '/ok — включить финансовый режим', '/start — окно сегодняшнего дня', '/prev — предыдущий день', '/next — следующий день', '/balance — баланс по этому чату', '/report — краткий отчёт по дням', '/csv — CSV этого чата', '/xlsx — Excel этого чата', '/tabl_lsx — таблица за последние 4 недели Чт–Ср', '/json — JSON этого чата', '/reset — обнулить данные чата (с подтверждением)', '/ping — проверка, жив ли бот', '/restore / /restore_off — восстановление GZ / JSON / ISON / CSV', '/dozvon — окно дозвона по связанным чатам', '/ost — слово «ост:» в Ф91 ВКЛ/ВЫКЛ']
    if is_owner_chat(chat_id):
        lines.extend(['/stopforward — отключить пересылку', '/backup_channel_on / _off — включить/выключить бэкап в канал', '/diag — диагностика бота', '/errors — последние ошибки', '/journal — скачать журнал действий бота', '/runtime_export — скачать Runtime/Watcher файлы из MEGA одним ZIP', '/articles — описание статей: статья = ключевые слова', '/mega_status — статус MEGA/MEGAcmd', '/mega_backup_now — безопасно загрузить latest_global.json в MEGA', '/mega_restore_now — принудительно полностью восстановить данные из MEGA', '/restore_guard — статус аварийной защиты восстановления', '/restore_guard_off — отключить guard и разрешить MEGA автобэкап', '/restore_guard_on — вернуть автоматическую защиту guard', '/buttons — переключить кнопки: text/icons', '/mask — переключить маскировку тотального секрета', '/day5 — финсутки: 00:00 / 05:00', '/off_on_backup_excel — Excel-бэкап всех чатов ВКЛ/ВЫКЛ', '/queues — состояние очередей и нагрузки'])
    lines.append('/help — эта справка')
    return '\n'.join(lines)
try:
    _v177_legacy_0053_build_help_text.__name__ = 'build_help_text'
except Exception:
    pass

def _v177_legacy_0054_build_info_text(chat_id: int) -> str:
    """Компактный INFO: одна функция показывается один раз, без дублей команд и кнопок."""
    layout = version_mode_layout()
    identity = f'🤖 {BOT_DISPLAY_NAME} | {version_animal_badge()} | {VERSION}'
    lines = [identity, 'ℹ️ INFO', '', f"Финансы: {('✅ ВКЛ' if is_finance_mode(chat_id) else '⬜ ВЫКЛ')}", f"Текущее окно: {('✅ ВКЛ' if chat_buttons_current_window_enabled(chat_id) else '⬜ ВЫКЛ')}", f"Журнал чата: {('✅ ВКЛ' if is_chat_journal_enabled(chat_id) else '⬜ ВЫКЛ')}"]
    if layout in {'v84', 'v85', 'v86', 'v87'}:
        lines.append(f"Финансы-кнопки: {('✅ ВКЛ' if main_financial_value_buttons_enabled(chat_id) else '⬜ ВЫКЛ')}")
    if _v85_enabled('gomonk_wallets'):
        lines.append(f"Гомонковые: {('✅ ВКЛ' if gomonk_enabled(chat_id) else '⬜ ВЫКЛ')}")
    if layout in {'v86', 'v87'}:
        lines.append(f"Валюта: {currency_mode(chat_id).upper().replace('_', '-')}")
        lines.append(f"Подпись «ост:»: {('✅ ВКЛ' if remaining_ost_label_enabled(chat_id) else '⬜ ВЫКЛ')}")
    if version_mode_feature('forward_copy_edit'):
        lines.append(f"💰Перес: {forward_copy_edit_mode(chat_id).replace('normal', 'обычно').replace('button', 'кнопка').replace('slash', 'слеш')}")
    if is_owner_chat(chat_id):
        lines.extend([f"Кнопки интерфейса: {('значки' if icon_button_mode_enabled(chat_id) else 'текст')}", f"Restore guard: {('✅ ВКЛ' if RESTORE_GUARD_ACTIVE else '⬜ ВЫКЛ')}", f"Guard override: {('✅ ВКЛ' if restore_guard_manual_override_enabled() else '⬜ ВЫКЛ')}", f"Автобэкап MEGA: {('РАЗРЕШЁН' if not RESTORE_GUARD_ACTIVE else 'ЗАБЛОКИРОВАН')}", f"Маска секрета: {('✅ ВКЛ' if total_secret_mask_enabled(chat_id) else '⬜ ВЫКЛ')}", f'Финансовые сутки: с {finance_day_start_label(chat_id)}', f"Диспетчер: pending {UPDATE_DISPATCHER.stats().get('pending', 0)}", f"Таймер ввода: {_format_duration_short(internal_timer_seconds('input_wait'))}; окна: {_format_duration_short(internal_timer_seconds('window_auto_return'))}", f'Excel: {excel_table_style_caption(chat_id)}'])
        if version_mode_feature('mega_priority'):
            lines.append(f"MEGA: {('приоритетный' if mega_backup_priority_enabled(chat_id) else 'обычный')} режим")
    lines.extend(['', 'Слеш-команды:'])
    commands = ['/ok — включить финансовый режим', '/start — открыть окно сегодняшнего дня', '/prev — предыдущий день', '/next — следующий день', '/balance — баланс по текущему чату', '/report — краткий отчёт', '/csv — CSV текущего чата', '/xlsx — Excel текущего чата', '/tabl_lsx — Excel-таблица по периоду Чт–Ср', '/json — JSON текущего чата', '/ost — включить/выключить подпись «ост:»', '/restore — включить восстановление GZ / JSON / ISON / CSV', '/restore_off — выключить режим восстановления', '/dozvon — открыть дозвон по связанным чатам', '/reset — обнулить данные чата с подтверждением', '/ping — проверить работу бота', '/help — полная справка']
    if is_owner_chat(chat_id):
        commands.extend(['/stopforward — полностью отключить пересылку', '/backup_channel_on — включить бэкап в канал', '/backup_channel_off — выключить бэкап в канал', '/diag — диагностика бота', '/errors — последние ошибки', '/journal — скачать журнал действий', '/articles — описание статей и ключевых слов', '/mega_status — статус MEGA', '/mega_backup_now — запустить безопасный бэкап MEGA', '/mega_restore_now — вручную полностью обновить данные из MEGA', '/restore_guard — статус защиты восстановления', '/buttons — переключить вид кнопок', '/mask — переключить маскировку тотального секрета', '/day5 — начало финансовых суток 00:00 / 05:00', '/off_on_backup_excel — Excel-бэкап всех чатов ВКЛ/ВЫКЛ', '/queues — состояние очередей и нагрузки'])
    seen_commands = set()
    for row in commands:
        cmd = row.split(' — ', 1)[0].strip().casefold()
        if cmd in seen_commands:
            continue
        seen_commands.add(cmd)
        lines.append(row)
    lines.extend(['', 'Нажмите нужную кнопку ниже. Полное описание — «📘 Инструкция».', '', identity])
    return '\n'.join(lines)
try:
    _v177_legacy_0054_build_info_text.__name__ = 'build_info_text'
except Exception:
    pass

def _v177_legacy_0059_get_connected_chat_ids(chat_id: int):
    connected = set()
    fr = data.get('forward_rules', {}) or {}
    src_key = str(chat_id)
    for dst in (fr.get(src_key, {}) or {}).keys():
        try:
            connected.add(int(dst))
        except Exception:
            pass
    for src, dsts in fr.items():
        if src_key in (dsts or {}):
            try:
                connected.add(int(src))
            except Exception:
                pass
    connected.discard(int(chat_id))
    return sorted(connected, key=lambda cid: get_chat_display_name(cid).lower())
try:
    _v177_legacy_0059_get_connected_chat_ids.__name__ = 'get_connected_chat_ids'
except Exception:
    pass

def build_dozvon_menu(chat_id: int):
    kb = types.InlineKeyboardMarkup()
    buttons = []
    for cid in get_connected_chat_ids(chat_id):
        buttons.append(IB(chat_button_title(cid, get_chat_display_name(cid)), callback_data=f'dzv:{cid}'))
    if buttons:
        add_buttons_in_rows(kb, buttons, 3)
    kb.row(IB('⬅️ Назад осн. окно', callback_data=f"d:{get_chat_store(chat_id).get('current_view_day', today_key())}:back_main"), IB('❌ Закрыть', callback_data='dzv:close'))
    return kb

def stop_dozvon_for_target(target_chat_id: int, reason: str='reply'):
    target_chat_id = int(target_chat_id)
    for session_key in list(_dozvon_target_index.get(target_chat_id, set())):
        sess = _dozvon_sessions.get(session_key)
        if sess:
            sess['stop'] = True
            sess['stop_reason'] = reason

def _cleanup_dozvon_session(session_key):
    sess = _dozvon_sessions.pop(session_key, None)
    if not sess:
        return None
    target_chat_id = int(sess['target_chat_id'])
    idx = _dozvon_target_index.get(target_chat_id)
    if idx and session_key in idx:
        idx.discard(session_key)
        if not idx:
            _dozvon_target_index.pop(target_chat_id, None)
    return sess

def _run_dozvon_session(session_key):
    sess = _dozvon_sessions.get(session_key)
    if not sess:
        return
    source_chat_id = int(sess['source_chat_id'])
    target_chat_id = int(sess['target_chat_id'])
    source_name = get_chat_display_name(source_chat_id)
    ping_text = f'📞 Дозвон от {source_name}'
    try:
        for phase in range(2):
            end_ts = time.time() + DOZVON_BURST_SECONDS
            while time.time() < end_ts:
                if sess.get('stop'):
                    break
                try:
                    sent = bot.send_message(target_chat_id, ping_text)
                    delete_message_later(target_chat_id, sent.message_id, 3)
                except Exception as e:
                    log_error(f'dozvon send to {target_chat_id}: {e}')
                    sess['stop'] = True
                    sess['stop_reason'] = 'send_error'
                    break
                time.sleep(DOZVON_INTERVAL_SECONDS)
            if sess.get('stop'):
                break
            if phase == 0:
                pause_until = time.time() + DOZVON_PAUSE_SECONDS
                while time.time() < pause_until:
                    if sess.get('stop'):
                        break
                    time.sleep(0.2)
                if sess.get('stop'):
                    break
    finally:
        sess = _cleanup_dozvon_session(session_key) or {}
        reason = sess.get('stop_reason')
        if reason == 'reply':
            send_and_auto_delete(source_chat_id, f'📞 Дозвон остановлен: {get_chat_display_name(target_chat_id)} ответил(а).', HELPER_DELETE_DELAY)
        elif reason == 'send_error':
            send_and_auto_delete(source_chat_id, f'⚠️ Дозвон остановлен: не удалось отправить сообщения в {get_chat_display_name(target_chat_id)}.', HELPER_DELETE_DELAY)
        else:
            send_and_auto_delete(source_chat_id, f'📞 Дозвон завершён: {get_chat_display_name(target_chat_id)}.', HELPER_DELETE_DELAY)

def start_dozvon(source_chat_id: int, target_chat_id: int):
    source_chat_id = int(source_chat_id)
    target_chat_id = int(target_chat_id)
    session_key = (source_chat_id, target_chat_id)
    existing = _dozvon_sessions.get(session_key)
    if existing:
        existing['stop'] = True
        existing['stop_reason'] = 'restart'
        time.sleep(0.1)
    sess = {'source_chat_id': source_chat_id, 'target_chat_id': target_chat_id, 'stop': False, 'stop_reason': None}
    _dozvon_sessions[session_key] = sess
    _dozvon_target_index[target_chat_id].add(session_key)
    send_and_auto_delete(source_chat_id, f'📞 Дозвон запущен: {get_chat_display_name(target_chat_id)}', HELPER_DELETE_DELAY)
    if not DOZVON_TASK_POOL.submit(f'{source_chat_id}:{target_chat_id}', _run_dozvon_session, session_key):
        send_and_auto_delete(source_chat_id, '⛔ Очередь дозвона переполнена.', 12)

def _direction_state_label(enabled: bool, left: str, arrow: str, right: str) -> str:
    icon = '✅' if enabled else '⬜'
    return f'{icon} {left} {arrow} {right}'

def _forward_arrow_icon(ab_on: bool, ba_on: bool) -> str:
    if ab_on and ba_on:
        return '🔄'
    if ab_on:
        return '⏩️'
    if ba_on:
        return '⏪️'
    return '⬜'

def _forward_fin_icon(ab_fin: bool, ba_fin: bool) -> str:
    if ab_fin and ba_fin:
        return '💰🔄'
    if ab_fin:
        return '💰▶️'
    if ba_fin:
        return '💰◀️'
    return '⬜'

def _v177_legacy_0060_build_forward_status_lines() -> list[str]:
    """Статус В22: короткая схема связей.
    Всегда показываем Чат A первым:
    Чат A -(⏩️/⏪️/🔄/⬜)-(💰▶️/💰◀️/💰🔄/⬜)-Чат B
    """
    lines = []
    fr = data.get('forward_rules', {}) or {}
    ff = data.get('forward_finance', {}) or {}
    seen_pairs = set()

    def _sorted_pair(a: int, b: int):
        name_a = get_chat_display_name(a).lower()
        name_b = get_chat_display_name(b).lower()
        if (name_a, a) <= (name_b, b):
            return (a, b)
        return (b, a)
    all_pairs = set()
    for src, dsts in fr.items():
        try:
            src_id = int(src)
        except Exception:
            continue
        for dst in (dsts or {}).keys():
            try:
                dst_id = int(dst)
            except Exception:
                continue
            all_pairs.add(_sorted_pair(src_id, dst_id))
    for a_id, b_id in sorted(all_pairs, key=lambda p: (get_chat_display_name(p[0]).lower(), get_chat_display_name(p[1]).lower())):
        pair_key = (a_id, b_id)
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)
        ab_on = str(b_id) in (fr.get(str(a_id), {}) or {})
        ba_on = str(a_id) in (fr.get(str(b_id), {}) or {})
        if not (ab_on or ba_on):
            continue
        ab_fin = bool((ff.get(str(a_id), {}) or {}).get(str(b_id), False))
        ba_fin = bool((ff.get(str(b_id), {}) or {}).get(str(a_id), False))
        name_a = chat_button_title(a_id)
        name_b = chat_button_title(b_id)
        lines.append(f'• {name_a} -({_forward_arrow_icon(ab_on, ba_on)})-({_forward_fin_icon(ab_fin, ba_fin)})-{name_b}')
    if not lines:
        lines.append('• Связи пересылки не настроены')
    return lines
try:
    _v177_legacy_0060_build_forward_status_lines.__name__ = 'build_forward_status_lines'
except Exception:
    pass

def build_forward_status_text(title: str | None=None) -> str:
    lines = []
    if title:
        lines.append(title)
        lines.append('')
    if title and 'Пересылка' in str(title):
        lines.append('Шаги: 1) выберите чат A → 2) выберите чат B → 3) включите 📨 пересылку и 💰 финучёт пересылки по нужным направлениям.')
        lines.append('')
    lines.append('Текущие связи:')
    lines.extend(build_forward_status_lines())
    return '\n'.join(lines)

def _find_forward_origin_by_copied_message(chat_id: int, msg_id: int):
    """
    Ищет origin (source_chat_id, source_msg_id) по копии сообщения в конкретном чате.
    Нужно для правильного reply, когда пользователь отвечает на сообщение,
    которое бот ранее переслал из другого чата.
    """
    try:
        for (src_chat_id, src_msg_id), pairs in forward_map.items():
            for pair_chat_id, pair_msg_id in pairs:
                if int(pair_chat_id) == int(chat_id) and int(pair_msg_id) == int(msg_id):
                    return (int(src_chat_id), int(src_msg_id))
    except Exception:
        pass
    return (None, None)

def resolve_reply_target_message_id(source_chat_id: int, reply_to_message_id: int | None, dst_chat_id: int):
    """
    Возвращает message_id, к которому нужно привязать reply в целевом чате.

    Поддерживает оба сценария:
    1) reply на исходное сообщение текущего чата
    2) reply на сообщение, которое бот переслал сюда из другого чата
    """
    if not reply_to_message_id:
        return None
    source_chat_id = int(source_chat_id)
    dst_chat_id = int(dst_chat_id)
    reply_to_message_id = int(reply_to_message_id)
    try:
        for link_dst_chat_id, link_dst_msg_id in get_forward_links(source_chat_id, reply_to_message_id):
            if int(link_dst_chat_id) == dst_chat_id:
                return int(link_dst_msg_id)
    except Exception:
        pass
    try:
        origin_chat_id, origin_msg_id = _find_forward_origin_by_copied_message(source_chat_id, reply_to_message_id)
        if origin_chat_id is not None and origin_msg_id is not None:
            if dst_chat_id == int(origin_chat_id):
                return int(origin_msg_id)
            for link_dst_chat_id, link_dst_msg_id in get_forward_links(origin_chat_id, origin_msg_id):
                if int(link_dst_chat_id) == dst_chat_id:
                    return int(link_dst_msg_id)
    except Exception:
        pass
    return None
_telegram_send_last_ts = {}
_telegram_send_rate_lock = threading.RLock()
_telegram_global_rate_lock = threading.RLock()
_telegram_global_last_ts = 0.0
try:
    TELEGRAM_GLOBAL_MIN_GAP = max(0.01, float(os.getenv('TELEGRAM_GLOBAL_MIN_GAP', '0.04') or '0.04'))
except Exception:
    TELEGRAM_GLOBAL_MIN_GAP = 0.04

def _telegram_retry_after_seconds(err: Exception):
    """Достаёт retry_after из Telegram 429: Too Many Requests."""
    try:
        result_json = getattr(err, 'result_json', None) or {}
        params = result_json.get('parameters') or {}
        if 'retry_after' in params:
            return int(params.get('retry_after') or 0)
    except Exception:
        pass
    text = str(err or '')
    m = re.search('retry after\\s+(\\d+)', text, re.I)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass
    return None

def is_telegram_429(err: Exception) -> bool:
    """True если Telegram ограничил частоту. Для UI такие ошибки нельзя держать sleep-ом."""
    try:
        return _telegram_retry_after_seconds(err) is not None
    except Exception:
        return 'too many requests' in str(err or '').lower()

def _is_fast_ui_purpose(purpose: str) -> bool:
    p = str(purpose or '').lower()
    fast_marks = ('safe_edit', 'commander', 'command_window', 'window_switch', 'ui_toggle', 'countdown', 'secret_window', 'secret media', 'secret_edit_debounce', 'category_wait_countdown', 'o9_secret_wait_countdown')
    return any((x in p for x in fast_marks))

def _telegram_rate_limit_chat(chat_id, min_gap: float=0.35):
    """Мягкий лимит отправки в один чат, чтобы реже получать 429 при шквале пересылок."""
    try:
        cid = int(chat_id)
    except Exception:
        return
    with _telegram_send_rate_lock:
        now_ts = time.time()
        prev_ts = float(_telegram_send_last_ts.get(cid, 0) or 0)
        wait = float(min_gap) - (now_ts - prev_ts)
        if wait > 0:
            time.sleep(wait)
        _telegram_send_last_ts[cid] = time.time()

def _telegram_rate_limit_global():
    """Общий лимитер Telegram API для всех чатов, чтобы не ловить шквал 429."""
    global _telegram_global_last_ts
    with _telegram_global_rate_lock:
        now_ts = time.time()
        wait = TELEGRAM_GLOBAL_MIN_GAP - (now_ts - _telegram_global_last_ts)
        if wait > 0:
            time.sleep(wait)
        _telegram_global_last_ts = time.time()

def _tg_first_chat_id(args, kwargs):
    if 'chat_id' in kwargs:
        return kwargs.get('chat_id')
    if args:
        return args[0]
    return None

def _tg_call_retry(func, *args, attempts: int=7, purpose: str='telegram', **kwargs):
    """
    Telegram API wrapper: если Telegram вернул 429, ждём retry_after и повторяем.
    Это нужно, чтобы пересылка не терялась, а доставлялась позже.
    """
    last_err = None
    for attempt in range(1, int(attempts) + 1):
        try:
            chat_id = _tg_first_chat_id(args, kwargs)
            _telegram_rate_limit_global()
            if chat_id is not None:
                ui_gap = effective_fast_telegram_gap() if _is_fast_ui_purpose(purpose) else 0.35
                _telegram_rate_limit_chat(chat_id, min_gap=ui_gap)
            try:
                if verbose_telegram_journal_enabled():
                    bot_journal('telegram_api_call', chat_id, f"{purpose}: {getattr(func, '__name__', str(func))} attempt={attempt}/{attempts}")
            except Exception:
                pass
            try:
                for _obj in list(args) + list(kwargs.values()):
                    if hasattr(_obj, 'seek'):
                        try:
                            _obj.seek(0)
                        except Exception:
                            pass
            except Exception:
                pass
            _res = func(*args, **kwargs)
            try:
                if chat_id is not None and is_chat_bot_removed(int(chat_id)):
                    set_chat_bot_removed(int(chat_id), False, 'telegram api success')
            except Exception:
                pass
            return _res
        except TypeError:
            raise
        except Exception as e:
            last_err = e
            retry_after = _telegram_retry_after_seconds(e)
            if retry_after is None:
                try:
                    chat_id_for_mark = _tg_first_chat_id(args, kwargs)
                    if chat_id_for_mark is not None and _is_bot_removed_error(e):
                        set_chat_bot_removed(int(chat_id_for_mark), True, str(e)[:240])
                except Exception:
                    pass
                raise
            wait = max(1, int(retry_after)) + 1
            log_info(f'[TG 429 RETRY] {purpose}: attempt={attempt}/{attempts}, wait={wait}s, error={str(e)[:220]}')
            try:
                bot_journal('telegram_429_retry', chat_id if 'chat_id' in locals() else None, f'{purpose}: attempt={attempt}/{attempts}, wait={wait}s, error={str(e)[:220]}', 'WARN')
            except Exception:
                pass
            if _is_fast_ui_purpose(purpose):
                raise e
            if attempt >= int(attempts):
                break
            time.sleep(wait)
    raise last_err

def _call_with_optional_reply(send_func, *args, reply_to_message_id=None, **kwargs):
    if reply_to_message_id:
        for extra in ({'reply_to_message_id': int(reply_to_message_id), 'allow_sending_without_reply': True}, {'reply_to_message_id': int(reply_to_message_id)}, {}):
            try:
                return _tg_call_retry(send_func, *args, purpose='send_with_reply', **kwargs, **extra)
            except TypeError:
                continue
    return _tg_call_retry(send_func, *args, purpose='send', **kwargs)

def build_balance_panel_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup()
    bal = get_chat_store(chat_id).get('balance', 0)
    kb.row(IB(f'🏦 Остаток: {format_chat_amount(chat_id, bal, True)}', callback_data='bp:open'))
    return kb

def _cancel_timer(timer_map: dict, key, scheduler_key: str | None=None):
    timer_map.pop(key, None)
    if scheduler_key:
        try:
            DELAYED_SCHEDULER.cancel(scheduler_key)
        except Exception:
            pass

def collapse_balance_panel(chat_id: int):
    store = get_chat_store(chat_id)
    panel_id = store.get('balance_panel_id')
    if not panel_id:
        return
    try:
        bot.edit_message_text('📌 Быстрый остаток', chat_id=chat_id, message_id=panel_id, reply_markup=build_balance_panel_keyboard(chat_id))
        store['balance_panel_mode'] = 'mini'
        save_data(data)
        _sync_finance_window_state_from_runtime(chat_id, schedule_delta=True)
    except Exception as e:
        err = str(e).lower()
        if 'message is not modified' not in err:
            log_error(f'collapse_balance_panel({chat_id}): {e}')

def schedule_balance_panel_collapse(chat_id: int, delay: float | None=None):
    if delay is None:
        delay = internal_timer_seconds('balance_collapse', BALANCE_PANEL_COLLAPSE_DELAY)

    def _job():
        try:
            collapse_balance_panel(chat_id)
        except Exception as e:
            log_error(f'schedule_balance_panel_collapse({chat_id}): {e}')
    store = get_chat_store(chat_id)
    key = store.get('balance_panel_id') or chat_id
    scheduler_key = f'balance-panel-collapse:{int(chat_id)}:{int(key)}'
    _cancel_timer(_balance_panel_collapse_timers, key, scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, delay, _job)
    _balance_panel_collapse_timers[key] = deadline

def send_minimized_balance_panel(chat_id: int):
    if finance_window_mode(chat_id) not in {'open', 'first'}:
        return
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return
    store = get_chat_store(chat_id)
    panel_id = store.get('balance_panel_id')
    if panel_id:
        try:
            bot.edit_message_text('📌 Быстрый остаток', chat_id=chat_id, message_id=panel_id, reply_markup=build_balance_panel_keyboard(chat_id))
            store['balance_panel_mode'] = 'mini'
            save_data(data)
            _sync_finance_window_state_from_runtime(chat_id, schedule_delta=True)
            return
        except Exception as e:
            err = str(e).lower()
            if 'message is not modified' in err:
                store['balance_panel_mode'] = 'mini'
                save_data(data)
                _sync_finance_window_state_from_runtime(chat_id, schedule_delta=True)
                return
            log_error(f'send_minimized_balance_panel edit({chat_id}): {e}')
            try:
                bot.delete_message(chat_id, panel_id)
            except Exception:
                pass
            store['balance_panel_id'] = None
    try:
        sent = bot.send_message(chat_id, '📌 Быстрый остаток', reply_markup=build_balance_panel_keyboard(chat_id))
        store['balance_panel_id'] = sent.message_id
        store['balance_panel_mode'] = 'mini'
        save_data(data)
        _finance_window_state(chat_id)['auto_reopen_on_boot'] = True
        _sync_finance_window_state_from_runtime(chat_id, schedule_delta=True)
    except Exception as e:
        log_error(f'send_minimized_balance_panel({chat_id}): {e}')

def _v177_legacy_0062_refresh_balance_panel_now(chat_id: int):
    if finance_window_mode(chat_id) not in {'open', 'first'}:
        return
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return
    store = get_chat_store(chat_id)
    panel_id = store.get('balance_panel_id')
    if not panel_id:
        send_minimized_balance_panel(chat_id)
        return
    mode = store.get('balance_panel_mode') or 'mini'
    try:
        if mode == 'open':
            day_key = store.get('current_view_day', today_key())
            txt, _ = render_day_window(chat_id, day_key)
            bot.edit_message_text(txt, chat_id=chat_id, message_id=panel_id, reply_markup=build_main_keyboard(day_key, chat_id), parse_mode='HTML')
            _set_panel_open_state(chat_id, panel_id)
        else:
            bot.edit_message_text('📌 Быстрый остаток', chat_id=chat_id, message_id=panel_id, reply_markup=build_balance_panel_keyboard(chat_id))
    except Exception as e:
        err = str(e).lower()
        if 'message is not modified' in err:
            if mode == 'open':
                schedule_balance_panel_collapse(chat_id)
            return
        if 'message to edit not found' in err or 'message_id_invalid' in err:
            try:
                bot_journal('balance_panel_stale_recreate_v238', chat_id, f'old_mid={panel_id}', 'WARN')
            except Exception:
                pass
        else:
            log_error(f'refresh_balance_panel_now({chat_id}): {e}')
        store['balance_panel_id'] = None
        store['balance_panel_mode'] = 'mini'
        save_data(data)
        _sync_finance_window_state_from_runtime(chat_id, schedule_delta=True)
        send_minimized_balance_panel(chat_id)
try:
    _v177_legacy_0062_refresh_balance_panel_now.__name__ = 'refresh_balance_panel_now'
except Exception:
    pass

def schedule_balance_panel_refresh(chat_id: int, delay: float | None=None):
    if delay is None:
        delay = internal_timer_seconds('main_window_refresh', BALANCE_PANEL_REFRESH_DELAY)
    if finance_window_mode(chat_id) not in {'open', 'first'}:
        return
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return

    def _job():
        try:
            store = get_chat_store(chat_id)
            if store.get('balance_panel_id'):
                refresh_balance_panel_now(chat_id)
            else:
                send_minimized_balance_panel(chat_id)
        except Exception as e:
            log_error(f'schedule_balance_panel_refresh({chat_id}): {e}')
    scheduler_key = f'balance-panel-refresh:{int(chat_id)}'
    _cancel_timer(_balance_panel_refresh_timers, chat_id, scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, delay, _job)
    _balance_panel_refresh_timers[chat_id] = deadline

def open_balance_panel_in_message(chat_id: int, message_id: int, day_key: str | None=None):
    if finance_window_mode(chat_id) not in {'open', 'first'}:
        return
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return
    store = get_chat_store(chat_id)
    day_key = day_key or store.get('current_view_day', today_key())
    store['current_view_day'] = day_key
    try:
        txt, _ = render_day_window(chat_id, day_key)
        bot.edit_message_text(txt, chat_id=chat_id, message_id=message_id, reply_markup=build_main_keyboard(day_key, chat_id), parse_mode='HTML')
        set_active_window_id(chat_id, day_key, message_id)
        _set_panel_open_state(chat_id, message_id)
    except Exception as e:
        err = str(e).lower()
        if 'message is not modified' in err:
            set_active_window_id(chat_id, day_key, message_id)
            _set_panel_open_state(chat_id, message_id)
            return
        log_error(f'open_balance_panel_in_message({chat_id},{message_id}): {e}')

def build_day_report_lines(chat_id: int) -> list[str]:
    store = get_chat_store(chat_id)
    daily = store.get('daily_records', {}) or {}
    mode = currency_mode(chat_id)
    if mode != 'ars':
        lines = ['Отчёт:']
        running_balance = 0.0
        for dk in sorted(daily.keys()):
            recs = daily.get(dk, []) or []
            expense = sum((abs(float(r.get('amount', 0) or 0)) for r in recs if float(r.get('amount', 0) or 0) < 0))
            income = sum((float(r.get('amount', 0) or 0) for r in recs if float(r.get('amount', 0) or 0) >= 0))
            running_balance += sum((float(r.get('amount', 0) or 0) for r in recs))
            lines.append(f'{fmt_date_ddmmyy(dk)} | приход {format_chat_amount(chat_id, income, True)} | расход {format_chat_amount(chat_id, -expense, True)} | ост {format_chat_amount(chat_id, running_balance, True)}')
        return lines
    lines = ['Отчёт:']
    lines.append(f"{'Дата':<8}|{report_header_cell('Приход', 7)}|{report_header_cell('Расход', 7)}|{report_header_cell('Остаток', 7)}")
    running_balance = 0.0
    for dk in sorted(daily.keys()):
        recs = daily.get(dk, []) or []
        expense = sum((abs(float(r.get('amount', 0) or 0)) for r in recs if float(r.get('amount', 0) or 0) < 0))
        income = sum((float(r.get('amount', 0) or 0) for r in recs if float(r.get('amount', 0) or 0) >= 0))
        running_balance += sum((float(r.get('amount', 0) or 0) for r in recs))
        lines.append(f'{fmt_date_ddmmyy(dk):<8}|{report_cell(income, 7)}|{report_cell(expense, 7)}|{report_cell(running_balance, 7)}')
    return lines

def week_start_monday(day_key: str) -> str:
    """Возвращает YYYY-MM-DD (понедельник недели) для day_key"""
    try:
        d = datetime.strptime(day_key, '%Y-%m-%d').date()
    except Exception:
        d = now_local().date()
    start = d - timedelta(days=d.weekday())
    return start.strftime('%Y-%m-%d')

def week_bounds_from_start(start_key: str):
    """start_key (YYYY-MM-DD, понедельник) -> (start_key, end_key)"""
    try:
        s = datetime.strptime(start_key, '%Y-%m-%d').date()
    except Exception:
        s = now_local().date() - timedelta(days=now_local().date().weekday())
    e = s + timedelta(days=6)
    return (s.strftime('%Y-%m-%d'), e.strftime('%Y-%m-%d'))

def week_start_thursday(day_key: str) -> str:
    """
    Возвращает YYYY-MM-DD (четверг недели ЧТ–СР) для day_key
    """
    try:
        d = datetime.strptime(day_key, '%Y-%m-%d').date()
    except Exception:
        d = now_local().date()
    offset = (d.weekday() - 3) % 7
    start = d - timedelta(days=offset)
    return start.strftime('%Y-%m-%d')

def week_bounds_thu_wed(start_key: str):
    """
    start_key (четверг) -> (четверг, среда)
    """
    try:
        s = datetime.strptime(start_key, '%Y-%m-%d').date()
    except Exception:
        s = now_local().date()
    e = s + timedelta(days=6)
    return (s.strftime('%Y-%m-%d'), e.strftime('%Y-%m-%d'))

def _load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        log_error(f'JSON load error {path}: {e}')
        return default

def _save_json(path: str, obj):
    """Атомарная запись: читатель никогда не видит половину JSON."""
    tmp_path = str(path) + f'.tmp.{threading.get_ident()}.{time.time_ns()}'
    try:
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp_path, path)
    except Exception as e:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        log_error(f'JSON save error {path}: {e}')

def journal_download_base_name_for(kind: str='full') -> str:
    key = 'current' if str(kind or '').casefold() == 'current' else 'full'
    try:
        gs = data.get('_global_settings', {}) or {}
        explicit = str(gs.get(f'journal_download_base_name_{key}_v229') or '')
        legacy = str(gs.get('journal_download_base_name') or '')
    except Exception:
        explicit = ''
        legacy = ''
    default = 'Журнал_текущей_версии' if key == 'current' else 'Журнал_бота'
    return _sanitize_journal_download_base(explicit or legacy or default)

def journal_download_base_name() -> str:
    """Backward-compatible label: full/common journal base name."""
    return journal_download_base_name_for('full')

def set_journal_download_base_name(value: str | None, kind: str='full') -> str:
    key = 'current' if str(kind or '').casefold() == 'current' else 'full'
    default = 'Журнал_текущей_версии' if key == 'current' else 'Журнал_бота'
    base = _sanitize_journal_download_base(value or default)
    gs = data.setdefault('_global_settings', {})
    gs[f'journal_download_base_name_{key}_v229'] = base
    if key == 'full':
        gs['journal_download_base_name'] = base
    try:
        save_data(data, full=True)
    except Exception:
        pass
    try:
        if mega_is_configured() and (not RESTORE_GUARD_ACTIVE):
            schedule_config_backup_for_chats(int(OWNER_ID or 0), delay=0.5)
    except Exception:
        pass
    return base

def journal_download_filename(kind: str='full') -> str:
    key = 'current' if str(kind or '').casefold() == 'current' else 'full'
    base = journal_download_base_name_for(key)
    stamp = now_local().strftime('%Y-%m-%d_%H-%M-%S')
    suffix = 'текущая_версия' if key == 'current' else 'полный'
    return f'{base}_{suffix}_{stamp}.txt'

def journal_filename_begin_wait(chat_id: int, seconds: float=120.0, kind: str='full') -> float:
    deadline = time.monotonic() + max(15.0, float(seconds))
    key = 'current' if str(kind or '').casefold() == 'current' else 'full'
    with _JOURNAL_FILENAME_WAIT_LOCK:
        _JOURNAL_FILENAME_WAIT[int(chat_id)] = {'deadline': deadline, 'kind': key}
    return deadline

def handle_journal_filename_input(msg) -> bool:
    try:
        chat_id = int(msg.chat.id)
        if not is_owner_chat(chat_id) or str(getattr(msg, 'content_type', '')) != 'text':
            return False
        with _JOURNAL_FILENAME_WAIT_LOCK:
            row = _JOURNAL_FILENAME_WAIT.get(chat_id)
            if not row:
                return False
            if isinstance(row, dict):
                deadline = float(row.get('deadline') or 0.0)
                kind = str(row.get('kind') or 'full')
            else:
                deadline = float(row or 0.0)
                kind = 'full'
            if time.monotonic() > deadline:
                _JOURNAL_FILENAME_WAIT.pop(chat_id, None)
                return False
            _JOURNAL_FILENAME_WAIT.pop(chat_id, None)
        raw = str(getattr(msg, 'text', '') or '').strip()
        reset = raw.upper() in {'СБРОС', 'RESET', 'АВТО', 'ПО УМОЛЧАНИЮ'}
        default = 'Журнал_текущей_версии' if kind == 'current' else 'Журнал_бота'
        base = set_journal_download_base_name(default if reset else raw, kind=kind)
        try:
            bot.delete_message(chat_id, int(msg.message_id))
        except Exception:
            pass
        label = 'текущей версии' if kind == 'current' else 'общего журнала'
        send_and_auto_delete(chat_id, f'✅ Имя {label}: {base}\nПример: {journal_download_filename(kind)}', 15)
        try:
            bot_journal('journal_download_name_changed_v229', chat_id, f'kind={kind}; base={base}')
        except Exception:
            pass
        return True
    except Exception as exc:
        try:
            log_error(f'journal filename input v229: {exc}')
        except Exception:
            pass
        return False
try:
    WINDOW_MARKER_CONSTANTS.setdefault('journal_name_edit:*', 'Ф252')
    WINDOW_MARKER_CONSTANTS.setdefault('journal_name_reset:*', 'Ф89')
except Exception:
    pass
# v262
