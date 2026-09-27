#!/usr/bin/env python3
"""очнись_13 R1 launcher.

R1 is authoritative and must start without R2.
Boot policy:
1) keep a valid local SQLite immediately;
2) if local SQLite is missing/invalid, do one short R2 health check and one authenticated restore attempt;
3) if R2 is absent, start with a normal local SQLite and continue standalone;
4) never invoke MEGAcmd/mega-* on R1.

The preboot HTTP spool remains so Telegram updates arriving while the large runtime imports
are not silently lost during a rolling deploy.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import runpy
import shutil
import sqlite3
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests
from runtime_config import install_internal_runtime_config, CONFIG_VERSION as INTERNAL_CONFIG_VERSION

install_internal_runtime_config('front')
os.environ['FAST_RUNTIME_MEGA_DISABLED'] = '1'
os.environ['MEGA_ENABLED'] = '0'
os.environ['MEGA_AUTORESTORE'] = '0'
os.environ['SPLIT_EMERGENCY_MEGA'] = '0'

_PREBOOT_LOCK = threading.RLock()
_PREBOOT_PATH = Path(os.getenv('PREBOOT_WEBHOOK_SPOOL_FILE','/tmp/och13/preboot_webhooks.ndjson') or '/tmp/och13/preboot_webhooks.ndjson')
_PREBOOT_MAX_BODY = 4 * 1024 * 1024
_PREBOOT_CAPTURED = 0


def _expected_route() -> str:
    token = str(os.getenv('B_T','') or '').strip()
    host = str(os.getenv('RENDER_EXTERNAL_HOSTNAME','') or '').strip()
    render_url = f'https://{host}' if host else str(os.getenv('RENDER_EXTERNAL_URL','') or '').strip()
    app_url = render_url or str(os.getenv('WEBHOOK_URL','') or os.getenv('APP_URL','') or '').strip()
    seed = str(os.getenv('WEBHOOK_SECRET','') or '').strip()
    if not seed:
        authority = str(os.getenv('RENDER_SERVICE_ID','') or app_url)
        seed = hashlib.sha256(('telegram-webhook-v163|' + token + '|' + authority).encode('utf-8')).hexdigest()[:40]
    return '/tg/' + seed


def _capture(raw: bytes, path: str):
    global _PREBOOT_CAPTURED
    if str(path or '').split('?',1)[0] != _expected_route():
        return False, 'route_mismatch', 0
    if not raw or len(raw) > _PREBOOT_MAX_BODY:
        return False, 'body_size', 0
    try:
        payload = json.loads(raw.decode('utf-8'))
        update_id = int(payload.get('update_id'))
    except Exception:
        return False, 'invalid_update', 0
    if not isinstance(payload, dict):
        return False, 'invalid_payload', update_id
    known=('callback_query','message','edited_message','channel_post','edited_channel_post','deleted_business_messages')
    kind=next((k for k in known if k in payload),'other')
    if kind == 'other':
        return False, 'unsupported_update', update_id
    row=json.dumps({'captured_at':time.time(),'update_id':update_id,'type':kind,'payload':payload},ensure_ascii=False,separators=(',',':'))+'\n'
    with _PREBOOT_LOCK:
        _PREBOOT_PATH.parent.mkdir(parents=True,exist_ok=True)
        with open(_PREBOOT_PATH,'a',encoding='utf-8') as fh:
            fh.write(row); fh.flush()
            try: os.fsync(fh.fileno())
            except Exception: pass
        _PREBOOT_CAPTURED += 1
    return True, kind, update_id


class _BootServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


class _BootHandler(BaseHTTPRequestHandler):
    def _reply(self,status:int,body=True,extra=None):
        payload={'ok':status<400,'role':'front','release':'очнись_13','phase':'boot','telegram_spooled':_PREBOOT_CAPTURED}
        if extra: payload.update(extra)
        raw=json.dumps(payload,ensure_ascii=False,separators=(',',':')).encode('utf-8') if body else b''
        self.send_response(status); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(raw))); self.end_headers()
        if raw: self.wfile.write(raw)
    def do_GET(self): self._reply(200,True)
    def do_HEAD(self): self._reply(200,False)
    def do_POST(self):
        try: length=int(self.headers.get('Content-Length','0') or '0')
        except Exception: length=0
        if length<=0 or length>_PREBOOT_MAX_BODY:
            self._reply(503,True,{'captured':False,'reason':'body_size'}); return
        ok,detail,uid=_capture(self.rfile.read(length),self.path)
        # 503 deliberately keeps Telegram retry semantics until real Waitress is bound.
        self._reply(503,True,{'captured':bool(ok),'reason':detail,'update_id':uid})
    def log_message(self,fmt,*args): return


def _start_boot_server():
    port=int(os.getenv('PORT','5000') or '5000')
    srv=_BootServer(('0.0.0.0',port),_BootHandler)
    threading.Thread(target=srv.serve_forever,name='och13-preboot-http',daemon=True).start()
    print(f'[OCH13 R1] preboot port 0.0.0.0:{port}',flush=True)
    return srv


def _stop_boot_server(srv):
    try: srv.shutdown()
    except Exception: pass
    try: srv.server_close()
    except Exception: pass


def _db_path() -> Path:
    return Path(os.getenv('DB_FILE','bot_state.sqlite3') or 'bot_state.sqlite3').resolve()


def _db_valid(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 4096: return False
    try:
        con=sqlite3.connect(str(path),timeout=10)
        try:
            row=con.execute('PRAGMA quick_check').fetchone()
            return bool(row and str(row[0]).lower()=='ok')
        finally: con.close()
    except Exception: return False


def _db_revision(path: Path) -> float:
    if not _db_valid(path): return 0.0
    try:
        con=sqlite3.connect(str(path),timeout=10)
        try:
            rev=0.0
            for kind in ('split_state_revision_r18','user_state_shadow_v265','runtime_continuity_v263'):
                try:
                    row=con.execute("SELECT v FROM meta WHERE kind=? AND k='latest'",(kind,)).fetchone()
                    if row:
                        rev=max(rev,float((json.loads(row[0]) or {}).get('saved_at') or 0.0))
                except Exception: pass
            return rev
        finally: con.close()
    except Exception: return 0.0


def _ensure_empty_db(path: Path):
    path.parent.mkdir(parents=True,exist_ok=True)
    for suffix in ('-wal','-shm'):
        try: Path(str(path)+suffix).unlink(missing_ok=True)
        except Exception: pass
    con=sqlite3.connect(str(path),timeout=20)
    try:
        con.execute('PRAGMA journal_mode=WAL'); con.execute('PRAGMA synchronous=FULL')
        con.execute('CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)')
        con.execute('CREATE TABLE IF NOT EXISTS chats (chat_id TEXT PRIMARY KEY, v TEXT NOT NULL)')
        con.execute('CREATE TABLE IF NOT EXISTS meta (kind TEXT NOT NULL, k TEXT NOT NULL, v TEXT NOT NULL, PRIMARY KEY(kind,k))')
        con.execute("CREATE TABLE IF NOT EXISTS cold_fields (chat_id TEXT NOT NULL, k TEXT NOT NULL, v TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT '', PRIMARY KEY(chat_id,k))")
        con.execute('CREATE TABLE IF NOT EXISTS r32_state_revisions (shard_key TEXT PRIMARY KEY, revision INTEGER NOT NULL, event_id TEXT NOT NULL, updated_at REAL NOT NULL)')
        con.commit()
    finally: con.close()
    return _db_valid(path)


def _peer_base() -> str:
    raw=str(os.getenv('PEER_PRIVATE_URL','') or os.getenv('PEER_SERVICE_URL','') or '').strip().rstrip('/')
    if raw and not raw.startswith(('http://','https://')):
        raw=('http://' if raw.endswith('.internal') or '.internal:' in raw else 'https://')+raw
    return raw


def _r2_health(base: str):
    if not base: return False,'R2 URL not configured'
    try:
        r=requests.get(base+'/peer/health',timeout=(0.55,1.15),headers={'User-Agent':'och13-r1-boot-health'})
        return (200<=r.status_code<300),f'HTTP {r.status_code}'
    except Exception as exc:
        return False,f'{type(exc).__name__}: {str(exc)[:180]}'


def _restore_from_r2(target: Path):
    base=_peer_base(); secret=str(os.getenv('PEER_SHARED_SECRET','') or '').strip()
    if not base or not secret: return False,'R2 URL/secret not configured'
    ok,detail=_r2_health(base)
    if not ok: return False,'health '+detail
    work=Path(tempfile.mkdtemp(prefix='och13_r2_restore_'))
    gz=work/'state.sqlite3.gz'; raw=work/'state.sqlite3'
    try:
        with requests.get(base+'/internal/restore/latest',headers={'X-Peer-Secret':secret,'User-Agent':'och13-r1-boot-restore'},timeout=(0.8,18),stream=True) as r:
            if r.status_code != 200:
                return False,f'restore HTTP {r.status_code}: {(r.text or "")[:180]}'
            total=0; max_bytes=96*1024*1024
            with open(gz,'wb') as fh:
                for chunk in r.iter_content(1024*1024):
                    if not chunk: continue
                    total += len(chunk)
                    if total > max_bytes: return False,'restore payload too large'
                    fh.write(chunk)
        with gzip.open(gz,'rb') as src, open(raw,'wb') as dst:
            shutil.copyfileobj(src,dst,1024*1024)
        if not _db_valid(raw): return False,'R2 snapshot quick_check failed'
        target.parent.mkdir(parents=True,exist_ok=True)
        tmp=target.with_suffix(target.suffix+'.och13.tmp')
        shutil.copy2(raw,tmp); os.replace(tmp,target)
        for suffix in ('-wal','-shm'):
            try: Path(str(target)+suffix).unlink(missing_ok=True)
            except Exception: pass
        return _db_valid(target),f'R2 restore OK bytes={gz.stat().st_size}'
    except Exception as exc:
        return False,f'{type(exc).__name__}: {str(exc)[:260]}'
    finally:
        shutil.rmtree(work,ignore_errors=True)


def main():
    server=_start_boot_server(); started=time.time(); target=_db_path()
    trace={'schema':13,'release':'очнись_13','internal_config':INTERNAL_CONFIG_VERSION,'started_at':started,'policy':'R1_LOCAL_SQLITE_THEN_OPTIONAL_R2','mega_contacted':False}
    try:
        local_ok=_db_valid(target); trace['local_valid_before']=local_ok
        if local_ok:
            mode='standalone-local'; detail='valid local SQLite kept'
        else:
            restored,detail=_restore_from_r2(target); trace['r2_restore_ok']=restored; trace['r2_restore_detail']=detail
            if restored:
                mode='distributed-restore'
            else:
                if not _ensure_empty_db(target): raise RuntimeError('cannot initialize local SQLite')
                mode='standalone-empty'
                os.environ['OCH1227_EMPTY_BOOT']='1'; os.environ['OCH1227_EMPTY_BOOT_REASON']=detail[:1000]
        revision=_db_revision(target)
        trace.update(mode=mode,final_revision=revision,local_valid_after=_db_valid(target),elapsed_ms=round((time.time()-started)*1000,1))
        os.environ['SPLIT_PREBOOT_AUTHORITATIVE_R20']='1'; os.environ['SPLIT_PREBOOT_REVISION_R20']=str(revision)
        os.environ['R13_BOOT_MODE']=mode; os.environ['R13_BOOT_TRACE_JSON']=json.dumps(trace,ensure_ascii=False,separators=(',',':'))
        os.environ['R68_RESTORE_TRACE_JSON']=os.environ['R13_BOOT_TRACE_JSON']; os.environ['R49_RESTORE_TRACE_JSON']=os.environ['R13_BOOT_TRACE_JSON']
        os.environ['PREBOOT_WEBHOOK_SPOOL_FILE']=str(_PREBOOT_PATH); os.environ['BOT_DEFER_MAIN_R54']='1'
        print('[OCH13 R1 BOOT]',os.environ['R13_BOOT_TRACE_JSON'],flush=True)
        runtime_ns=runpy.run_path(str(Path(__file__).with_name('bot.py')),run_name='__main__')
        os.environ.pop('BOT_DEFER_MAIN_R54',None)
        runtime_main=runtime_ns.get('main')
        if not callable(runtime_main): raise RuntimeError('bot runtime loaded without callable main()')
        print(f'[OCH13 R1] runtime ready; boot_mode={mode}; captured={_PREBOOT_CAPTURED}',flush=True)
        _stop_boot_server(server); time.sleep(0.05); runtime_main()
    finally:
        _stop_boot_server(server)


if __name__=='__main__':
    main()
