#!/usr/bin/env python3
from __future__ import annotations
import io, os, py_compile, re, subprocess, sys, tokenize
from pathlib import Path
ROOT=Path(__file__).resolve().parent
STARTUP_SMOKE=str(os.getenv('FINALIZATION_STARTUP_SMOKE','0') or '0').strip().lower() in {'1','true','yes','on'}
RUNTIME_BUILD=str(os.getenv('FINALIZATION_RUNTIME_BUILD','0') or '0').strip().lower() in {'1','true','yes','on'}
checks=[]
def check(name,cond,detail=''):
    checks.append((name,bool(cond),str(detail or '')))
def read(name):
    p=ROOT/name
    return p.read_text(encoding='utf-8',errors='replace') if p.is_file() else ''
required=['bot.py','runtime_flat.py','start_front.py','runtime_config.py','requirements.txt','FINALIZATION_GATE.py']
check('runtime_files',all((ROOT/x).is_file() for x in required))
for rel in ['bot.py','runtime_flat.py','start_front.py','runtime_config.py','FINALIZATION_GATE.py']:
    try: py_compile.compile(str(ROOT/rel),doraise=True); check('compile_'+rel,True)
    except Exception as exc: check('compile_'+rel,False,exc)
bot=read('bot.py'); runtime=read('runtime_flat.py'); start=read('start_front.py'); cfg=read('runtime_config.py'); docker=read('Dockerfile')
check('release_name',"BOT_DISPLAY_NAME = 'очнись_13.7'" in runtime and "OCHNIS_RELEASE = 'очнись_13.7'" in bot)
check('thin_bot_entry','import runtime_flat as _runtime' in bot and 'main = _runtime.main' in bot)
check('flat_runtime_source',runtime.startswith('# OCHNIS_13 FLAT RUNTIME'))
check('no_owner_install_runtime','_owner_install(' not in runtime)
check('no_exec_compile_runtime','exec(compile(' not in runtime)
for symbol in ['handle_finance_message','get_forward_links','_v229_command_tasks','_reminder_send_cycle','handle_secret_note_message','tenant_google_config','_write_simple_xlsx','_mega_run']:
    check('domain_'+symbol,(f'def {symbol}(' in runtime) or (f'{symbol} =' in runtime))
# R1 independent / R2 optional
check('r2_boot_probe','def _och13_probe_r2()' in start and "'/peer/health'" in start)
check('r1_primary','OCH13_R1_PRIMARY_R2_OPTIONAL' in start and 'EMPTY R1' in start)
check('r2_optional_runtime',"'STANDALONE_R1'" in runtime and "'DISTRIBUTED_R1_PLUS_R2'" in runtime)
# Fast lexical ownership checks. Avoid parsing the full 5 MB runtime AST during Docker build.
straight=['add_record_to_chat','handle_finance_edit','update_record_in_chat','normalize_chat_records','_v258_record_strong_keys','_v260_bind_forward_finance_record','get_forward_links','_forward_single_to_target','schedule_forward_any_message','forward_any_message','resolve_forward_targets','add_forward_link','remove_forward_link','set_forward_finance','remove_forward_finance','_persist_forward_finance_delivery_now']
def _top_defs(src,name):
    return list(re.finditer(r'(?m)^def\s+'+re.escape(name)+r'\s*\(',src))
def _func_segment(src,m):
    lines=src[m.start():].splitlines(keepends=True)
    out=[]
    for i,line in enumerate(lines):
        if i>0 and line.strip() and not line.startswith((' ','\t')):
            break
        out.append(line)
    return ''.join(out)
owners={x:_top_defs(runtime,x) for x in straight}
check('straight_single_owner',all(len(owners[x])==1 for x in straight),';'.join(f'{x}={len(owners[x])}' for x in straight if len(owners[x])!=1))
bad=[]
for x in straight:
    for m in owners[x]:
        seg=_func_segment(runtime,m)
        try:
            names=[tok.string for tok in tokenize.generate_tokens(io.StringIO(seg).readline) if tok.type==tokenize.NAME]
        except Exception:
            names=[]
        for marker in ('_V152_ORIG','_V215_PREV','_V217_PREV','_V262_BASE','_OCH129_PARENT','_canon_','_v177_legacy'):
            if any(marker in name for name in names): bad.append(x+':'+marker)
check('straight_no_wrapper_chain',not bad,';'.join(bad))
# 13.5 retained safety
check('redis_remote_replay_hard_disabled',"event_replay_policy'] = 'metadata-dedupe-only'" in runtime and 'def split_recover_remote_events_v268' in runtime)
check('redis_no_raw_telegram_payload',"'executable_payload': False" in runtime)
check('mega_controlplane_inherits_lease',"enabled_for_call = True if recovery else bool(MEGA_ENABLED)" in runtime)
check('mega_existing_session_ok','mega-login reported already logged in' in runtime)
check('finance_mega_degraded_not_quarantine','constitution_ledger_durability_degraded_och135' in runtime)
check('forward_immutable_edit_replace_fallback','forward_edit_replace_fallback_och135' in runtime)
# 13.6 manual restore gates
check('restore_slash_modes',"arg in {'mega', 'folder', 'folders', 'папка', 'папки'}" in runtime and "arg in {'latest', 'current', 'последняя', 'последний'}" in runtime)
check('restore_raw_sqlite_upload','def v182_prepare_sqlite_restore_document' in runtime and "('.sqlite3', '.sqlite', '.db')" in runtime)
check('restore_telegram_prebackup','def _och136_store_manual_restore_telegram' in runtime and "durable:manual_pre_restore" in runtime)
check('restore_redis_not_authority','Redis is cache-only and is never counted as a recovery anchor' in runtime and 'Redis restore disabled by OCH13.6 policy' in runtime)
check('restore_postseal_telegram_mega','def r64_publish_restore_snapshot_v271' in runtime and "'telegram_ok':bool(tg_ok)" in runtime and "'redis_ok':False" in runtime)
check('restore_browser_r1_fallback','fallback_from_r2' in runtime and '_r71_local_mega_list(path)' in runtime)
check('restore_file_r1_fallback','def _v265_heavy_download_mega_file(remote, workdir):' in runtime and "runner('mega-get'" in runtime)
check('restore_exact_controlplane','def _r81_mega_get_exact' in runtime and "control_plane=True" in runtime[runtime.find('def _r81_mega_get_exact'):runtime.find('def _r221_manual_mega_ready')])
check('restore_browser_single_public_owner',runtime.count('def _v265_peer_mega_request(path):')==1 and runtime.count('def _v265_heavy_download_mega_file(remote, workdir):')==1)
check('restore_alias_commands',"commands=['restore_mega', 'restore_folder']" in runtime and "commands=['restore_latest']" in runtime)
check('restore_prebackup_no_redis_call','_split_cache_snapshot_to_redis_v266' not in runtime[runtime.find('def _v153_backup_before_restore'):runtime.find('def _v153_apply_global_restore')])
check('restore_selected_controlplane',"mega_is_configured(control_plane=True)" in runtime[runtime.find('def _v242_restore_selected_mega_database'):runtime.find('# ===== SOURCE 08_reliability_tasks.py')])

# 13.7 callback-core regression fence
r71_assign = runtime.find('_R71_CONTOUR_CORE = contour_callback_guard')
r71_def = runtime.find('def _r71_contour_callback_guard(call, resolved):')
check('r71_contour_core_bound', r71_assign >= 0 and r71_def >= 0 and r71_assign < r71_def)
chain_names=set(re.findall(r'\b(_R\d+_[A-Z0-9_]*(?:CORE|PREV|ORIG|BASE|PARENT)[A-Z0-9_]*)\b',runtime))
missing_chain=[n for n in sorted(chain_names) if not re.search(r'(?m)^\s*'+re.escape(n)+r'\s*=',runtime)]
check('release_chain_globals_bound', not missing_chain, str(missing_chain))

# packaging
if not RUNTIME_BUILD:
    check('docker_present',(ROOT/'Dockerfile').is_file())
    check('docker_flat_only','runtime_flat.py' in docker and 'owners_manifest.json' not in docker)
    check('source_archive_not_in_production',not (ROOT/'SOURCE_12_36.zip').is_file())
if STARTUP_SMOKE:
    env=dict(os.environ)
    env.update({'BOT_DEFER_MAIN_R54':'1','B_T':env.get('B_T') or '123456:STARTUPSMOKE','DB_FILE':'/tmp/och136_gate.sqlite3','MEGA_ENABLED':'0','REDIS_ENABLED':'0','TELEGRAM_BACKUP_ENABLED':'0','REDIS_URL':'','PEER_PRIVATE_URL':'','PEER_SERVICE_URL':'','PEER_SHARED_SECRET':'','MEGA_EMAIL':'','MEGA_PASSWORD':'','TRAFFIC_AUDIT_ENABLED':'0'})
    code="import os,sys,bot; assert callable(bot.main); assert bot.BOT_DISPLAY_NAME=='очнись_13.7'; assert (getattr(bot,'_SPLIT_STATE',{}) or {}).get('och13_mode')=='STANDALONE_R1'; print('OCH137_STARTUP_IMPORT_OK'); sys.stdout.flush(); os._exit(0)"
    try:
        cp=subprocess.run([sys.executable,'-c',code],cwd=str(ROOT),env=env,text=True,capture_output=True,timeout=120)
        check('startup_without_r2',cp.returncode==0 and 'OCH137_STARTUP_IMPORT_OK' in cp.stdout,(cp.stdout+'\n'+cp.stderr)[-1800:])
    except Exception as exc: check('startup_without_r2',False,exc)
passed=sum(1 for _,ok,_ in checks if ok); total=len(checks)
for name,ok,detail in checks:
    if not ok: print('FAIL',name,detail)
print(f'FINALIZATION OCHNIS 13.7: {passed}/{total} PASS')
sys.stdout.flush(); raise SystemExit(0 if passed==total else 1)
