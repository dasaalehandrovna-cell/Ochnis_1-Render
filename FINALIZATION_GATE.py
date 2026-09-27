#!/usr/bin/env python3
from __future__ import annotations

import ast
import os
import py_compile
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RUNTIME_BUILD = str(os.getenv('FINALIZATION_RUNTIME_BUILD','0') or '0').strip().lower() in {'1','true','yes','on'}
STARTUP_SMOKE = str(os.getenv('FINALIZATION_STARTUP_SMOKE','0') or '0').strip().lower() in {'1','true','yes','on'}
checks=[]

def check(name, cond, detail=''):
    checks.append((name, bool(cond), str(detail or '')))
    if not cond:
        print('FAIL', name, detail)

def read(name):
    p=ROOT/name
    return p.read_text(encoding='utf-8',errors='replace') if p.is_file() else ''

required=['bot.py','runtime_flat.py','start_front.py','runtime_config.py','requirements.txt','FINALIZATION_GATE.py']
check('runtime_files', all((ROOT/x).is_file() for x in required), ','.join(x for x in required if not (ROOT/x).is_file()))

for rel in ['bot.py','runtime_flat.py','start_front.py','runtime_config.py','FINALIZATION_GATE.py']:
    try:
        py_compile.compile(str(ROOT/rel), doraise=True)
        ast.parse(read(rel), filename=rel)
        good=True; detail=''
    except Exception as exc:
        good=False; detail=str(exc)
    check('compile_'+rel, good, detail)

bot=read('bot.py'); runtime=read('runtime_flat.py'); start=read('start_front.py'); cfg=read('runtime_config.py'); docker=read('Dockerfile')

# OCHNIS 13 removes runtime source reconstruction completely.
check('release_name', "BOT_DISPLAY_NAME = 'очнись_13.4'" in runtime and "OCHNIS_RELEASE = 'очнись_13.4'" in bot)
check('no_owner_install_runtime', '_owner_install(' not in runtime and '_owner_install(' not in bot)
check('no_exec_compile_runtime', 'exec(compile(' not in runtime and 'exec(compile(' not in bot)
check('thin_bot_entry', 'import runtime_flat as _runtime' in bot and 'main = _runtime.main' in bot)
check('flat_runtime_source', runtime.startswith('# OCHNIS_13 FLAT RUNTIME'))

# Core user-visible domains must still be present in the flattened runtime.
for symbol in ['handle_finance_message','get_forward_links','_v229_command_tasks','_reminder_send_cycle','handle_secret_note_message','tenant_google_config','_write_simple_xlsx','_mega_run']:
    present = (('def '+symbol+'(' in runtime) or ('async def '+symbol+'(' in runtime) or (symbol+' =' in runtime))
    check('domain_'+symbol, present, symbol)

# R1/R2 contract: startup detection, R2 restore, standalone R1, dynamic accelerator fallback.
check('r2_boot_probe', 'def _och13_probe_r2()' in start and "'/peer/health'" in start)
check('r2_boot_restore', 'def _och13_restore_from_r2' in start and "'/internal/restore/latest'" in start and 'R2_VALIDATED_SNAPSHOT' in start)
check('r1_never_blocked_by_r2', "trace['policy'] = 'OCH13_R1_PRIMARY_R2_OPTIONAL'" in start and 'EMPTY R1' in start)
check('runtime_flat_launcher', "with_name('runtime_flat.py')" in start)
check('auto_accel', 'def _och13_auto_accel_routes()' in runtime and 'def _och13_boot_probe()' in runtime)
check('standalone_mode', "'STANDALONE_R1'" in runtime)
check('distributed_mode', "'DISTRIBUTED_R1_PLUS_R2'" in runtime)
check('fallback_wrapper', 'def _r1234_note_fallback' in runtime and 'def submit_interactive_file_job' in runtime)

# OCHNIS 13.4 STRAIGHT gates: critical hot paths have one owner and no historical wrapper chain.
_runtime_tree = ast.parse(runtime, filename='runtime_flat.py')
_straight_names = [
    'add_record_to_chat','handle_finance_edit','update_record_in_chat','normalize_chat_records','_v258_record_strong_keys',
    '_v260_bind_forward_finance_record','get_forward_links','_forward_single_to_target','schedule_forward_any_message',
    'forward_any_message','resolve_forward_targets','add_forward_link','remove_forward_link','set_forward_finance',
    'remove_forward_finance','_persist_forward_finance_delivery_now'
]
_straight_defs = {}
for _node in _runtime_tree.body:
    if isinstance(_node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _node.name in _straight_names:
        _straight_defs.setdefault(_node.name, []).append(_node)
check('straight_single_owner', all(len(_straight_defs.get(_name, [])) == 1 for _name in _straight_names),
      '; '.join(f'{_name}={len(_straight_defs.get(_name, []))}' for _name in _straight_names if len(_straight_defs.get(_name, [])) != 1))
_forbidden = ('_V152_ORIG','_V215_PREV','_V217_PREV','_V262_BASE','_OCH129_PARENT','_canon_','_v177_legacy')
_bad_straight=[]
for _name in _straight_names:
    for _node in _straight_defs.get(_name, []):
        _seg = ast.get_source_segment(runtime, _node) or ''
        _hits=[_x for _x in _forbidden if _x in _seg]
        if _hits: _bad_straight.append(f'{_name}:{",".join(_hits)}')
check('straight_no_wrapper_chain', not _bad_straight, '; '.join(_bad_straight))
_edit_nodes=_straight_defs.get('handle_finance_edit',[])
_edit_src=ast.get_source_segment(runtime,_edit_nodes[0]) if _edit_nodes else ''
check('straight_edit_no_source_order_fallback', "source_order_msg_id" not in (_edit_src or ''), 'source_order_msg_id still in handle_finance_edit')
_critical_assign=[]
for _node in _runtime_tree.body:
    if isinstance(_node, ast.Assign):
        for _target in _node.targets:
            if isinstance(_target, ast.Name) and _target.id in _straight_names:
                _critical_assign.append((_target.id, _node.lineno))
check('straight_no_public_reassign', not _critical_assign, str(_critical_assign))

# OCHNIS 13.3 regression gates: restored/other-bot message ids cannot suppress new finance.
check('finance_bot_scoped_key', 'finance2:' in runtime and 'telegram_bot_id' in runtime)
check('finance_local_msg_namespace', "for key in ('forward_dst_msg_id', 'source_msg_id', 'origin_msg_id', 'msg_id')" in runtime and "'source_order_msg_id'):" not in runtime[runtime.find('def _record_message_ids_v257'):runtime.find('def _remember_finance_source_identity_v257')])
check('finance_collision_diagnostic', 'finance_message_id_collision_och132' in runtime and 'finance_message_id_collision_passed_och132' in runtime)
check('finance_forward_source_first', 'def _v260_find_forward_finance_record' in runtime and 'finance_forward_message_id_collision_och132' in runtime and 'fwd-fin2:' in runtime)
check('finance_forward_op_ledger_bot_scoped', "return f\"{int(_current_bot_id_for_forwarding() or 0)}:{int(source_chat_id)}:{int(source_msg_id)}:{int(dst_chat_id)}\"" in runtime)
check('probe_network_no_state_mutation', "_probe_network_only = str(purpose or '').startswith('probe_')" in runtime)

ns={}
try:
    exec(compile(cfg,'runtime_config.py','exec'),ns,ns)
    front=ns.get('FRONT_INTERNAL_ENV') or {}
    cfg_ok=True
except Exception as exc:
    front={}; cfg_ok=False
    check('runtime_config_exec',False,exc)
if cfg_ok:
    check('runtime_config_exec',True)
    for key in ['OCH13_R2_AUTO_ACCEL','OCH13_R2_BOOT_PROBE','OCH13_R2_BOOT_RESTORE']:
        check('cfg_'+key, str(front.get(key))=='1', front.get(key))
    check('peer_watch_enabled', str(front.get('PEER_PING_ENABLED'))=='1', front.get('PEER_PING_ENABLED'))
    budgets={'UI_WORKERS':2,'FAST_UI_WORKERS':2,'WINDOW_RENDER_WORKERS':2,'UI_CLEANUP_WORKERS':1,'UI_DELETE_WORKERS':1,'BACKGROUND_WORKERS':1,'SCHEDULER_WORKERS':1,'R21_HEAVY_DISPATCH_WORKERS':1}
    bad=[]
    for k,lim in budgets.items():
        try:
            if int(front.get(k,999))>lim: bad.append(f'{k}={front.get(k)}>{lim}')
        except Exception: bad.append(f'{k}=invalid')
    check('memory_worker_budget',not bad,'; '.join(bad))

if not RUNTIME_BUILD:
    check('docker_present',(ROOT/'Dockerfile').is_file())
    check('docker_flat_only','runtime_flat.py' in docker and 'owners_manifest.json' not in docker and '01_core_data.py' not in docker)
    old=[p.name for p in ROOT.glob('[0-1][0-9]_*.py')]
    check('no_legacy_runtime_parts',not old,','.join(sorted(old)))
    check('source_archive_not_in_production',not (ROOT/'SOURCE_12_36.zip').is_file(),'SOURCE_12_36.zip must stay outside production FAST')

if STARTUP_SMOKE:
    env=dict(os.environ)
    env.update({
        'BOT_DEFER_MAIN_R54':'1','B_T':env.get('B_T') or '123456:STARTUPSMOKE',
        'DB_FILE':env.get('DB_FILE') or '/tmp/och13_gate.sqlite3',
        'MEGA_ENABLED':'0','REDIS_ENABLED':'0','TELEGRAM_BACKUP_ENABLED':'0','REDIS_URL':'',
        'PEER_PRIVATE_URL':'','PEER_SERVICE_URL':'','PEER_SHARED_SECRET':'',
        'MEGA_EMAIL':'','MEGA_PASSWORD':'','TRAFFIC_AUDIT_ENABLED':'0',
    })
    code=(
        "import bot; "
        "assert callable(bot.main); "
        "assert bot.BOT_DISPLAY_NAME=='очнись_13.4'; "
        "assert (getattr(bot,'_SPLIT_STATE',{}) or {}).get('och13_mode')=='STANDALONE_R1'; "
        "print('OCH13_STARTUP_IMPORT_OK')"
    )
    try:
        cp=subprocess.run([sys.executable,'-c',code],cwd=str(ROOT),env=env,text=True,capture_output=True,timeout=120)
        check('startup_without_r2',cp.returncode==0 and 'OCH13_STARTUP_IMPORT_OK' in cp.stdout,(cp.stdout+'\n'+cp.stderr)[-1800:])
    except Exception as exc:
        check('startup_without_r2',False,exc)

passed=sum(1 for _,v,_ in checks if v); total=len(checks)
print(f'FINALIZATION OCHNIS 13.4: {passed}/{total} PASS')
if passed!=total:
    for name,val,detail in checks:
        if not val: print(' -',name,detail)
    raise SystemExit(1)
