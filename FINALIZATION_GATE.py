#!/usr/bin/env python3
from pathlib import Path
import ast, os, py_compile, subprocess, sys, re
ROOT=Path(__file__).resolve().parent
checks=[]
def ck(name,cond,detail=''):
    checks.append((name,bool(cond),detail))
    if not cond: print('FAIL',name,detail)
def txt(name): return (ROOT/name).read_text('utf8') if (ROOT/name).is_file() else ''
required=['bot.py','start_front.py','runtime_config.py','requirements.txt','FINALIZATION_GATE.py']
ck('runtime_files',all((ROOT/x).is_file() for x in required))
for rel in ['bot.py','start_front.py','runtime_config.py','FINALIZATION_GATE.py']:
    try: ast.parse(txt(rel),filename=rel); good=True; detail=''
    except Exception as e: good=False; detail=str(e)
    ck('ast_'+rel,good,detail)
bot=txt('bot.py'); start=txt('start_front.py'); cfg=txt('runtime_config.py'); docker=txt('Dockerfile')
ck('release_name',"BOT_DISPLAY_NAME = 'очнись_13'" in bot)
ck('no_runtime_exec','_owner_install(' not in bot and 'exec(compile(' not in bot and 'owners_manifest.json' not in bot)
ck('release_flat','och13-release-flat' in bot and '1239' not in bot[:5000])
ck('r1_no_mega_boot', all(x not in start.lower() for x in ('subprocess','mega-login','mega-get','mega-put','mega-cmd')) and "MEGA_ENABLED'] = '0'" in start)
ck('r1_auto_r2','def _r13_peer_monitor_loop' in bot and 'OCH13_AUTO_STANDALONE_DISTRIBUTED' in bot and "R13_AUTO_DISTRIBUTED" in cfg)
ck('hot_path_cached','def _r1234_use_heavy(kind):' in bot and 'and _r1234_cached_peer_ok()' in bot)
ck('primary_sqlite','R1_LOCAL_SQLITE_THEN_OPTIONAL_R2' in start and 'SPLIT_PREBOOT_AUTHORITATIVE_R20' in start)
ck('r2_restore_boot',"/internal/restore/latest" in start and '/peer/health' in start)
ck('docker_no_mega',not docker or ('mega.nz' not in docker.lower() and not re.search(r'^\s*RUN\s+.*(?:megacmd|mega-login|mega-get|mega-put)', docker, re.I|re.M)))
ck('runtime_cfg_front_no_mega','OCH13: R1 never launches/owns MEGAcmd' in cfg)
# Count top-level function duplicates only as an audit. Historic inline chains may remain in the release-flat file,
# but there is no dynamic loader or second executable catalog.
try:
    tree=ast.parse(bot); defs={}; dup=0
    for n in tree.body:
        if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)):
            if n.name in defs: dup+=1
            defs[n.name]=n.lineno
    ck('function_inventory',len(defs)>500,f'unique={len(defs)} duplicate_def_events={dup}')
except Exception as e: ck('function_inventory',False,str(e))
if os.getenv('FINALIZATION_STARTUP_SMOKE','0').lower() in {'1','true','yes','on'}:
    env=dict(os.environ); env.setdefault('B_T','123456:STARTUPSMOKE'); env['BOT_DEFER_MAIN_R54']='1'; env['MEGA_ENABLED']='0'; env['PEER_SERVICE_URL']=''; env['PEER_SHARED_SECRET']=''
    cp=subprocess.run([sys.executable,'-c','import bot; assert callable(bot.main); print("STARTUP_IMPORT_OK")'],cwd=str(ROOT),env=env,text=True,capture_output=True,timeout=180)
    ck('startup_import',cp.returncode==0 and 'STARTUP_IMPORT_OK' in cp.stdout,(cp.stdout+'\n'+cp.stderr)[-1600:])
passed=sum(1 for _,v,_ in checks if v); total=len(checks)
print(f'FINALIZATION OCHNIS13: {passed}/{total} PASS')
for n,v,d in checks:
    if not v: print(' -',n,d)
if passed!=total: raise SystemExit(1)
