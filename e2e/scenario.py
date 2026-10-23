"""Isolated real HTTP server and actual subprocess CLI; no hardware probes."""
import json, os, signal, subprocess, sys, tempfile, threading, time
from pathlib import Path
from http.server import ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from fleetq.server import Fleet, make_handler

TOKEN = 'fixture-token-0123456789abcdef'
with tempfile.TemporaryDirectory(prefix='fleetq-e2e-') as directory:
    root = Path(directory)
    cfg = root / 'fleet.toml'
    cfg.write_text(f'''[server]\ndb = "{root / 'q.db'}"\n[[host]]\nname = "fixture-a"\nmachine = "fixture-machine-a"\ncapacity_gb = 100\n[[host]]\nname = "fixture-b"\nmachine = "fixture-machine-b"\ncapacity_gb = 100\n''')
    fleet = Fleet(cfg)
    fleet.apply_probe({}, {"fixture-machine-a": True, "fixture-machine-b": True}, {}, time.time())
    server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(fleet, TOKEN))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    env = {**os.environ, 'FLEETQ_URL': f'http://127.0.0.1:{server.server_port}', 'FLEETQ_TOKEN': TOKEN, 'FLEETQ_OWNER': 'fixture-owner'}
    env.pop('FLEETQ_JOB', None)
    processes = []
    def command(*args, owner=None, extra=None):
        return subprocess.run([sys.executable, '-m', 'fleetq.cli', *map(str,args)], env={**env, **({'FLEETQ_OWNER':owner} if owner else {}), **(extra or {})}, text=True, capture_output=True, timeout=20)
    def ok(*args, **kwargs):
        result = command(*args, **kwargs)
        assert result.returncode == 0, (args, result.stdout, result.stderr)
        return result
    def acquire(*args):
        return json.loads(ok('acquire', '--host', 'fixture-a', '--label', 'isolated fixture', '--json', '--no-keepalive', *args).stdout)
    def show(job):
        return json.loads(ok('show', job['id'], '--json').stdout)
    def release(job):
        ok('release', job['id'])
    def spawn(*args):
        p = subprocess.Popen([sys.executable, '-m', 'fleetq.cli', *map(str,args)], env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        processes.append(p)
        return p
    def poll(predicate):
        deadline=time.monotonic()+8
        while time.monotonic()<deadline:
            if predicate(): return
            time.sleep(.05)
        raise AssertionError('fixture condition timed out')
    scenario=sys.argv[1]
    try:
        if scenario == 'lifecycle':
            ok('status', '--json'); ok('roster'); ok('log')
            job=acquire(); assert job['state']=='running'
            assert command('release',job['id'],owner='other-owner').returncode==1
            assert show(job)['state']=='running'
            assert command('inside','--host','fixture-a').returncode==1
            ok('inside','--host','fixture-a',extra={'FLEETQ_JOB':str(job['id'])})
            assert command('inside','--host','fixture-b',extra={'FLEETQ_JOB':str(job['id'])}).returncode==1
            release(job); assert show(job)['state']=='done'
        elif scenario == 'queue':
            first=acquire('--exclusive'); second=acquire(); assert second['state']=='queued'
            assert command('wait',second['id'],'--timeout','.001').returncode==3
            release(first); ok('wait',second['id'],'--timeout','.1'); assert show(second)['state']=='running'; release(second)
            first=acquire('--exclusive'); second=acquire(); release(second); assert show(second)['state']=='cancelled'; release(first)
        elif scenario == 'memory':
            first=acquire('--mem','60'); second=acquire('--mem','30'); assert second['state']=='running'
            third=acquire('--mem','20'); assert third['state']=='queued'
            exclusive=acquire('--exclusive'); last=acquire('--mem','1'); assert last['state']=='queued'
            release(first); assert show(third)['state']=='running'; assert show(last)['state']=='queued'
            release(second); release(third); assert show(exclusive)['state']=='running'; release(exclusive); assert show(last)['state']=='running'; release(last)
        elif scenario == 'run':
            script='import os,sys; assert os.environ["FLEETQ_JOB"].isdigit(); print("fixture command"); sys.exit(7)'
            result=command('run','--host','fixture-a','--label','exit fixture','--',sys.executable,'-c',script)
            assert result.returncode==7, result.stderr
            assert 'fixture command' in result.stdout
            job=acquire('--exclusive')
            ok('run','--host','fixture-a','--label','nested fixture','--',sys.executable,'-c','print("nested")',extra={'FLEETQ_JOB':str(job['id'])}); release(job)
            ok('run','--host','fixture-a,fixture-b','--each','--label','each fixture','--',sys.executable,'-c','import os,sys; assert sys.argv[1]==os.environ["FLEETQ_HOST"]; print(os.environ["FLEETQ_RANK"])','{host}')
            job=acquire('--exclusive'); assert job['state']=='running'; release(job)
        elif scenario == 'errors':
            for args in [('acquire','--host','absent','--label','x'),('show','999999'),('acquire','--host','fixture-a','--mem','bad','--label','x')]:
                assert command(*args).returncode!=0
            assert command('status',extra={'FLEETQ_TOKEN':'wrong'}).returncode!=0
            req=Request(env['FLEETQ_URL']+'/v1/jobs',data=b'{broken',headers={'Authorization':'Bearer '+TOKEN,'Content-Type':'application/json'})
            try: urlopen(req); raise AssertionError('malformed JSON accepted')
            except HTTPError as e: assert e.code==400
            assert command('status',extra={'FLEETQ_URL':'http://127.0.0.1:1'}).returncode==2
            cfg.write_text('[[host]]\nname="broken"\n')
            result=subprocess.run([sys.executable,'-m','fleetq.server','--config',str(cfg),'--check'],env=env,capture_output=True,text=True)
            assert result.returncode!=0
        elif scenario == 'signals':
            job=acquire('--exclusive')
            p=spawn('run','--host','fixture-a','--label','waiting signal','--wait-timeout','.001','--',sys.executable,'-c','raise Exception("must not run")')
            out,err=p.communicate(timeout=10); assert p.returncode==3,(out,err)
            release(job)
            marker=root/'started'
            p=spawn('run','--host','fixture-a','--label','signal fixture','--',sys.executable,'-c',f'from pathlib import Path; import time; Path({str(marker)!r}).touch(); time.sleep(60)')
            poll(marker.exists); p.send_signal(signal.SIGTERM); p.communicate(timeout=10)
            assert p.returncode!=0
            job=acquire('--exclusive'); assert job['state']=='running'; release(job)
        else: raise AssertionError(scenario)
        print(json.dumps({'scenario':scenario,'result':'passed'}))
    finally:
        for p in processes:
            if p.poll() is None: p.kill(); p.communicate()
        server.shutdown(); server.server_close()
