"""End-to-end: real HTTP server in a thread, real CLI, fake health checks."""
import json
import os
import sys
import threading
import time
from http.server import ThreadingHTTPServer
import urllib.request
import urllib.error

import pytest

from fleetq import cli
from fleetq.core import Observation
from fleetq.server import Fleet, make_handler, read_token

TOKEN = 'test-token-0123456789abcdef'
RO_TOKEN = 'readonly-token-0123456789abcdef'
CONFIG = """
[server]
db = "{db}"
stale_heartbeat_s = 180
[[host]]
name = "a"
machine = "ma"
aliases = ["alpha"]
capacity_gb = 100
[[host]]
name = "b"
machine = "mb"
capacity_gb = 100
[[service]]
name = "a-llm"
host = "a"
url = "http://ma:8000/v1"
models = ["m"]
roles = ["flight"]
mem_gb = 40
"""


class Clock:
    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t


@pytest.fixture
def env(tmp_path, monkeypatch):
    cfg = tmp_path / 'fleet.toml'
    cfg.write_text(CONFIG.format(db=tmp_path / 'q.db'))
    clock = Clock()
    fleet = Fleet(cfg, clock=clock)
    fleet.apply_probe({'a-llm': Observation('up', ['m'], 0, at=clock.t)}, {'ma': True, 'mb': True}, {}, clock.t)
    server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(fleet, TOKEN, RO_TOKEN))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f'http://127.0.0.1:{server.server_address[1]}'
    monkeypatch.setenv('FLEETQ_URL', url)
    monkeypatch.setenv('FLEETQ_TOKEN', TOKEN)
    monkeypatch.setenv('FLEETQ_OWNER', 'tester')
    monkeypatch.setattr(cli, 'POLL_S', 0.05)
    yield fleet, clock, url
    server.shutdown()


def api(url, method, path, body=None, token=TOKEN):
    req = urllib.request.Request(url + path, data=json.dumps(body).encode() if body is not None else None,
                                 method=method, headers={'Authorization': 'Bearer ' + token})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_run_passes_through_exit_code_and_records_it(env):
    fleet, _, url = env
    code = cli.main(['run', '--host', 'alpha', '--label', 'exit test', '--',
                     sys.executable, '-c', 'import sys; sys.exit(3)'])
    assert code == 3
    recent = api(url, 'GET', '/v1/status')[1]['recent']
    assert recent[0]['label'] == 'exit test' and recent[0]['state'] == 'done' and recent[0]['exit_code'] == 3


def test_run_waits_in_line_until_the_lease_ahead_is_released(env, capsys):
    fleet, _, url = env
    assert cli.main(['acquire', '--host', 'a', '--exclusive', '--label', 'bench', '--owner', 'other']) == 0
    lease_id = api(url, 'GET', '/v1/status')[1]['hosts'][0]['running'][0]['id']
    result = {}
    t = threading.Thread(target=lambda: result.update(code=cli.main(
        ['run', '--host', 'a', '--label', 'after', '--', sys.executable, '-c', 'pass'])))
    t.start()
    t.join(0.5)
    assert t.is_alive()  # still in line
    queued = api(url, 'GET', '/v1/status')[1]['hosts'][0]['queued']
    assert queued[0]['label'] == 'after' and 'held exclusively by' in queued[0]['reason']
    # Someone else's lease cannot be released without --force.
    assert cli.main(['release', str(lease_id)]) == 1
    assert cli.main(['release', str(lease_id), '--owner', 'other']) == 0
    t.join(5)
    assert result['code'] == 0
    assert 'waiting (#1 in line): a is held exclusively by' in capsys.readouterr().err


def test_auth_is_required_except_health(env):
    _, _, url = env
    assert api(url, 'GET', '/v1/status', token='wrong')[0] == 401
    with urllib.request.urlopen(url + '/healthz') as r:
        assert r.status == 200


def test_readonly_token_can_only_read(env):
    _, _, url = env
    full = api(url, 'GET', '/v1/status')[1]
    code, body = api(url, 'GET', '/v1/status', token=RO_TOKEN)
    assert code == 200 and body == full
    assert api(url, 'GET', '/v1/roster', token=RO_TOKEN)[0] == 200
    assert api(url, 'GET', '/v1/events?limit=5', token=RO_TOKEN)[0] == 200
    for method, path, body in [('POST', '/v1/jobs', {'label': 'x', 'owner': 'o', 'hosts': ['a']}),
                               ('POST', '/v1/probe', None), ('POST', '/v1/resolve', {}),
                               ('POST', '/v1/jobs/1/release', {}), ('POST', '/v1/jobs/1/heartbeat', None),
                               ('POST', '/v1/services/a-llm/mark-up', {}), ('POST', '/v1/status', None),
                               ('GET', '/v1/jobs/1', None)]:
        code, err = api(url, method, path, body, token=RO_TOKEN)
        assert code == 403 and 'read-only token' in err['error'], (method, path)
    assert api(url, 'GET', '/v1/status')[1]['hosts'][0]['running'] == []  # nothing was submitted
    # The full token keeps working for writes.
    assert api(url, 'POST', '/v1/jobs', {'label': 'x', 'owner': 'o', 'hosts': ['a']}, token=TOKEN)[0] == 200


def test_readonly_token_is_off_unless_configured(tmp_path):
    cfg = tmp_path / 'fleet.toml'
    cfg.write_text(CONFIG.format(db=tmp_path / 'q.db'))
    server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(Fleet(cfg, clock=Clock()), TOKEN))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f'http://127.0.0.1:{server.server_address[1]}'
        assert api(url, 'GET', '/v1/status', token=RO_TOKEN)[0] == 401
        assert api(url, 'GET', '/v1/status', token='')[0] == 401
    finally:
        server.shutdown()


def test_token_files_must_be_private_and_long(tmp_path):
    class Parser:
        def error(self, msg):
            raise SystemExit(msg)
    path = tmp_path / 'readonly_token'
    path.write_text(RO_TOKEN + '\n')
    path.chmod(0o644)
    with pytest.raises(SystemExit, match='must not be readable'):
        read_token(Parser(), path)
    path.chmod(0o600)
    assert read_token(Parser(), path) == RO_TOKEN
    path.write_text('short')
    with pytest.raises(SystemExit, match='at least 20'):
        read_token(Parser(), path)


def test_bad_requests_are_explained(env):
    _, _, url = env
    code, body = api(url, 'POST', '/v1/jobs', {'label': 'x', 'owner': 'o', 'uses': ['nope']})
    assert code == 400 and 'unknown role or service "nope"' in body['error']
    code, body = api(url, 'POST', '/v1/jobs', {'label': 'x', 'owner': 'o'})
    assert code == 400 and 'name a host' in body['error']
    code, body = api(url, 'POST', '/v1/jobs', {'label': 'x', 'owner': 'o', 'hosts': ['a'], 'mem_gb': 500})
    assert code == 400 and 'can never fit' in body['error']


def test_takedown_then_restore_seen_by_health_check(env):
    fleet, clock, url = env
    _, job = api(url, 'POST', '/v1/jobs', {'label': 'swap model', 'owner': 'o', 'takes_down': ['flight']})
    assert job['state'] == 'running' and job['hosts'] == ['a'] and job['roles_taken_down'] == ['flight']
    _, user = api(url, 'POST', '/v1/jobs', {'label': 'mission', 'owner': 'o', 'uses': ['flight']})
    assert user['state'] == 'queued' and 'taken down by' in user['reason']
    clock.t += 60
    api(url, 'POST', f'/v1/jobs/{job["id"]}/release', {'owner': 'o'})
    svc = api(url, 'GET', '/v1/status')[1]['hosts'][0]['services'][0]
    assert svc['status'] == 'restoring'
    clock.t += 30
    fleet.apply_probe({'a-llm': Observation('up', ['m'], 0, at=clock.t)}, {}, {}, clock.t)
    status = api(url, 'GET', '/v1/status')[1]
    assert status['hosts'][0]['services'][0]['status'] == 'up'
    assert status['hosts'][0]['running'][0]['label'] == 'mission'
    events = [e['text'] for e in api(url, 'GET', '/v1/events')[1]['events']]
    assert any('a-llm is back after' in e for e in events)


def test_queued_run_whose_client_vanished_is_dropped(env):
    fleet, clock, url = env
    api(url, 'POST', '/v1/jobs', {'label': 'hold', 'owner': 'o', 'hosts': ['a'], 'exclusive': True})
    _, ghost = api(url, 'POST', '/v1/jobs', {'label': 'ghost', 'owner': 'o', 'hosts': ['a'], 'kind': 'run'})
    assert ghost['state'] == 'queued'
    clock.t += 200
    fleet.apply_probe({}, {}, {}, clock.t)
    assert api(url, 'GET', f'/v1/jobs/{ghost["id"]}')[1]['state'] == 'cancelled'


def test_running_run_job_without_heartbeat_is_flagged_not_released(env):
    fleet, clock, url = env
    _, job = api(url, 'POST', '/v1/jobs', {'label': 'long bench', 'owner': 'o', 'hosts': ['a'], 'kind': 'run'})
    clock.t += 400
    fleet.apply_probe({}, {}, {}, clock.t)
    status = api(url, 'GET', '/v1/status')[1]
    assert status['hosts'][0]['running'][0]['state'] == 'running'
    assert any('stale: no heartbeat' in w for w in status['warnings'])


def test_overdue_lease_is_flagged(env):
    fleet, clock, url = env
    api(url, 'POST', '/v1/jobs', {'label': 'model swap', 'owner': 'o', 'hosts': ['b'], 'expect_min': 30})
    clock.t += 31 * 60
    assert any('overdue' in w for w in api(url, 'GET', '/v1/status')[1]['warnings'])


def test_unreachable_server_refuses_unless_no_queue(monkeypatch, tmp_path):
    monkeypatch.setenv('FLEETQ_URL', 'http://127.0.0.1:1')
    monkeypatch.setenv('FLEETQ_TOKEN', TOKEN)
    marker = tmp_path / 'ran'
    cmd = [sys.executable, '-c', f'open({str(marker)!r}, "w").close()']
    assert cli.main(['run', '--host', 'a', '--label', 'x', '--'] + cmd) == cli.EXIT_UNREACHABLE
    assert not marker.exists()
    assert cli.main(['run', '--host', 'a', '--label', 'x', '--no-queue', '--'] + cmd) == 0
    assert marker.exists()


def test_status_renders_for_humans(env):
    _, _, url = env
    api(url, 'POST', '/v1/jobs', {'label': 'bench', 'owner': 'o', 'hosts': ['a'], 'exclusive': True})
    api(url, 'POST', '/v1/jobs', {'label': 'next', 'owner': 'o', 'hosts': ['a'], 'mem_gb': 10})
    text = cli.render_status(api(url, 'GET', '/v1/status')[1])
    assert 'a (ma) · online' in text
    assert 'memory    100 GB: services 40 · jobs 0 · free 60' in text
    assert '    #1  bench\n        o · for 0s · exclusive' in text
    # The job holding the host is listed above, so the reason names it by id alone.
    assert '    1. #2  next\n           o · waits: a is held exclusively by #1\n' in text


def test_status_lists_a_multi_host_job_once_and_wraps_to_width(env):
    _, _, url = env
    api(url, 'POST', '/v1/jobs', {'label': 'tp2 ' + 'word ' * 40, 'owner': 'o', 'hosts': ['a', 'b'],
                                  'mem_gb': 5})
    st = api(url, 'GET', '/v1/status')[1]
    text = cli.render_status(st, width=80)
    assert text.count('tp2 word') == 1
    assert '#1  same job as on a' in text
    assert all(len(line) <= 80 for line in text.splitlines())
    assert '#1  same job as on' not in cli.render_status(st, 'b')
    assert '\033[' not in text and '\033[' in cli.render_status(st, color=True)


def test_wedged_server_is_detected_across_probes_and_its_restart_skips_the_line(env):
    fleet, clock, url = env

    def probe(progress, in_flight=1):
        fleet.apply_probe({'a-llm': Observation('up', ['m'], in_flight, at=clock.t, progress=progress)},
                          {}, {}, clock.t)

    probe(500.0)
    _, bench = api(url, 'POST', '/v1/jobs', {'label': 'bench', 'owner': 'o', 'hosts': ['a'], 'exclusive': True})
    assert bench['state'] == 'queued' and 'a-llm (1 in flight)' in bench['reason']
    for _ in range(7):  # frozen counter for 210 s, past wedge_after_s = 180
        clock.t += 30
        probe(500.0)
    status = api(url, 'GET', '/v1/status')[1]
    svc = status['hosts'][0]['services'][0]
    assert svc['status'] == 'wedged' and svc['stuck_s'] == 210
    assert any('WEDGED' in w for w in status['warnings'])
    assert not any('untracked requests in flight' in w for w in status['warnings'])
    assert 'a-llm is wedged' in api(url, 'GET', f'/v1/jobs/{bench["id"]}')[1]['reason']
    _, restart = api(url, 'POST', '/v1/jobs', {'label': 'restart', 'owner': 'o', 'takes_down': ['flight']})
    assert restart['state'] == 'running'  # ahead of #bench, which waits for a quiet a-llm
    clock.t += 60
    api(url, 'POST', f'/v1/jobs/{restart["id"]}/release', {'owner': 'o'})
    clock.t += 30
    probe(0.0, in_flight=0)  # back up, counters restarted, nothing in flight
    assert api(url, 'GET', '/v1/status')[1]['hosts'][0]['services'][0]['status'] == 'up'
    assert api(url, 'GET', f'/v1/jobs/{bench["id"]}')[1]['state'] == 'running'


def test_untracked_memory_and_busy_gpu_are_reported(env):
    fleet, clock, url = env
    fleet.apply_probe({}, {}, {'a': {'free_gib': 20.0, 'util': 0.9}}, clock.t)
    status = api(url, 'GET', '/v1/status')[1]
    assert any('about 40 GB in use that fleetq does not track' in w for w in status['warnings'])
    assert any('GPU 90% busy with no fleetq job' in w for w in status['warnings'])
    _, job = api(url, 'POST', '/v1/jobs', {'label': 'bench', 'owner': 'o', 'hosts': ['a'], 'exclusive': True})
    assert job['state'] == 'queued' and 'GPU is 90% busy' in job['reason']


def test_for_url_resolves_and_off_fleet_urls_run_without_a_slot(env, capsys):
    _, _, url = env
    code, body = api(url, 'POST', '/v1/resolve', {'for_url': 'http://ma:8000/v1'})
    assert code == 200 and body['hosts'] == ['a']
    code, body = api(url, 'POST', '/v1/resolve', {'for_url': 'http://example.com/v1'})
    assert code == 422 and body['not_fleet']
    assert cli.main(['run', '--for-url', 'http://example.com/v1', '--label', 'x', '--',
                     sys.executable, '-c', 'pass']) == 0
    assert 'not on a fleet box; running without a slot' in capsys.readouterr().err


def test_nested_run_inside_a_covering_job_does_not_queue(env, tmp_path, monkeypatch, capsys):
    _, _, url = env
    _, lease = api(url, 'POST', '/v1/jobs', {'label': 'swap model', 'owner': 'tester', 'hosts': ['a'],
                                            'exclusive': True})
    monkeypatch.setenv('FLEETQ_JOB', str(lease['id']))
    out = tmp_path / 'env'
    code = cli.main(['run', '--host', 'a', '--exclusive', '--label', 'bench inside', '--',
                     sys.executable, '-c', f'import os; open({str(out)!r}, "w").write(os.environ["FLEETQ_JOB"])'])
    assert code == 0 and out.read_text() == str(lease['id'])
    assert f'running inside #{lease["id"]}' in capsys.readouterr().err
    assert cli.main(['inside', '--host', 'alpha']) == 0
    assert cli.main(['inside', '--host', 'b']) == 1
    monkeypatch.delenv('FLEETQ_JOB')
    assert cli.main(['inside', '--host', 'a']) == 1


def test_run_gives_its_command_its_own_job_id(env, tmp_path):
    out = tmp_path / 'env'
    assert cli.main(['run', '--host', 'b', '--label', 'x', '--', sys.executable, '-c',
                     f'import os; open({str(out)!r}, "w").write(os.environ["FLEETQ_JOB"])']) == 0
    assert out.read_text().isdigit()


def test_waiting_behind_your_own_job_prints_a_hint(env, capsys):
    _, _, url = env
    api(url, 'POST', '/v1/jobs', {'label': 'my lease', 'owner': 'tester', 'hosts': ['a'], 'exclusive': True})
    code = cli.main(['run', '--host', 'a', '--label', 'x', '--wait-timeout', '0.001', '--', sys.executable, '-c', 'pass'])
    assert code == cli.EXIT_TIMEOUT
    assert 'rerun with FLEETQ_JOB=1 set' in capsys.readouterr().err


def test_uses_url_takes_a_shared_lease_on_the_service_behind_it(env):
    _, _, url = env
    _, job = api(url, 'POST', '/v1/jobs', {'label': 'mission', 'owner': 'mc', 'kind': 'run',
                                          'uses_url': 'http://ma:8000/v1'})
    assert job['state'] == 'running' and job['hosts'] == ['a'] and job['uses'] == ['a-llm']
    _, takedown = api(url, 'POST', '/v1/jobs', {'label': 'swap', 'owner': 'o', 'takes_down': ['flight']})
    assert takedown['state'] == 'queued' and 'in use by #' in takedown['reason']
    code, body = api(url, 'POST', '/v1/jobs', {'label': 'x', 'owner': 'mc', 'uses_url': 'http://example.com/v1'})
    assert code == 422 and body['not_fleet']


def test_waiting_client_rides_out_a_brief_server_outage(env, monkeypatch):
    _, _, url = env
    _, job = api(url, 'POST', '/v1/jobs', {'label': 'queued', 'owner': 'o', 'hosts': ['a'], 'kind': 'run'})
    client = cli.Client()
    real, calls = client.call, []

    def flaky(method, path, body=None):
        calls.append(path)
        if len(calls) <= 2:
            raise cli.Unreachable('restarting')
        return {**real(method, path, body), 'state': 'running'}
    monkeypatch.setattr(client, 'call', flaky)
    got = cli.wait_until_running(client, {**job, 'state': 'queued'}, None, heartbeat=False)
    assert got['state'] == 'running' and len(calls) == 3
    monkeypatch.setattr(cli, 'OUTAGE_S', 0)
    calls.clear()
    monkeypatch.setattr(client, 'call', lambda *a, **k: (_ for _ in ()).throw(cli.Unreachable('down')))
    with pytest.raises(cli.Unreachable):
        cli.wait_until_running(client, {**job, 'state': 'queued'}, None, heartbeat=False)


def test_per_host_memory_resolves_aliases_adds_hosts_and_checks_capacity(env):
    _, _, url = env
    _, job = api(url, 'POST', '/v1/jobs', {'label': 'tp2', 'owner': 'o', 'mem_map': {'alpha': 50, 'b': 90}})
    assert job['hosts'] == ['a', 'b'] and job['mem_map'] == {'a': 50.0, 'b': 90.0}
    hosts = {h['name']: h for h in api(url, 'GET', '/v1/status')[1]['hosts']}
    assert (hosts['a']['jobs_gb'], hosts['b']['jobs_gb']) == (50, 90)
    code, body = api(url, 'POST', '/v1/jobs', {'label': 'x', 'owner': 'o', 'hosts': ['a'], 'mem_map': {'a': 500}})
    assert code == 400 and 'can never fit: 500 GB on a' in body['error']
    code, body = api(url, 'POST', '/v1/jobs', {'label': 'x', 'owner': 'o', 'mem_map': {'a': 'lots'}})
    assert code == 400 and 'memory for a must be a number' in body['error']


def test_existing_database_gains_the_per_host_memory_column(tmp_path):
    import sqlite3
    from fleetq.store import Store
    path = tmp_path / 'old.db'
    old = sqlite3.connect(path)
    old.executescript("""CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, state TEXT NOT NULL,
      kind TEXT NOT NULL, owner TEXT NOT NULL, label TEXT NOT NULL, hosts TEXT NOT NULL,
      mem_gb REAL NOT NULL DEFAULT 0, exclusive INTEGER NOT NULL DEFAULT 0, uses TEXT NOT NULL DEFAULT '[]',
      takes_down TEXT NOT NULL DEFAULT '[]', created_at REAL NOT NULL, started_at REAL, ended_at REAL,
      heartbeat_at REAL, expect_s REAL, command TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '',
      exit_code INTEGER, note TEXT NOT NULL DEFAULT '');
      INSERT INTO jobs (state, kind, owner, label, hosts, created_at) VALUES ('running','lease','o','old','["a"]',1);""")
    old.commit()
    old.close()
    store = Store(path)
    assert store.job(1).mem_map == {} and store.job(1).mem_on('a') == 0
    assert store.job(1).serves == [] and store.job(1).serves_caps == []


def test_mem_option_takes_a_default_and_per_host_values():
    assert cli.parse_mem(['30']) == (30.0, {})
    assert cli.parse_mem(['spark1=100,spark2=90']) == (0.0, {'spark1': 100.0, 'spark2': 90.0})
    assert cli.parse_mem(['20', 'a=5']) == (20.0, {'a': 5.0})
    assert cli.parse_mem(None) == (0.0, {})
    with pytest.raises(SystemExit):
        cli.parse_mem(['a=lots'])


def test_each_runs_the_command_once_per_host_with_placeholders(env, tmp_path, capsys):
    script = ('import os, sys; open(os.path.join(sys.argv[1], os.environ["FLEETQ_HOST"]), "w")'
              '.write(sys.argv[2] + " " + os.environ["FLEETQ_RANK"] + " " + os.environ["FLEETQ_JOB"]); print("hello", sys.argv[2])')
    code = cli.main(['run', '--host', 'a,b', '--each', '--label', 'fan out', '--',
                     sys.executable, '-c', script, str(tmp_path), '{host}:{machine}:{rank}/{nhosts}'])
    assert code == 0
    a, b = (tmp_path / 'a').read_text().split(), (tmp_path / 'b').read_text().split()
    assert a[:2] == ['a:ma:0/2', '0'] and b[:2] == ['b:mb:1/2', '1'] and a[2] == b[2]  # one job for both
    out = capsys.readouterr()
    assert '[a] hello a:ma:0/2' in out.out and '[b] hello b:mb:1/2' in out.out
    assert 'each: a exit 0, b exit 0' in out.err


def test_each_stops_the_other_hosts_when_one_fails(env, monkeypatch):
    monkeypatch.setattr(cli, 'STOP_GRACE_S', 2)
    script = 'import sys, time; sys.exit(3) if sys.argv[1] == "0" else time.sleep(60)'
    start = time.time()
    code = cli.main(['run', '--host', 'a,b', '--each', '--label', 'ranks', '--', sys.executable, '-c', script, '{rank}'])
    assert code == 3 and time.time() - start < 15


def test_each_keep_going_lets_the_others_finish(env, tmp_path):
    marker = tmp_path / 'finished'
    script = ('import sys, time; r = sys.argv[1]\n'
              'if r == "0": sys.exit(3)\n'
              'time.sleep(0.5); open(sys.argv[2], "w").close()')
    code = cli.main(['run', '--host', 'a,b', '--each', '--keep-going', '--label', 'ranks', '--',
                     sys.executable, '-c', script, '{rank}', str(marker)])
    assert code == 3 and marker.exists()


def test_status_can_show_several_hosts(env):
    _, _, url = env
    st = api(url, 'GET', '/v1/status')[1]
    both, one = cli.render_status(st, 'a,b'), cli.render_status(st, 'b')
    assert 'a (ma)' in both and 'b (mb)' in both
    assert 'a (ma)' not in one and 'b (mb)' in one


def start_queued_run(url, marker, extra=()):
    """Hold host a with someone else's lease and queue a `run` behind it; returns (thread, result, job id)."""
    api(url, 'POST', '/v1/jobs', {'label': 'hold', 'owner': 'other', 'hosts': ['a'], 'exclusive': True})
    result = {}
    t = threading.Thread(target=lambda: result.update(code=cli.main(
        ['run', '--host', 'a', '--exclusive', '--label', 'behind', *extra, '--',
         sys.executable, '-c', f'open({str(marker)!r}, "w").close()'])))
    t.start()
    for _ in range(100):
        queued = api(url, 'GET', '/v1/status')[1]['hosts'][0]['queued']
        if queued:
            return t, result, queued[0]['id']
        time.sleep(0.02)
    raise AssertionError('run never queued')


def test_released_queued_run_exits_without_running_its_command(env, tmp_path, capsys):
    _, _, url = env
    marker = tmp_path / 'ran'
    t, result, job_id = start_queued_run(url, marker)
    assert cli.main(['release', str(job_id)]) == 0
    t.join(5)
    assert not t.is_alive() and result['code'] == cli.EXIT_CANCELLED
    assert not marker.exists()
    assert f'#{job_id} is cancelled' in capsys.readouterr().err
    assert api(url, 'GET', f'/v1/jobs/{job_id}')[1]['state'] == 'cancelled'


def test_cancelled_queued_run_exits_without_running_its_command(env, tmp_path):
    fleet, clock, url = env
    marker = tmp_path / 'ran'
    t, result, job_id = start_queued_run(url, marker)
    clock.t += 200  # past stale_heartbeat_s: the server drops the queued run
    fleet.apply_probe({}, {}, {}, clock.t)
    assert api(url, 'GET', f'/v1/jobs/{job_id}')[1]['state'] == 'cancelled'
    t.join(5)
    assert not t.is_alive() and result['code'] == cli.EXIT_CANCELLED
    assert not marker.exists()


def test_run_rechecks_its_slot_right_before_starting(env, tmp_path, monkeypatch):
    _, _, url = env
    marker = tmp_path / 'ran'
    real = cli.wait_until_running

    def released_after_last_poll(client, job, timeout_s, heartbeat):
        got = real(client, job, timeout_s, heartbeat)
        api(url, 'POST', f'/v1/jobs/{job["id"]}/release', {'owner': 'tester'})  # lands between poll and exec
        return got
    monkeypatch.setattr(cli, 'wait_until_running', released_after_last_poll)
    code = cli.main(['run', '--host', 'b', '--label', 'x', '--', sys.executable, '-c',
                     f'open({str(marker)!r}, "w").close()'])
    assert code == cli.EXIT_CANCELLED and not marker.exists()


def test_acquire_wait_reports_a_cancelled_lease(env, monkeypatch):
    _, _, url = env
    api(url, 'POST', '/v1/jobs', {'label': 'hold', 'owner': 'other', 'hosts': ['a'], 'exclusive': True})
    real = cli.wait_until_running

    def release_then_wait(client, job, timeout_s, heartbeat):
        api(url, 'POST', f'/v1/jobs/{job["id"]}/release', {'owner': 'tester'})
        return real(client, job, timeout_s, heartbeat)
    monkeypatch.setattr(cli, 'wait_until_running', release_then_wait)
    assert cli.main(['acquire', '--host', 'a', '--label', 'lease', '--wait']) == cli.EXIT_CANCELLED


def test_models_endpoint_and_cli_filter_by_capability(env, capsys):
    fleet, clock, url = env
    fleet.services[0].capabilities = ['text', 'image']
    code, body = api(url, 'GET', '/v1/models')
    assert code == 200 and [(m['model'], m['url'], m['state']) for m in body['models']] == [('m', 'http://ma:8000/v1', 'up')]
    assert api(url, 'GET', '/v1/models?cap=image')[1]['models'][0]['capabilities'] == ['text', 'image']
    assert api(url, 'GET', '/v1/models?cap=image,video')[1]['models'] == []
    code, body = api(url, 'GET', '/v1/models?cap=vison')
    assert code == 400 and 'unknown capability vison' in body['error']
    assert cli.main(['models', '--cap', 'image']) == 0
    out = capsys.readouterr().out
    assert out.startswith('+  m  text,image') and 'http://ma:8000/v1' in out and 'flight' in out
    st = api(url, 'GET', '/v1/status')[1]['hosts'][0]['services'][0]
    assert st['url'] == 'http://ma:8000/v1' and st['served'] == [{'id': 'm'}]


def test_serves_job_is_probed_while_running_and_dropped_on_release(env, monkeypatch):
    fleet, clock, url = env
    code, job = api(url, 'POST', '/v1/jobs', {'label': 'try model', 'owner': 'o',
                                              'serves': ['http://ma:8001/v1'], 'serves_caps': ['text']})
    assert code == 200 and job['state'] == 'running' and job['hosts'] == ['a'] and job['serves'] == ['http://ma:8001/v1']
    asked = []

    def fake_probe_all(services, machines, collectors, now, port):
        asked.extend(s.url for s in services)
        obs = {s.name: Observation('up', ['m'], 0, at=now) for s in services if s.name == 'a-llm'}
        obs.update({s.name: Observation('up', ['trial'], 0, at=now, served=[{'id': 'trial'}])
                    for s in services if s.name.startswith('job ')})
        return obs, {'ma': True, 'mb': True}, {}
    monkeypatch.setattr('fleetq.probes.probe_all', fake_probe_all)
    fleet.probe_now()
    assert 'http://ma:8001/v1' in asked
    trial = [m for m in api(url, 'GET', '/v1/models')[1]['models'] if m['job']]
    assert [(m['model'], m['capabilities'], m['job']['id']) for m in trial] == [('trial', ['text'], job['id'])]
    api(url, 'POST', f'/v1/jobs/{job["id"]}/release', {'owner': 'o'})
    assert fleet.job_obs == {}
    assert [m for m in api(url, 'GET', '/v1/models?all=1')[1]['models'] if m['job']] == []
    asked.clear()
    fleet.probe_now()
    assert asked == ['http://ma:8000/v1']


def test_serves_and_caps_are_checked(env):
    _, _, url = env
    code, body = api(url, 'POST', '/v1/jobs', {'label': 'x', 'owner': 'o', 'hosts': ['a'], 'serves': ['ma:8001']})
    assert code == 400 and 'model server URL' in body['error']
    code, body = api(url, 'POST', '/v1/jobs', {'label': 'x', 'owner': 'o', 'hosts': ['a'],
                                               'serves': ['http://ma:8001/v1'], 'serves_caps': ['vision']})
    assert code == 400 and 'unknown capability vision' in body['error']
    code, body = api(url, 'POST', '/v1/jobs', {'label': 'x', 'owner': 'o', 'hosts': ['a'], 'serves_caps': ['text']})
    assert code == 400 and 'give --serves too' in body['error']


def test_config_rejects_an_unknown_capability(tmp_path):
    from fleetq.server import load_config
    cfg = tmp_path / 'f.toml'
    cfg.write_text(CONFIG.format(db=tmp_path / 'q.db') + 'capabilities = ["text", "vison"]\n')
    with pytest.raises(ValueError, match='service a-llm: unknown capability vison'):
        load_config(cfg)
    cfg.write_text(CONFIG.format(db=tmp_path / 'q.db') + 'capabilities = ["text"]\nmodel_capabilities = { m = ["text", "image"] }\n')
    svc = load_config(cfg)[3][0]
    assert svc.capabilities == ['text'] and svc.model_capabilities == {'m': ['text', 'image']}


def test_run_serves_flag_reaches_the_job(env):
    fleet, _, url = env
    code = cli.main(['run', '--host', 'a', '--serves', 'http://ma:8002/v1', '--caps', 'text,image',
                     '--label', 'serve test', '--', sys.executable, '-c', 'pass'])
    assert code == 0
    recent = api(url, 'GET', '/v1/status')[1]['recent'][0]
    assert recent['serves'] == ['http://ma:8002/v1'] and recent['serves_caps'] == ['text', 'image']
