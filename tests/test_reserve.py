"""Loads, reservations, backfill deadlines, update and when: server and CLI end to end."""
import json
import os
import signal
import subprocess
import sys
import threading
import time

import pytest

from fleetq import cli, core, server
from test_server import api, env  # noqa: F401  (env is a fixture)


def submit(fleet, **kw):
    body = {'label': kw.pop('label', 'job'), 'owner': kw.pop('owner', 'me'), 'hosts': kw.pop('hosts', ['a'])}
    return fleet.submit({**body, **kw})


def kinds(fleet):
    return [e['kind'] for e in fleet.store.events(100)]


def test_reservation_that_collides_with_another_is_refused_with_free_windows(env, capsys):
    fleet, clock, url = env
    bench = submit(fleet, label='bench', quiet=['gpu', 'cpu'], start_in_s=3600, expect_min=60)
    assert bench['state'] == 'queued' and bench['start_in_s'] == 3600
    code, body = api(url, 'POST', '/v1/jobs', {'label': 'gpu', 'owner': 'me', 'hosts': ['a'], 'mem_gb': 5,
                                               'start_in_s': 5400, 'expect_min': 60})
    assert code == 409 and f'collides with reservation #{bench["id"]}' in body['error']
    assert 'needs gpu quiet on a' in body['error'] and body['conflicts'] == [bench['id']]
    now = clock.t
    assert body['free'][:2] == [{'start': now, 'until': now + 3600}, {'start': now + 7200, 'until': None}]
    # What does not collide may overlap: a download, or the other host.
    assert submit(fleet, label='dl', load=['net', 'disk'], start_in_s=5400, expect_min=60)['state'] == 'queued'
    assert submit(fleet, hosts=['b'], start_in_s=5400, expect_min=60)['state'] == 'queued'
    # The CLI shows the free windows.
    assert cli.main(['reserve', '--host', 'a', '--in', '90m', '--for', '1h', '--label', 'clash']) == 1
    err = capsys.readouterr().err
    assert 'collides with reservation' in err and 'free: now until' in err
    assert cli.main(['run', '--host', 'a', '--in', '90m', '--expect', '60', '--label', 'clash', '--', 'true']) == 1
    assert 'free: now until' in capsys.readouterr().err
    code, body = api(url, 'POST', '/v1/jobs', {'label': 'x', 'owner': 'me', 'hosts': ['a'], 'start_in_s': 60})
    assert code == 400 and 'needs a length' in body['error']


def test_reservation_comes_due_on_a_tick_and_lapses_when_nobody_claims_it(env):
    fleet, clock, _ = env
    r = submit(fleet, label='booked', exclusive=True, start_in_s=600, expect_min=60, takes_down=['a-llm'])
    claimed = submit(fleet, label='claimed', hosts=['b'], start_in_s=600, expect_min=60)
    assert fleet.next_due() == clock.t + 600
    clock.t += 600
    v = fleet.version
    fleet.tick()
    assert fleet.version > v  # long-polls wake up when a reservation comes due
    job = fleet.get(r['id'])
    assert job['state'] == 'running' and any(f.startswith('no-show') for f in job['flags'])
    clock.t += 1
    fleet.get(r['id'], seen=True, owner='someone')        # a third party looking does not claim it
    fleet.heartbeat(r['id'], owner='someone')             # nor does its heartbeat
    fleet.get(claimed['id'], seen=True, owner='me')       # its owner checks on it: claimed
    assert fleet.next_due() == clock.t - 1 + 901
    clock.t += 900
    fleet.tick()
    lapsed = fleet.get(r['id'])
    assert lapsed['state'] == 'cancelled' and lapsed['note'].startswith('no-show')
    # It never ran anything, so a-llm was never really down: no restore to wait for.
    assert 'lapsed' in kinds(fleet) and fleet.store.restores() == {}
    assert fleet.status()['hosts'][0]['running'] == []
    assert fleet.get(claimed['id'])['state'] == 'running'


def test_running_work_overrunning_into_a_reservation_is_flagged_not_stopped(env):
    fleet, clock, _ = env
    bench = submit(fleet, label='bench', exclusive=True, expect_min=30)
    assert bench['state'] == 'running'
    booked = submit(fleet, label='booked', mem_gb=10, start_in_s=600, expect_min=60)
    clock.t += 700
    fleet.tick()
    assert fleet.get(booked['id'])['state'] == 'queued'
    st = fleet.status()
    flag = f'overrunning into reservation #{booked["id"]} "booked" (me), due 1m ago'
    assert any(flag in w for w in st['warnings'])
    assert fleet.get(bench['id'])['state'] == 'running'
    fleet.tick()
    assert kinds(fleet).count('overrun') == 1
    fleet.release(bench['id'], {'owner': 'me'})
    assert fleet.get(booked['id'])['state'] == 'running'


def test_update_lowers_claims_at_once_and_refuses_to_raise_them(env):
    fleet, _, url = env
    dl = submit(fleet, label='download', mem_gb=50, load=['gpu', 'net'])
    bench = submit(fleet, label='bench', quiet=['gpu'])
    assert bench['state'] == 'queued' and 'loads it' in bench['reason']
    code, body = api(url, 'POST', f'/v1/jobs/{dl["id"]}/update', {'owner': 'me', 'load': ['net', 'disk']})
    assert code == 409 and 'can only lower claims' in body['error'] and 'queue a new job' in body['error']
    code, body = api(url, 'POST', f'/v1/jobs/{dl["id"]}/update', {'owner': 'me', 'mem_gb': 60})
    assert code == 409 and '60 GB on a (holds 50)' in body['error']
    assert api(url, 'POST', f'/v1/jobs/{dl["id"]}/update', {'owner': 'other', 'load': ['net']})[0] == 409
    assert cli.main(['update', str(dl['id']), '--load', 'net', '--mem', '20', '--owner', 'me']) == 0
    assert fleet.get(dl['id'])['load'] == ['net'] and fleet.get(dl['id'])['mem_gb'] == 20
    assert fleet.get(bench['id'])['state'] == 'running'


def test_when_says_now_or_estimates_the_start(env, capsys):
    fleet, _, _ = env
    submit(fleet, label='bench', exclusive=True, expect_min=60)
    assert cli.main(['when', '--host', 'b', '--mem', '5', '--expect', '10']) == 0
    assert capsys.readouterr().out.startswith('starts now on b')
    r = fleet.when({'hosts': ['a'], 'mem_gb': 5, 'expect_min': 10})
    assert not r['starts_now'] and 'held exclusively' in r['reason'] and r['start_in_s'] == 3600
    assert cli.main(['when', '--host', 'a', '--mem', '5', '--expect', '10']) == 0
    out = capsys.readouterr().out
    assert 'not now on a' in out and 'estimated start' in out and '(in 60m)' in out
    assert fleet.store.active_jobs()[-1].label == 'bench'  # when queues nothing


def test_status_shows_reservations_backfill_window_and_unused_gpu(env):
    fleet, clock, _ = env
    submit(fleet, label='download', load=['net'], expect_min=60)
    head = submit(fleet, label='bench', quiet=['gpu'], exclusive=True)
    booked = submit(fleet, label='later', hosts=['b'], start_in_s=7200, expect_min=90)
    fleet.actual['a'] = ({'util': 0.6}, clock.t)
    st = fleet.status()
    a = next(h for h in st['hosts'] if h['name'] == 'a')
    assert a['backfill']['before'] == head['id'] and a['backfill']['fits_s'] == (3600 - 300) / 1.2
    b = next(h for h in st['hosts'] if h['name'] == 'b')
    assert [j['id'] for j in b['reservations']] == [booked['id']] and b['queued'] == []
    assert b['backfill']['reservation'] and b['backfill']['before'] == booked['id']
    assert any('no job there declares --load gpu' in w for w in st['warnings'])
    text = cli.render_status(st)
    assert f'run jobs with --expect up to 45m fit before #{head["id"]}' in text
    assert 'reserved' in text and 'later' in text and 'for 1h30m' in text
    assert 'loads net' in text


def test_run_job_ahead_of_the_line_is_stopped_at_its_deadline(env, monkeypatch, capsys):
    fleet, clock, url = env
    monkeypatch.setattr(cli, 'HEARTBEAT_S', 0.1)
    monkeypatch.setattr(cli, 'STOP_GRACE_S', 0.5)
    first = submit(fleet, label='first', expect_min=60)
    submit(fleet, label='bench', quiet=['gpu'])  # waits for #1, due in an hour
    result = {}
    stubborn = 'import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)'
    t = threading.Thread(target=lambda: result.update(code=cli.main(
        ['run', '--host', 'a', '--expect', '20', '--label', 'short', '--', sys.executable, '-c', stubborn])))
    t.start()
    for _ in range(100):
        short = [j for j in fleet.store.active_jobs() if j.label == 'short']
        if short and short[0].state == 'running':
            break
        time.sleep(0.05)
    assert short[0].deadline == first['started_at'] + 3600
    time.sleep(0.5)  # let the child install its handler
    clock.t = short[0].deadline + 1  # the server's clock passes the deadline; the next heartbeat says so
    t.join(15)
    assert not t.is_alive() and result['code'] == -9  # ignored SIGTERM, so SIGKILL after the grace time
    err = capsys.readouterr().err
    assert 'went ahead of the line and must end within' in err
    assert 'deadline reached' in err and 'was stopped at its deadline' in err
    done = api(url, 'GET', f'/v1/jobs/{short[0].id}')[1]
    assert done['state'] == 'done' and done['note'] == 'stopped at its deadline'


def test_run_without_expect_waits_its_turn(env):
    fleet, _, _ = env
    submit(fleet, label='first', expect_min=60)
    submit(fleet, label='bench', quiet=['gpu'])
    job = fleet.submit({'label': 'blind', 'owner': 'me', 'hosts': ['a'], 'kind': 'run'})
    assert job['state'] == 'queued' and 'with --expect it may go first' in job['reason']
    job = fleet.submit({'label': 'long', 'owner': 'me', 'hosts': ['a'], 'kind': 'run', 'expect_min': 50})
    assert job['state'] == 'queued'


def test_time_parsing():
    assert cli.parse_duration('90') == 5400 and cli.parse_duration('1h30m') == 5400
    assert cli.parse_duration('2h') == 7200 and cli.parse_duration('45s') == 45
    base = time.mktime((2026, 10, 21, 21, 0, 0, 0, 0, -1))
    assert cli.parse_at('21:30', base) == base + 1800
    assert cli.parse_at('20:30', base) == base + 23.5 * 3600  # already past: tomorrow
    assert cli.parse_at('tomorrow 21:00', base) == base + 86400


def test_probe_loop_runs_a_pass_when_a_reservation_comes_due(env, monkeypatch):
    fleet, clock, _ = env
    monkeypatch.setattr(fleet, 'probe_now', lambda: None)
    monkeypatch.setattr(fleet, 'probe_fast', lambda: False)
    fleet.cfg.update(probe_interval_s=3600, fast_probe_s=0)  # no probe comes along in time
    monkeypatch.setattr(core, 'RESERVE_MIN_LEAD_S', 0)      # book half a second ahead
    t0, base = time.monotonic(), clock.t
    monkeypatch.setattr(fleet, 'clock', lambda: base + time.monotonic() - t0)  # a clock that runs
    booked = submit(fleet, label='booked', start_in_s=0.5, expect_min=60)
    stop = threading.Event()
    threading.Thread(target=server.probe_loop, args=(fleet, stop), daemon=True).start()
    try:
        assert fleet.get(booked['id'])['state'] == 'queued'
        for _ in range(40):
            if fleet.get(booked['id'])['state'] == 'running':
                break
            time.sleep(0.05)
        assert fleet.get(booked['id'])['state'] == 'running'
    finally:
        stop.set()


# ---- regressions from the adversarial review ---------------------------------------------

def test_back_to_back_reservations_each_start_on_time(env):
    fleet, clock, _ = env
    t0 = clock.t
    r1 = submit(fleet, label='R1', exclusive=True, start_in_s=600, expect_min=60)
    r2 = submit(fleet, label='R2', exclusive=True, start_in_s=4200, expect_min=60)
    clock.t = t0 + 600.05  # the pass always runs a little after the start
    fleet.tick()
    assert fleet.get(r1['id'], seen=True, owner='me')['state'] == 'running'
    clock.t = t0 + 4200.05
    fleet.tick()
    late = fleet.get(r2['id'])
    assert late['state'] == 'queued' and 'held exclusively by #' in late['reason']
    assert any(f'overrunning into reservation #{r2["id"]}' in f for f in fleet.get(r1['id'])['flags'])
    fleet.release(r1['id'], {'owner': 'me'})
    assert fleet.get(r2['id'])['state'] == 'running'


def test_back_to_back_run_at_reservation_starts_on_time(env):
    fleet, clock, _ = env
    t0 = clock.t
    r1 = submit(fleet, label='R1', kind='run', exclusive=True, start_in_s=600, expect_min=60)
    submit(fleet, label='R2', exclusive=True, start_in_s=4200, expect_min=60)
    clock.t = t0 + 601
    fleet.heartbeat(r1['id'])
    fleet.tick()
    started = fleet.get(r1['id'])
    assert started['state'] == 'running' and started['deadline'] is None


def test_a_start_under_a_minute_ahead_is_an_ordinary_queued_job(env):
    fleet, _, _ = env
    submit(fleet, label='R', mem_gb=10, expect_min=60)
    head = submit(fleet, label='A', exclusive=True, expect_min=60)
    now = submit(fleet, label='X', start_in_s=0, expect_min=30)
    assert now['start_at'] is None and now['state'] == 'queued'
    assert now['reason'] == f'in line behind #{head["id"]} "A" (me) on a'


def test_a_booking_cannot_land_on_the_window_of_a_job_waiting_in_line(env, capsys):
    fleet, _, url = env
    submit(fleet, label='R', mem_gb=10, expect_min=60)
    head = submit(fleet, label='A', exclusive=True, expect_min=60)  # estimated 60 to 120 min from now
    code, body = api(url, 'POST', '/v1/jobs', {'label': 'chain', 'owner': 'me', 'hosts': ['a'],
                                               'start_in_s': 31 * 60, 'expect_min': 30})
    assert code == 409 and f'collides with #{head["id"]} "A" (me) (waiting in line)' in body['error']
    assert body['conflicts'] == [head['id']]
    # Before the head's window, or after it, a booking fits.
    assert submit(fleet, label='before', start_in_s=10 * 60, expect_min=30)['start_at'] is not None
    assert submit(fleet, label='after', start_in_s=121 * 60, expect_min=30)['start_at'] is not None


def test_deadline_kill_leaves_no_descendant(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, 'STOP_GRACE_S', 0.5)
    pidfile = tmp_path / 'pid'
    script = f'bash -c \'trap "" TERM; echo $$ > {pidfile}; sleep 30\' & wait'
    cli.run_one(['bash', '-c', script], None, cli.Deadline({'deadline_in_s': 0.5}))
    pid = int(pidfile.read_text())
    time.sleep(0.2)
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


def test_run_without_a_deadline_stays_in_the_terminal_session():
    code = cli.run_one([sys.executable, '-c', 'import os, sys; sys.exit(0 if os.getsid(0) == %d else 1)'
                        % os.getsid(0)], None)
    assert code == 0


def test_deadline_is_cleared_once_nothing_it_went_ahead_of_is_waiting(env):
    fleet, clock, _ = env
    submit(fleet, label='R', mem_gb=10, expect_min=60)
    head = submit(fleet, label='A', exclusive=True, expect_min=60)
    short = fleet.submit({'label': 'S', 'owner': 'me', 'hosts': ['a'], 'kind': 'run', 'mem_gb': 5,
                          'expect_min': 20})
    assert short['state'] == 'running' and short['deadline_in_s'] == 3600
    later = submit(fleet, label='later', exclusive=True)  # queued after S started: not protected by it
    fleet.release(head['id'], {'owner': 'me'})            # the job S went ahead of leaves the line
    clock.t += 600
    fleet.heartbeat(short['id'])
    fleet.tick()
    got = fleet.get(short['id'])
    assert got['deadline'] is None and got['deadline_in_s'] is None and 'deadline-cleared' in kinds(fleet)
    assert fleet.get(later['id'])['state'] == 'queued'
    # The client takes the server's latest answer, including no deadline at all.
    d = cli.Deadline({'deadline_in_s': 5})
    d.update({'deadline_in_s': None})
    assert d.at is None


def test_lapse_leaves_no_restore_so_users_of_the_service_go_ahead(env):
    fleet, clock, _ = env
    r = submit(fleet, label='booked', start_in_s=60, expect_min=60, takes_down=['a-llm'])
    clock.t += 60
    fleet.tick()
    clock.t += 901
    fleet.tick()
    assert fleet.get(r['id'])['state'] == 'cancelled' and fleet.store.restores() == {}
    assert submit(fleet, label='user', uses=['a-llm'])['state'] == 'running'


def test_only_the_owner_looking_claims_a_reservation(env):
    fleet, clock, url = env
    r = submit(fleet, label='booked', start_in_s=60, expect_min=60)
    clock.t += 60
    fleet.tick()
    api(url, 'GET', f'/v1/jobs/{r["id"]}?owner=someone')  # fleetq show by someone else
    api(url, 'POST', f'/v1/jobs/{r["id"]}/heartbeat', {'owner': 'someone'})
    assert cli.main(['show', str(r['id']), '--owner', 'someone']) == 0
    assert any(f.startswith('no-show') for f in fleet.get(r['id'])['flags'])
    api(url, 'GET', f'/v1/jobs/{r["id"]}?wait=0&owner=me')  # the owner's long-poll claims it
    assert not any(f.startswith('no-show') for f in fleet.get(r['id'])['flags'])
    clock.t += 1000
    fleet.tick()
    assert fleet.get(r['id'])['state'] == 'running'


def test_ctrl_c_reaches_a_command_in_its_own_session(tmp_path):
    # A deadline job's command has its own session, so the terminal's Ctrl-C reaches only
    # fleetq; fleetq passes it on to the command's group.
    child = ('import signal, sys, time; signal.signal(signal.SIGINT, lambda *a: sys.exit(7)); '
             f'open({str(tmp_path / "ready")!r}, "w").close(); time.sleep(30)')
    driver = ('import sys; from fleetq import cli; '
              f'sys.exit(cli.run_one([sys.executable, "-c", {child!r}], None, cli.Deadline({{"deadline_in_s": 60}})))')
    proc = subprocess.Popen([sys.executable, '-c', driver])
    for _ in range(100):
        if (tmp_path / 'ready').exists():
            break
        time.sleep(0.05)
    proc.send_signal(signal.SIGINT)
    assert proc.wait(10) == 7


def test_fleetq_wait_by_the_owner_claims_a_reservation_that_already_started(env):
    fleet, clock, _ = env
    r = submit(fleet, label='booked', owner='tester', start_in_s=60, expect_min=60)
    clock.t += 60
    fleet.tick()
    assert cli.main(['wait', str(r['id'])]) == 0
    clock.t += 901
    fleet.tick()
    assert fleet.get(r['id'])['state'] == 'running'


def test_an_older_client_without_owner_still_claims_a_reservation(env):
    fleet, clock, url = env
    r = submit(fleet, label='booked', start_in_s=60, expect_min=60)
    clock.t += 60
    fleet.tick()
    # An older CLI's `fleetq wait` long-polls without an owner; its covers() GET is plain.
    api(url, 'GET', f'/v1/jobs/{r["id"]}?wait=0&state=queued')
    clock.t += 1000
    fleet.tick()
    assert fleet.get(r['id'])['state'] == 'running'
    r2 = submit(fleet, label='booked2', hosts=['b'], start_in_s=60, expect_min=60)
    clock.t += 60
    fleet.tick()
    api(url, 'POST', f'/v1/jobs/{r2["id"]}/heartbeat', {})  # an older client's heartbeat
    clock.t += 1000
    fleet.tick()
    assert fleet.get(r2['id'])['state'] == 'running'


def driver(tmp_path, command, deadline_s=None, grace=1, before=''):
    """A fleetq stand-in in a session of its own: run_one(command), then write how it returned.
    The command goes in the environment, so only the command itself matches its marker."""
    dl = f'cli.Deadline({{"deadline_in_s": {deadline_s}}})' if deadline_s else 'None'
    code = (f'import json, os, sys; from fleetq import cli; cli.STOP_GRACE_S = {grace}; {before}\n'
            f'code = cli.run_one(json.loads(os.environ["TEST_CMD"]), None, {dl})\n'
            f'open({str(tmp_path / "returned")!r}, "w").write(str(code))')
    env = {**os.environ, 'TEST_CMD': json.dumps(command)}
    proc = subprocess.Popen([sys.executable, '-c', code], start_new_session=True, env=env)
    time.sleep(0.5)  # fleetq starts the command, then its watchdog
    return proc


def running(marker):
    return subprocess.run(['pgrep', '-f', marker], capture_output=True, text=True).stdout.split()


def wait_for(cond, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.05)
    return False


@pytest.mark.parametrize('deadline_s', [None, 600], ids=['plain', 'deadline'])
def test_command_dies_with_fleetq_even_when_fleetq_is_killed(tmp_path, deadline_s):
    # A supervisor's group kill (timeout -s KILL, an agent harness) takes fleetq down hard.
    marker = f'sleep 131.{1 if deadline_s else 2}'
    proc = driver(tmp_path, ['sh', '-c', marker], deadline_s)
    try:
        assert wait_for(lambda: running(marker))
        os.killpg(proc.pid, signal.SIGKILL)
        assert proc.wait(5) == -signal.SIGKILL
        assert wait_for(lambda: not running(marker)), 'the command outlived fleetq'
    finally:
        for pid in running(marker):
            os.kill(int(pid), signal.SIGKILL)


@pytest.mark.parametrize('deadline_s', [None, 600], ids=['plain', 'deadline'])
def test_hangup_stops_the_command(tmp_path, deadline_s):
    # A closed terminal or ssh session hangs up fleetq's process group.
    marker = f'sleep 132.{1 if deadline_s else 2}'
    proc = driver(tmp_path, ['sh', '-c', marker], deadline_s)
    try:
        assert wait_for(lambda: running(marker))
        os.killpg(proc.pid, signal.SIGHUP)
        code = proc.wait(5)
        assert wait_for(lambda: not running(marker))
        if deadline_s:  # fleetq passes the hangup on, sees the command end, and carries on (to release)
            assert code == 0 and (tmp_path / 'returned').read_text() == str(-signal.SIGHUP)
        else:           # as before: the hangup ends fleetq and the command together
            assert code == -signal.SIGHUP and not (tmp_path / 'returned').exists()
    finally:
        for pid in running(marker):
            os.kill(int(pid), signal.SIGKILL)


def test_second_ctrl_c_while_draining_kills_the_leftovers_and_returns(tmp_path):
    marker = 'sleep 133.3'
    # The command exits at once, leaving a process that ignores SIGTERM and SIGINT; draining
    # it would take the 30 s grace time. Ctrl-C comes a second in.
    before = 'import os, signal, threading; threading.Timer(1.0, os.kill, (os.getpid(), signal.SIGINT)).start()'
    proc = driver(tmp_path, ['sh', '-c', f'(trap "" TERM INT; {marker}) & exit 0'], 600, grace=30, before=before)
    try:
        assert proc.wait(10) == 0 and (tmp_path / 'returned').read_text() == '0'
        assert wait_for(lambda: not running(marker))
    finally:
        for pid in running(marker):
            os.kill(int(pid), signal.SIGKILL)
