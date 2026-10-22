"""Outbound notifications: a local http.server captures what fleetq posts; the clock is fake."""
import json
import socket
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from fleetq import cli, notify
from fleetq.core import Observation
from fleetq.server import ApiError, Fleet, main

BASE = """
[server]
db = "{db}"
stale_heartbeat_s = 180
job_notify = true
job_notify_hosts = ["127.0.0.1"]
[[host]]
name = "a"
machine = "ma"
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
roles = ["chat-a"]
mem_gb = 40
"""
UP = {'ma': True, 'mb': True}


class Clock:
    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t


class Hook:
    """A webhook endpoint that records every POST it gets and answers with a chosen status."""

    def __init__(self, status=200, delay=0.0):
        self.posts, self.status, self.delay = [], status, delay
        hook = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get('Content-Length') or 0))
                hook.posts.append({'path': self.path, 'headers': dict(self.headers), 'body': body.decode()})
                time.sleep(hook.delay)
                self.send_response(hook.status)
                self.send_header('Content-Length', '0')
                self.end_headers()

            def do_GET(self):  # a followed redirect arrives as a GET; record it where tests will see it
                hook.posts.append({'path': self.path, 'headers': dict(self.headers), 'body': '', 'method': 'GET'})
                self.send_response(200)
                self.send_header('Content-Length', '0')
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f'http://127.0.0.1:{self.server.server_address[1]}'

    def kinds(self):
        return [json.loads(p['body'])['kind'] for p in self.posts]


@pytest.fixture
def hook(monkeypatch):
    monkeypatch.setenv('no_proxy', '*')
    h = Hook()
    yield h
    h.server.shutdown()


def make_fleet(tmp_path, extra='', **notifier):
    (tmp_path / 'fleet.toml').write_text(BASE.format(db=tmp_path / 'q.db') + extra)
    clock = Clock()
    fleet = Fleet(tmp_path / 'fleet.toml', clock=clock)
    fleet.notifier.backoff = (0,)
    for k, v in notifier.items():
        setattr(fleet.notifier, k, v)
    fleet.apply_probe({'a-llm': Observation('up', ['m'], 0, at=clock.t)}, UP, {}, clock.t)
    return fleet, clock


def roster(url, body=''):
    return f'[[notify]]\nurl = "{url}"\n{body}\n'


def lease(fleet, **kw):
    return fleet.submit({'label': 'bench', 'owner': 'alice', 'hosts': ['b'], **kw})


def test_started_job_is_posted_as_json_to_the_roster_url(tmp_path, hook):
    fleet, clock = make_fleet(tmp_path, roster(hook.url + '/hook'))
    job = lease(fleet)
    fleet.notifier.flush()
    assert len(hook.posts) == 1 and hook.posts[0]['path'] == '/hook'
    assert hook.posts[0]['headers']['Content-Type'] == 'application/json'
    event = json.loads(hook.posts[0]['body'])
    assert event == {'kind': 'started', 'text': f'#{job["id"]} started on b: bench', 'job': job['id'],
                     'service': None, 'at': clock.t}


def test_queued_and_other_chatty_events_are_not_sent_by_default(tmp_path, hook):
    fleet, _ = make_fleet(tmp_path, roster(hook.url))
    lease(fleet, exclusive=True)
    waiting = lease(fleet, exclusive=True)
    assert waiting['state'] == 'queued'
    fleet.notifier.flush()
    assert set(hook.kinds()) == {'started'}  # 'queued' is in the log but never posted unless asked for


def test_kinds_and_owners_filter_a_roster_entry(tmp_path, hook):
    extra = roster(hook.url + '/ci', 'kinds = ["done"]\nowners = ["ci-*"]') + roster(hook.url + '/alice', 'owners = "alice"')
    fleet, _ = make_fleet(tmp_path, extra)
    mine = lease(fleet, owner='alice')
    ci = lease(fleet, owner='ci-nightly')
    fleet.release(mine['id'], {'owner': 'alice'})
    fleet.release(ci['id'], {'owner': 'ci-nightly'})
    fleet.notifier.flush()
    got = sorted((p['path'], json.loads(p['body'])['kind'], json.loads(p['body'])['job']) for p in hook.posts)
    assert got == sorted([('/alice', 'started', mine['id']), ('/alice', 'done', mine['id']),
                          ('/ci', 'done', ci['id'])])


def test_owner_filter_skips_events_that_belong_to_no_job(tmp_path, hook):
    fleet, clock = make_fleet(tmp_path, roster(hook.url, 'kinds = ["wedged"]\nowners = ["*"]'))
    wedge(fleet, clock)
    fleet.notifier.flush()
    assert hook.posts == []


def test_per_job_notify_url_hears_only_about_its_own_job(tmp_path, hook):
    fleet, clock = make_fleet(tmp_path)
    mine = lease(fleet, notify=hook.url + '/mine', expect_min=30)
    lease(fleet, owner='bob')
    clock.t += 31 * 60
    fleet.tick()
    fleet.release(mine['id'], {'owner': 'alice'})
    fleet.notifier.flush()
    assert hook.kinds() == ['started', 'overdue', 'done']
    assert {p['path'] for p in hook.posts} == {'/mine'}


def test_a_url_in_both_roster_and_job_gets_one_post(tmp_path, hook):
    fleet, _ = make_fleet(tmp_path, roster(hook.url))
    lease(fleet, notify=hook.url)
    fleet.notifier.flush()
    assert hook.kinds() == ['started']


def test_job_notify_url_is_validated_and_hidden_from_readers(tmp_path, hook):
    fleet, _ = make_fleet(tmp_path)
    with pytest.raises(ApiError) as bad:
        lease(fleet, notify='ftp://x/y')
    assert bad.value.code == 400 and '--notify' in str(bad.value)
    job = lease(fleet, notify=hook.url + '/secret-topic')
    assert job['notify'] is True
    assert 'secret-topic' not in json.dumps(fleet.status())
    fleet.cfg['job_notify'] = False
    with pytest.raises(ApiError):
        lease(fleet, notify=hook.url)
    fleet.notifier.flush()
    hook.posts.clear()
    fleet.release(job['id'], {'owner': 'alice'})  # the job's stored URL is not used once job_notify is off
    fleet.notifier.flush()
    assert hook.posts == []


def test_cli_notify_flag_reaches_the_job(tmp_path, hook, monkeypatch):
    from http.server import ThreadingHTTPServer as Srv
    from fleetq.server import make_handler
    fleet, _ = make_fleet(tmp_path)
    srv = Srv(('127.0.0.1', 0), make_handler(fleet, 'tok-0123456789abcdef0123'))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    monkeypatch.setenv('FLEETQ_URL', f'http://127.0.0.1:{srv.server_address[1]}')
    monkeypatch.setenv('FLEETQ_TOKEN', 'tok-0123456789abcdef0123')
    monkeypatch.setenv('FLEETQ_OWNER', 'tester')
    try:
        assert cli.main(['acquire', '--host', 'b', '--label', 'via cli', '--notify', hook.url + '/cli']) == 0
        fleet.notifier.flush()
    finally:
        srv.shutdown()
    assert [p['path'] for p in hook.posts] == ['/cli'] and hook.kinds() == ['started']


def serve_cli(fleet, monkeypatch):
    from fleetq.server import make_handler
    srv = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(fleet, 'tok-0123456789abcdef0123'))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    monkeypatch.setenv('FLEETQ_URL', f'http://127.0.0.1:{srv.server_address[1]}')
    monkeypatch.setenv('FLEETQ_TOKEN', 'tok-0123456789abcdef0123')
    monkeypatch.setenv('FLEETQ_OWNER', 'tester')
    monkeypatch.setattr(cli, 'POLL_S', 0.05)
    return srv


def test_cli_notify_on_run_and_reserve(tmp_path, hook, monkeypatch):
    import sys
    fleet, clock = make_fleet(tmp_path)
    srv = serve_cli(fleet, monkeypatch)
    try:
        assert cli.main(['run', '--host', 'b', '--label', 'ran', '--notify', hook.url + '/run', '--',
                         sys.executable, '-c', 'pass']) == 0
        fleet.notifier.flush()
        assert [p['path'] for p in hook.posts] == ['/run', '/run'] and hook.kinds() == ['started', 'done']
        assert cli.main(['reserve', '--host', 'b', '--label', 'booked', '--in', '1h', '--for', '30m',
                         '--notify', hook.url + '/res']) == 0
        job = next(j for j in fleet.store.active_jobs() if j.label == 'booked')
        assert job.notify == hook.url + '/res'
        fleet.notifier.flush()
        assert len(hook.posts) == 2  # 'reserved' is not in the default kinds
        clock.t += 3601
        fleet.tick()
        fleet.notifier.flush()
    finally:
        srv.shutdown()
    assert [(p['path'], k) for p, k in zip(hook.posts[2:], hook.kinds()[2:])] == [('/res', 'started')]


def test_ntfy_gets_plain_text_with_an_ascii_title(tmp_path, hook):
    fleet, _ = make_fleet(tmp_path, roster(hook.url + '/topic', 'format = "ntfy"'))
    job = lease(fleet, label='Überlauf – bench')
    fleet.notifier.flush()
    post = hook.posts[0]
    assert post['headers']['Title'] == f'fleetq started #{job["id"]}'
    assert post['headers']['Content-Type'].startswith('text/plain')
    assert post['body'] == f'#{job["id"]} started on b: Überlauf – bench'


def test_ntfy_sh_urls_are_recognised_without_a_format():
    rules = notify.parse_rules([{'url': 'https://ntfy.sh/topic'}, {'url': 'https://hooks.example.com/x'},
                                {'url': 'https://ntfy.example.com/x', 'format': 'ntfy'}])
    assert [r.fmt for r in rules] == ['ntfy', 'json', 'ntfy']


def wedge(fleet, clock):
    for _ in range(3):
        fleet.apply_probe({'a-llm': Observation('up', ['m'], 1, at=clock.t, progress=5)}, {}, {}, clock.t)
        clock.t += 100


def test_flag_events_fire_once_per_transition_not_every_cycle(tmp_path, hook):
    fleet, clock = make_fleet(tmp_path, roster(hook.url, 'kinds = ["overdue", "stale", "wedged", "not_restored"]'))
    lease(fleet, expect_min=5)
    run = fleet.submit({'label': 'long', 'owner': 'alice', 'hosts': ['a'], 'kind': 'run'})
    wedge(fleet, clock)                      # 200 s of frozen progress: wedged; run job's heartbeat 300 s old: stale
    for _ in range(5):
        clock.t += 60
        fleet.apply_probe({'a-llm': Observation('up', ['m'], 1, at=clock.t, progress=5)}, {}, {}, clock.t)
        fleet.tick()
    fleet.notifier.flush()
    assert sorted(hook.kinds()) == ['overdue', 'stale', 'wedged']
    # the client comes back, then vanishes again: that is a second stale
    fleet.heartbeat(run['id'])
    clock.t += 200
    fleet.tick()
    fleet.notifier.flush()
    assert sorted(hook.kinds()) == ['overdue', 'stale', 'stale', 'wedged']
    # a fresh Fleet on the same database does not announce the overdue/stale states again
    again = Fleet(tmp_path / 'fleet.toml', clock=clock)
    again.notifier.send = lambda *a: hook.posts.append('resent')
    again.tick()
    again.notifier.flush()
    assert 'resent' not in hook.posts


def test_not_restored_is_announced_once_to_the_roster_and_the_takedown_job(tmp_path, hook):
    fleet, clock = make_fleet(tmp_path, roster(hook.url + '/roster', 'kinds = ["not_restored"]'))
    job = fleet.submit({'label': 'swap', 'owner': 'alice', 'takes_down': ['chat-a'], 'notify': hook.url + '/job'})
    fleet.release(job['id'], {'owner': 'alice'})
    clock.t += 700
    for _ in range(3):
        fleet.tick()
        clock.t += 30
    fleet.notifier.flush()
    assert sorted((p['path'], json.loads(p['body'])['kind']) for p in hook.posts) == [
        ('/job', 'done'), ('/job', 'not_restored'), ('/job', 'started'), ('/roster', 'not_restored')]
    not_restored = next(json.loads(p['body']) for p in hook.posts if json.loads(p['body'])['kind'] == 'not_restored')
    assert not_restored['service'] == 'a-llm' and not_restored['job'] == job['id']
    fleet.apply_probe({'a-llm': Observation('up', ['m'], 0, at=clock.t)}, {}, {}, clock.t)
    fleet.notifier.flush()
    assert hook.kinds().count('restored') == 1  # restored goes to the job's own URL


def test_overrun_unclaimed_and_lapsed_are_posted(tmp_path, hook):
    fleet, clock = make_fleet(tmp_path, roster(hook.url, 'kinds = ["unclaimed", "overrun", "lapsed"]'))
    fleet.cfg.update(unclaimed_after_s=100, noshow_after_s=300)
    holder = lease(fleet, hosts=['a'], exclusive=True, expect_min=60)
    waiter = lease(fleet, hosts=['a'], exclusive=True)  # queued; its client looked once, at submit
    booked = fleet.submit({'label': 'booked', 'owner': 'carol', 'hosts': ['a'], 'start_in_s': 60,
                           'expect_min': 30})
    clock.t += 61
    fleet.tick()                                         # the reservation is due and the holder is in its way
    fleet.release(holder['id'], {'owner': 'alice'})      # the reservation goes first
    clock.t += 400
    fleet.tick()                                         # nobody claims it: it lapses
    fleet.notifier.flush()
    got = [(json.loads(p['body'])['kind'], json.loads(p['body'])['job']) for p in hook.posts]
    assert ('overrun', holder['id']) in got and ('lapsed', booked['id']) in got
    assert [k for k, _ in got].count('overrun') == 1


def test_unclaimed_lease_is_posted_once(tmp_path, hook):
    fleet, clock = make_fleet(tmp_path, roster(hook.url, 'kinds = ["unclaimed"]'))
    fleet.cfg['unclaimed_after_s'] = 100
    holder = lease(fleet, hosts=['a'], exclusive=True)
    waiter = lease(fleet, hosts=['a'], exclusive=True)
    clock.t += 10
    fleet.release(holder['id'], {'owner': 'alice'})      # the waiter starts, but nobody is watching
    for _ in range(4):
        clock.t += 60
        fleet.tick()
    fleet.notifier.flush()
    assert [(json.loads(p['body'])['kind'], json.loads(p['body'])['job']) for p in hook.posts] == [
        ('unclaimed', waiter['id'])]


def test_failing_webhook_is_tried_three_times_then_dropped_without_slowing_the_request(tmp_path):
    bad = Hook(status=500)
    try:
        fleet, _ = make_fleet(tmp_path, roster(bad.url))
        began = time.monotonic()
        lease(fleet)
        assert time.monotonic() - began < 1
        fleet.notifier.flush()
        assert len(bad.posts) == 3
        lease(fleet)
        fleet.notifier.flush()
        assert len(bad.posts) == 6  # the next event gets its own three tries
    finally:
        bad.server.shutdown()


def test_slow_webhook_times_out_and_never_holds_up_step(tmp_path, monkeypatch):
    monkeypatch.setattr(notify, 'TIMEOUT_S', 0.2)
    slow = Hook(delay=1.0)
    try:
        fleet, clock = make_fleet(tmp_path, roster(slow.url))
        began = time.monotonic()
        lease(fleet)
        clock.t += 60
        fleet.tick()
        assert time.monotonic() - began < 0.5
        fleet.notifier.flush()
        assert len(slow.posts) == 3
    finally:
        slow.server.shutdown()


def test_unreachable_webhook_does_not_fail_requests(tmp_path):
    fleet, _ = make_fleet(tmp_path, roster('http://127.0.0.1:1/hook'))
    assert lease(fleet)['state'] == 'running'
    fleet.notifier.flush()


def test_events_of_a_rolled_back_transaction_are_not_posted(tmp_path, hook):
    fleet, _ = make_fleet(tmp_path, roster(hook.url))
    with pytest.raises(RuntimeError):
        with fleet.store.transaction():
            fleet.store.event(fleet.clock(), 'started', '#9 started', 9)
            raise RuntimeError('boom')
    fleet.notifier.flush()
    assert hook.posts == []
    with fleet.store.transaction():
        fleet.store.event(fleet.clock(), 'done', '#9 finished', 9)
    fleet.notifier.flush()
    assert hook.kinds() == ['done']


def test_full_queue_drops_instead_of_blocking(capsys):
    release = threading.Event()
    n = notify.Notifier([notify.Rule('http://x/y', 'json', frozenset(['started']))], send=lambda *a: release.wait(5),
                        max_queue=2)
    began = time.monotonic()
    for i in range(10):
        n.notify({'kind': 'started', 'text': str(i), 'job': i, 'service': None, 'at': 0.0})
    assert time.monotonic() - began < 0.5
    assert 'queue full' in capsys.readouterr().err
    release.set()
    n.flush()


def test_check_validates_notify_entries(tmp_path, capsys):
    cfg = tmp_path / 'fleet.toml'
    cfg.write_text(BASE.format(db=tmp_path / 'q.db') + roster('https://ntfy.sh/t') + roster('http://h/x', 'kinds = ["done"]'))
    assert main(['--check', '--config', str(cfg)]) == 0
    assert 'ok: 2 hosts on 2 machines, 1 services, 2 notify targets' in capsys.readouterr().out
    for body, msg in [('', 'url must be an http'), ('url = "ntfy.sh/t"', 'url must be an http'),
                      ('url = "http://h"\nkinds = ["finished"]', 'unknown kind finished'),
                      ('url = "http://h"\nkind = ["done"]', 'unknown key kind'),
                      ('url = "http://h"\nkinds = []', 'non-empty list'),
                      ('url = "http://h"\nowners = [1]', 'owners must be a list'),
                      ('url = "http://h"\nformat = "xml"', 'format must be one of')]:
        cfg.write_text(BASE.format(db=tmp_path / 'q.db') + '[[notify]]\n' + body + '\n')
        with pytest.raises(ValueError, match=msg):
            main(['--check', '--config', str(cfg)])


# ---- review fixes: SSRF, isolation, restarts, robustness ---------------------------------------------

class Redirector:
    """Answers every POST with a redirect to `target`, counting them."""

    def __init__(self, target):
        self.hits = 0
        redirector = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                redirector.hits += 1
                self.rfile.read(int(self.headers['Content-Length']))
                self.send_response(302)
                self.send_header('Location', target)
                self.send_header('Content-Length', '0')
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f'http://127.0.0.1:{self.server.server_address[1]}/'


def test_redirects_are_not_followed(tmp_path, hook):
    inner = Hook()
    red = Redirector(inner.url + '/cgi-bin/reboot?confirm=1')
    try:
        fleet, _ = make_fleet(tmp_path, roster(red.url))
        lease(fleet, notify=red.url)
        fleet.notifier.flush()
        assert red.hits == 3                # the redirect was seen and treated as a failure: three tries
        assert inner.posts == []            # neither the POST nor the GET a client would turn it into
    finally:
        red.server.shutdown()
        inner.server.shutdown()


def test_environment_proxies_are_ignored(tmp_path, hook, monkeypatch):
    """With a proxy in the environment, a notification must still go straight to the host named."""
    monkeypatch.delenv('no_proxy', raising=False)
    monkeypatch.delenv('NO_PROXY', raising=False)
    monkeypatch.setattr(urllib.request, '_opener', None)  # urlopen builds its opener, proxies included, once
    monkeypatch.setenv('http_proxy', hook.url)  # this stands in for the proxy: it records what reaches it
    fleet, _ = make_fleet(tmp_path, roster('http://notify.example.invalid/hook'))
    lease(fleet)
    fleet.notifier.flush()
    assert hook.posts == []


def test_job_urls_are_off_by_default_and_limited_to_the_allowlist(tmp_path):
    cfg = tmp_path / 'fleet.toml'
    cfg.write_text(BASE.format(db=tmp_path / 'q.db').replace('job_notify = true\njob_notify_hosts = ["127.0.0.1"]\n', ''))
    fleet = Fleet(cfg, clock=Clock())
    assert fleet.cfg['job_notify'] is False and fleet.cfg['job_notify_hosts'] == ['ntfy.sh']
    with pytest.raises(ApiError, match='job_notify is off'):
        lease(fleet, notify='https://ntfy.sh/topic')
    fleet.cfg['job_notify'] = True
    assert lease(fleet, notify='https://ntfy.sh/topic')['notify'] is True
    assert lease(fleet, notify='https://NTFY.sh/topic')['notify'] is True
    for url in ('http://10.0.0.1/cgi-bin/reboot', 'http://169.254.169.254/latest/meta-data/',
                'https://ntfy.sh.evil.example/x', 'http://127.0.0.1:9/', 'file:///etc/passwd', 'gopher://x/'):
        with pytest.raises(ApiError) as e:
            lease(fleet, notify=url)
        assert e.value.code == 400


def test_job_notify_must_be_a_real_boolean(tmp_path):
    cfg = tmp_path / 'fleet.toml'
    for value, msg in [('"false"', 'job_notify must be true or false'), ('0', 'job_notify must be true or false')]:
        cfg.write_text(BASE.format(db=tmp_path / 'q.db').replace('job_notify = true', f'job_notify = {value}'))
        with pytest.raises(ValueError, match=msg):
            main(['--check', '--config', str(cfg)])
    cfg.write_text(BASE.format(db=tmp_path / 'q.db').replace('["127.0.0.1"]', '"ntfy.sh"'))
    with pytest.raises(ValueError, match='job_notify_hosts must be a list'):
        main(['--check', '--config', str(cfg)])


@pytest.mark.parametrize('url', ['http://h:99999/', 'http://h:abc/', 'http://h/a b', 'http://h/\r\nX: y', ' http://h/',
                                 'http://[::1', 'ftp://h/', 'http://', '', 'http://h/\x00'])
def test_urls_with_bad_ports_whitespace_or_control_characters_are_refused(url):
    with pytest.raises(ValueError):
        notify.check_url(url, 'x')


def test_valid_urls_pass_the_check():
    for url in ('http://h/', 'https://ntfy.sh/topic', 'HTTP://h:8080/a?b=c', 'http://[::1]:8080/x'):
        assert notify.check_url(url, 'x') == url


def test_new_event_kinds_can_be_asked_for():
    assert notify.parse_rules([{'url': 'http://h/', 'kinds': ['deadline-cleared', 'wedge-cleared']}])


def drip_server():
    """A server that accepts a connection and answers one byte every 0.2 s, never finishing."""
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    sock.listen(50)

    def serve():
        while True:
            conn, _ = sock.accept()

            def drip(conn=conn):
                try:
                    conn.recv(65536)
                    while True:
                        conn.send(b'H')
                        time.sleep(0.2)
                except OSError:
                    pass

            threading.Thread(target=drip, daemon=True).start()

    threading.Thread(target=serve, daemon=True).start()
    return f'http://127.0.0.1:{sock.getsockname()[1]}/x'


def test_a_dripping_job_url_cannot_hold_up_the_rosters_alerts(hook):
    event = lambda k, j: {'at': 0, 'kind': k, 'text': 't', 'job': j, 'service': None}
    n = notify.Notifier(notify.parse_rules([{'url': hook.url + '/roster'}]), backoff=(0,), cap_s=2, max_queue=500)
    drip = drip_server()
    n.notify(event('started', 1), 'mallory', drip)
    for job in range(2, 102):
        n.notify(event('started', job), 'alice')
    began = time.monotonic()
    n.lanes['roster'].queue.join()
    assert time.monotonic() - began < 2
    assert len(hook.posts) == 101  # while the job lane is still busy with the dripping URL
    assert n.lanes['job'].queue.unfinished_tasks == 1
    n.flush()  # three attempts, each cut off after cap_s
    assert time.monotonic() - began < 10


def test_a_send_that_never_returns_is_cut_off_and_counts_as_a_failure(capsys):
    stuck = threading.Event()
    n = notify.Notifier([notify.Rule('http://h/', 'json', frozenset(['started']))], send=lambda *a: stuck.wait(30),
                        backoff=(0,), cap_s=0.1)
    began = time.monotonic()
    n.notify({'at': 0, 'kind': 'started', 'text': 't', 'job': 1, 'service': None})
    n.flush()
    stuck.set()
    assert time.monotonic() - began < 2
    err = capsys.readouterr().err
    assert err.count('no answer within') == 3 and 'gave up' in err


def test_a_url_that_keeps_failing_is_left_alone_for_a_while_then_tried_again():
    bad = Hook(status=500)
    now = [1000.0]
    try:
        n = notify.Notifier([notify.Rule(bad.url, 'json', frozenset(['started']))], backoff=(0,), clock=lambda: now[0])
        ev = lambda j: {'at': 0, 'kind': 'started', 'text': 't', 'job': j, 'service': None}
        for j in range(1, 4):
            n.notify(ev(j))
            n.flush()
        assert len(bad.posts) == 9                     # three messages, three tries each
        n.notify(ev(4))
        n.flush()
        assert len(bad.posts) == 9                     # quiet now: not even tried
        now[0] += 59
        n.notify(ev(5))
        n.flush()
        assert len(bad.posts) == 9
        now[0] += 2                                    # a minute has passed
        n.notify(ev(6))
        n.flush()
        assert len(bad.posts) == 12                    # tried again, failed again: quiet for longer
        now[0] += 120
        n.notify(ev(7))
        n.flush()
        assert len(bad.posts) == 12
        bad.status = 200
        now[0] += 600
        n.notify(ev(8))
        n.flush()
        assert len(bad.posts) == 13 and not n.failed   # success clears it
    finally:
        bad.server.shutdown()


def test_a_bad_job_url_does_not_cost_the_roster_its_copy(hook):
    n = notify.Notifier(notify.parse_rules([{'url': hook.url + '/roster'}]))
    n.notify({'at': 0, 'kind': 'started', 'text': 't', 'job': 1, 'service': None}, 'alice', 'http://[::1')
    n.flush()
    assert [p['path'] for p in hook.posts] == ['/roster']


def test_roster_alerts_survive_a_job_row_with_garbage_in_its_notify_column(tmp_path, hook):
    fleet, clock = make_fleet(tmp_path, roster(hook.url + '/roster'))
    holder = lease(fleet, exclusive=True)
    waiting = lease(fleet, exclusive=True)
    fleet.store.db.execute("UPDATE jobs SET notify = 'http://[::1' WHERE id = ?", (waiting['id'],))
    fleet.release(holder['id'], {'owner': 'alice'})
    fleet.notifier.flush()
    assert ('started', waiting['id']) in [(json.loads(p['body'])['kind'], json.loads(p['body'])['job']) for p in hook.posts]


def wedge_probe(fleet, clock, in_flight=2, progress=5.0):
    fleet.apply_probe({'a-llm': Observation('up', ['m'], in_flight, at=clock.t, progress=progress)}, UP, {}, clock.t)


def wedge_events(fleet):
    return [e['kind'] for e in reversed(fleet.store.events(200)) if e['kind'] in ('wedged', 'wedge-cleared')]


def test_a_stall_that_goes_on_across_restarts_is_announced_once_and_its_end_once(tmp_path):
    fleet, clock = make_fleet(tmp_path)
    for _ in range(8):
        clock.t += 30
        wedge_probe(fleet, clock)
    assert wedge_events(fleet) == ['wedged']
    for restart in range(2):
        fleet = Fleet(tmp_path / 'fleet.toml', clock=clock)  # a redeploy: observations start over
        for _ in range(8):
            clock.t += 30
            wedge_probe(fleet, clock)
        assert wedge_events(fleet) == ['wedged']
    clock.t += 30
    wedge_probe(fleet, clock, in_flight=0)                   # the stuck request is gone
    clock.t += 30
    wedge_probe(fleet, clock, in_flight=0)
    assert wedge_events(fleet) == ['wedged', 'wedge-cleared']
    for _ in range(8):                                       # a new stall is a new announcement
        clock.t += 30
        wedge_probe(fleet, clock)
    assert wedge_events(fleet) == ['wedged', 'wedge-cleared', 'wedged']


def test_an_engine_that_moves_again_clears_the_wedge(tmp_path):
    fleet, clock = make_fleet(tmp_path)
    for _ in range(8):
        clock.t += 30
        wedge_probe(fleet, clock)
    clock.t += 30
    wedge_probe(fleet, clock, progress=6.0)                  # still busy, but the counter moved
    assert wedge_events(fleet) == ['wedged', 'wedge-cleared']


def test_the_first_check_after_a_restart_does_not_clear_a_wedge(tmp_path):
    fleet, clock = make_fleet(tmp_path)
    for _ in range(8):
        clock.t += 30
        wedge_probe(fleet, clock)
    again = Fleet(tmp_path / 'fleet.toml', clock=clock)
    clock.t += 30
    wedge_probe(again, clock)
    assert wedge_events(again) == ['wedged']


# ---- token_file: Bearer auth for notify targets ---------------------------------------------------------

SECRET = 'tk_s3cr3t-0123456789abcdef'


def token_file(tmp_path, text=SECRET + '\n'):
    f = tmp_path / 'ntfy.token'
    f.write_text(text)
    return f


@pytest.mark.parametrize('fmt', ['json', 'ntfy'])
def test_token_is_sent_as_a_bearer_header_only_for_entries_that_have_one(tmp_path, hook, fmt):
    extra = (roster(hook.url + '/with', f'format = "{fmt}"\ntoken_file = "{token_file(tmp_path)}"')
             + roster(hook.url + '/without', f'format = "{fmt}"'))
    fleet, _ = make_fleet(tmp_path, extra)
    lease(fleet)
    fleet.notifier.flush()
    got = {p['path']: p['headers'].get('Authorization') for p in hook.posts}
    assert got == {'/with': 'Bearer ' + SECRET, '/without': None}


def test_a_jobs_own_url_never_gets_the_rosters_token(tmp_path, hook):
    fleet, _ = make_fleet(tmp_path, roster(hook.url + '/roster', f'token_file = "{token_file(tmp_path)}"'))
    lease(fleet, notify=hook.url + '/job')
    fleet.notifier.flush()
    got = {p['path']: p['headers'].get('Authorization') for p in hook.posts}
    assert got == {'/roster': 'Bearer ' + SECRET, '/job': None}


def test_check_fails_loudly_for_a_missing_empty_or_odd_token_file(tmp_path, capsys):
    cfg = tmp_path / 'fleet.toml'
    good = token_file(tmp_path)
    cfg.write_text(BASE.format(db=tmp_path / 'q.db') + roster('http://h/x', f'token_file = "{good}"'))
    assert main(['--check', '--config', str(cfg)]) == 0
    assert SECRET not in capsys.readouterr().out
    empty = tmp_path / 'empty'
    empty.write_text('\n')
    odd = tmp_path / 'odd'
    odd.write_text('two words\n')
    for body, msg in [(f'token_file = "{tmp_path}/missing"', 'cannot read token_file'),
                      (f'token_file = "{empty}"', 'is empty'), (f'token_file = "{odd}"', 'no spaces or line breaks'),
                      (f'token_file = "{tmp_path}"', 'cannot read token_file'), ('token_file = ""', 'must be a path'),
                      ('token = "abc"', 'not accepted inline')]:
        cfg.write_text(BASE.format(db=tmp_path / 'q.db') + roster('http://h/x', body))
        with pytest.raises(ValueError, match=msg) as err:
            main(['--check', '--config', str(cfg)])
        assert SECRET not in str(err.value)


def test_the_token_never_reaches_logs_or_reprs(tmp_path, capsys):
    bad = Hook(status=401)
    try:
        fleet, _ = make_fleet(tmp_path, roster(bad.url + '/x', f'token_file = "{token_file(tmp_path)}"'))
        lease(fleet)
        fleet.notifier.flush()
        assert len(bad.posts) == 3 and bad.posts[0]['headers']['Authorization'] == 'Bearer ' + SECRET
        assert SECRET not in capsys.readouterr().err
        assert SECRET not in repr(fleet.notifier.rules) and SECRET not in json.dumps(fleet.roster())
        assert SECRET not in json.dumps(fleet.events(50)) and SECRET not in json.dumps(fleet.status())
    finally:
        bad.server.shutdown()


def test_an_error_that_echoes_the_token_is_scrubbed_from_the_log(capsys):
    def leaky(url, fmt, event, token=''):
        raise RuntimeError(f'rejected {token}')

    n = notify.Notifier([notify.Rule('http://h/', 'json', frozenset(['started']), (), SECRET)], send=leaky,
                        backoff=(0,))
    n.notify({'at': 0, 'kind': 'started', 'text': 't', 'job': 1, 'service': None})
    n.flush()
    err = capsys.readouterr().err
    assert 'rejected ***' in err and SECRET not in err
