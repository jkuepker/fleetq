"""fleetq: check the fleet queue, get in line, and run work on the GPU boxes without collisions.

Server address: FLEETQ_URL, else ~/.config/fleetq/url, else http://fleetq:8470.
Token: FLEETQ_TOKEN, else ~/.config/fleetq/token.
Owner (who may release a job): --owner, else FLEETQ_OWNER, else user@host.
Nesting: `fleetq run` sets FLEETQ_JOB for its command. With FLEETQ_JOB set to a running job
that already covers the requested host, `fleetq run` (and tools that use fleetq) run inside
it instead of queueing behind it.
"""
from __future__ import annotations

import argparse
import getpass
import json
import os
import re
from pathlib import Path
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import textwrap
import threading
import time
import urllib.error
from urllib.parse import urlparse, urlunparse
import urllib.request

CONFIG_DIR = Path.home() / '.config' / 'fleetq'
POLL_S, HEARTBEAT_S, OUTAGE_S, STOP_GRACE_S = 5, 30, 120, 30
LONG_POLL_S = 25  # how long one GET /v1/jobs/<id>?wait=N may sit on the server
EXIT_UNREACHABLE, EXIT_TIMEOUT, EXIT_CANCELLED = 2, 3, 4


class Unreachable(Exception):
    pass


class ApiError(Exception):
    def __init__(self, code, message, body=None):
        super().__init__(message)
        self.code = code
        self.body = body or {}


def setting(env, filename, default=None):
    if os.environ.get(env):
        return os.environ[env].strip()
    path = CONFIG_DIR / filename
    if path.exists():
        return path.read_text().strip()
    return default


class Client:
    def __init__(self, url=None, token=None, timeout=15.0):
        self.url = (url or setting('FLEETQ_URL', 'url', 'http://fleetq:8470')).rstrip('/')
        self.token = token or setting('FLEETQ_TOKEN', 'token', '')
        self.timeout = timeout

    def call(self, method, path, body=None, timeout=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.url + path, data=data, method=method,
                                     headers={'Authorization': 'Bearer ' + self.token,
                                              'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            try:
                body = json.loads(exc.read())
            except ValueError:
                body = {}
            raise ApiError(exc.code, body.get('error', str(exc)), body) from None
        except (OSError, ValueError) as exc:
            raise Unreachable(f'fleetq server {self.url} unreachable: {getattr(exc, "reason", exc)}') from None


def default_owner():
    return os.environ.get('FLEETQ_OWNER') or f'{getpass.getuser()}@{socket.gethostname().split(".")[0]}'


def split_list(values):
    return [v.strip() for item in values or [] for v in item.split(',') if v.strip()]


def local_url(url):
    """Loopback URLs mean this machine; name it so the server can place the work."""
    if not url:
        return url
    u = urlparse(url if '://' in url else 'http://' + url)
    if u.hostname in ('127.0.0.1', 'localhost', '::1', '0.0.0.0'):
        me = socket.gethostname().split('.')[0]
        return urlunparse(u._replace(netloc=f'{me}:{u.port}' if u.port else me))
    return url


def parse_mem(values):
    """--mem 30, --mem spark1=100,spark2=90, or both: (GB for every host, {host: GB})."""
    if values is None or isinstance(values, (int, float)):
        return float(values or 0), {}
    default, per_host = 0.0, {}
    for item in split_list(values):
        name, sep, gb = item.rpartition('=')
        try:
            if sep:
                per_host[name.strip()] = float(gb)
            else:
                default = float(item)
        except ValueError:
            raise SystemExit(f'fleetq: --mem takes GB or host=GB, not {item!r}') from None
    return default, per_host


def job_payload(args, kind):
    mem, mem_map = parse_mem(args.mem)
    return {'kind': kind, 'label': args.label, 'owner': args.owner, 'hosts': split_list(args.host),
            'for_url': local_url(getattr(args, 'for_url', None)), 'mem_map': mem_map,
            'mem_gb': mem, 'exclusive': args.exclusive, 'uses': split_list(args.uses),
            'takes_down': split_list(args.takes_down), 'note': args.note or '',
            'expect_min': getattr(args, 'expect', None),
            'serves': [local_url(u) for u in split_list(getattr(args, 'serves', None))],
            'serves_caps': split_list(getattr(args, 'caps', None)),
            'command': shlex.join(args.command) if getattr(args, 'command', None) else ''}


def say(msg):
    sys.stderr.write(f'fleetq: {msg}\n')
    sys.stderr.flush()


def ago(now, then):
    if then is None:
        return '-'
    s = int(now - then)
    return f'{s}s' if s < 90 else f'{s // 60}m' if s < 5400 else f'{s // 3600}h{(s % 3600) // 60:02d}m'


def describe(job, now=None):
    now = now or time.time()
    bits = [f'#{job["id"]} "{job["label"]}"', f'({job["owner"]})', job['state'],
            'on ' + ', '.join(job['hosts'])]
    if job['exclusive']:
        bits.append('exclusive')
    per_host = job.get('mem_map') or {}
    if per_host:
        bits.append(', '.join(f'{h} {per_host.get(h, job["mem_gb"]):g} GB' for h in job['hosts']))
    elif job['mem_gb']:
        bits.append(f'{job["mem_gb"]:g} GB')
    if job['uses']:
        bits.append('uses ' + ', '.join(job['uses']))
    if job['takes_down']:
        bits.append('takes down ' + ', '.join(job['takes_down']))
    if job['state'] == 'running':
        bits.append(f'for {ago(now, job["started_at"])}')
    return ' '.join(bits)


BRIEF_RE = re.compile(r'#(\d+) "[^"]*" \(([^)]*)\)')


def wait_until_running(client, job, timeout_s, heartbeat):
    """Wait until the job runs. Returns the job, or None on timeout. Prints why it waits.

    Long-polls the server (GET ?wait=N answers as soon as the job changes); a server too old
    to know `wait` answers at once without `waited`, and then this polls every POLL_S.
    """
    last_reason, last_beat, start, down_since = None, time.time(), time.time(), None
    long_poll = True
    while job['state'] == 'queued':
        if job.get('reason') != last_reason:
            say(f'#{job["id"]} waiting (#{job.get("position", "?")} in line): {job.get("reason") or "scheduling"}')
            last_reason = job.get('reason')
            mine = [m.group(1) for m in BRIEF_RE.finditer(last_reason or '') if m.group(2) == job['owner']]
            if mine and not os.environ.get('FLEETQ_JOB'):
                say(f'  #{mine[0]} has your owner name. If this work belongs inside it, stop and rerun '
                    f'with FLEETQ_JOB={mine[0]} set, or it will wait for #{mine[0]} to be released.')
        if timeout_s is not None and time.time() - start > timeout_s:
            return None
        if not long_poll:
            time.sleep(POLL_S)
        try:
            if heartbeat and time.time() - last_beat >= HEARTBEAT_S:
                job = client.call('POST', f'/v1/jobs/{job["id"]}/heartbeat', {})
                last_beat = time.time()
            elif long_poll:
                wait = LONG_POLL_S
                if heartbeat:
                    wait = min(wait, last_beat + HEARTBEAT_S - time.time())
                if timeout_s is not None:
                    wait = min(wait, start + timeout_s - time.time())
                wait = max(wait, 0.0)
                job = client.call('GET', f'/v1/jobs/{job["id"]}?wait={wait:.2f}&state=queued',
                                  timeout=wait + client.timeout)
                long_poll = 'waited' in job
            else:
                job = client.call('GET', f'/v1/jobs/{job["id"]}')
            down_since = None
        except Unreachable:
            # Ride out a server restart (a redeploy takes seconds); give up only on a real outage.
            down_since = down_since or time.time()
            if time.time() - down_since > OUTAGE_S:
                raise
            time.sleep(POLL_S)
    return job


def left_queue(job):
    """Why a job that was waiting will never run (released, cancelled or dropped)."""
    note = f' ({job["note"]})' if job.get('note') else ''
    return f'#{job["id"]} is {job["state"]}{note}, so it will not run'


def release(client, job_id, owner, exit_code=None, note='', attempts=4):
    for i in range(attempts):
        try:
            return client.call('POST', f'/v1/jobs/{job_id}/release',
                               {'owner': owner, 'exit_code': exit_code, 'note': note})
        except Unreachable:
            if i == attempts - 1:
                raise
            time.sleep(2 * (i + 1))


# ---- commands ------------------------------------------------------------
def covers(client, parent_id, payload):
    """(covered, parent job or reason, hosts the request needs) for running job parent_id."""
    try:
        parent = client.call('GET', f'/v1/jobs/{int(parent_id)}')
    except (ApiError, ValueError) as exc:
        return False, f'FLEETQ_JOB={parent_id} is not a job ({exc})', []
    if parent['state'] != 'running':
        return False, f'#{parent["id"]} is {parent["state"]}, not running', []
    try:
        need = client.call('POST', '/v1/resolve', payload)['hosts']
    except ApiError as exc:
        if exc.body.get('not_fleet'):
            return True, parent, []
        raise
    missing = [h for h in need if h not in parent['hosts']]
    if missing:
        return False, f'#{parent["id"]} holds {", ".join(parent["hosts"])}, not {", ".join(missing)}', need
    return True, parent, need


def run_one(command, env):
    """Run a command in the foreground, passing SIGTERM (e.g. from an agent harness) on to it."""
    child = subprocess.Popen(command, env=env)
    main_thread = threading.current_thread() is threading.main_thread()  # handlers need it
    if main_thread:
        previous = signal.signal(signal.SIGTERM, lambda signum, frame: child.send_signal(signum))
    while True:
        try:
            code = child.wait()
            break
        except KeyboardInterrupt:
            continue  # the terminal already sent SIGINT to the child; wait for it to exit
    if main_thread:
        signal.signal(signal.SIGTERM, previous)
    return code


def expand(argv, values):
    """Fill {host}, {machine}, {rank} and {nhosts}; other braces (JSON, awk) stay untouched."""
    out = []
    for arg in argv:
        for key, value in values.items():
            arg = arg.replace('{' + key + '}', str(value))
        out.append(arg)
    return out


def relay(stream, sink, prefix, lock):
    for line in iter(stream.readline, b''):
        with lock:
            sink.write(prefix + line)
            sink.flush()
    stream.close()


def run_each(command, hosts, machines, env, prefix=True, keep_going=False):
    """Run the command once per host, all at once, and return the first failure's exit code.

    When one host's command fails, the others are stopped (they are usually ranks of one
    job that cannot finish without it) unless keep_going.
    """
    if not any('{' + k + '}' in ' '.join(command) for k in ('host', 'machine', 'rank', 'nhosts')):
        say('--each: the command has no {host}/{machine}/{rank} placeholder, so every host runs '
            'the same command (each gets FLEETQ_HOST, FLEETQ_MACHINE, FLEETQ_RANK)')
    lock, relays, procs = threading.Lock(), [], []
    width = max(len(h) for h in hosts)
    for rank, host in enumerate(hosts):
        values = {'host': host, 'machine': machines[host], 'rank': rank, 'nhosts': len(hosts)}
        argv = expand(command, values)
        penv = {**env, 'FLEETQ_HOST': host, 'FLEETQ_MACHINE': machines[host],
                'FLEETQ_RANK': str(rank), 'FLEETQ_NHOSTS': str(len(hosts))}
        say(f'[{host}] {shlex.join(argv)}')
        pipe = subprocess.PIPE if prefix else None
        proc = subprocess.Popen(argv, env=penv, stdout=pipe, stderr=pipe, stdin=subprocess.DEVNULL)
        if prefix:
            tag = f'[{host:<{width}}] '.encode()
            for stream, sink in ((proc.stdout, sys.stdout.buffer), (proc.stderr, sys.stderr.buffer)):
                t = threading.Thread(target=relay, args=(stream, sink, tag, lock), daemon=True)
                t.start()
                relays.append(t)
        procs.append((host, proc))

    def stop_all(signum=signal.SIGTERM):
        for _, proc in procs:
            if proc.poll() is None:
                proc.send_signal(signum)

    main_thread = threading.current_thread() is threading.main_thread()
    if main_thread:
        previous = signal.signal(signal.SIGTERM, lambda signum, frame: stop_all(signum))
    codes, first_failure, deadline = {}, None, None
    try:
        while len(codes) < len(procs):
            try:
                for host, proc in procs:
                    if host in codes or proc.poll() is None:
                        continue
                    codes[host] = proc.returncode
                    if proc.returncode != 0 and first_failure is None:
                        first_failure = host
                        if not keep_going and len(codes) < len(procs):
                            say(f'[{host}] exited {proc.returncode}; stopping the other hosts '
                                '(--keep-going lets them finish)')
                            stop_all()
                            deadline = time.time() + STOP_GRACE_S
                if deadline and time.time() > deadline:
                    stop_all(signal.SIGKILL)
                    deadline = None
                time.sleep(0.1)
            except KeyboardInterrupt:
                continue  # the terminal sent SIGINT to every command; wait for them to exit
    finally:
        if main_thread:
            signal.signal(signal.SIGTERM, previous)
    for t in relays:
        t.join(5)
    say('each: ' + ', '.join(f'{h} exit {codes[h]}' for h, _ in procs))
    return codes[first_failure] if first_failure else 0


def machines_for(client, hosts):
    """Machine (ssh) name for each fleetq host; the host name itself when the roster is unavailable."""
    known = {}
    if client is not None:
        try:
            known = {h['name']: h['machine'] for h in client.call('GET', '/v1/roster')['hosts']}
        except (Unreachable, ApiError):
            pass
    return {h: known.get(h, h) for h in hosts}


def execute(client, args, hosts, env):
    if not args.each:
        return run_one(args.command, env)
    if not hosts:
        say('--each needs hosts (--host, or roles and URLs that name them); running the command once')
        return run_one(args.command, env)
    return run_each(args.command, hosts, machines_for(client, hosts), env,
                    prefix=not args.no_prefix, keep_going=args.keep_going)


def cmd_run(client, args):
    if not args.command:
        say('nothing to run; put the command after --')
        return 64
    payload = job_payload(args, 'run')
    parent_id = os.environ.get('FLEETQ_JOB')
    try:
        if parent_id:
            ok, detail, need = covers(client, parent_id, payload)
            if ok:
                say(f'running inside #{detail["id"]} "{detail["label"]}" (FLEETQ_JOB): {shlex.join(args.command)}')
                if payload['serves']:
                    say(f'--serves is not recorded: #{detail["id"]} holds the slot, so only its own --serves '
                        'URLs are listed by fleetq models')
                return execute(client, args, need or detail['hosts'], dict(os.environ))
            say(f'{detail}; queueing separately')
        job = client.call('POST', '/v1/jobs', payload)
    except Unreachable as exc:
        if not args.no_queue:
            say(f'{exc}. Refusing to start unqueued work; pass --no-queue to run anyway.')
            return EXIT_UNREACHABLE
        say(f'{exc}. --no-queue given: running WITHOUT a queue slot.')
        return execute(None, args, split_list(args.host), dict(os.environ))
    except ApiError as exc:
        if exc.body.get('not_fleet'):
            say(f'{str(exc).split(";")[0]}; running without a slot')
            return execute(None, args, [], dict(os.environ))
        raise
    timeout_s = args.wait_timeout * 60 if args.wait_timeout else None
    try:
        started = wait_until_running(client, job, timeout_s, heartbeat=True)
    except KeyboardInterrupt:
        release(client, job['id'], args.owner, note='cancelled while waiting')
        say(f'#{job["id"]} cancelled')
        return 130
    if started is None:
        release(client, job['id'], args.owner, note='gave up waiting')
        say(f'#{job["id"]} gave up after {args.wait_timeout} min in line')
        return EXIT_TIMEOUT
    if started['state'] != 'running':
        say(f'{left_queue(started)}: {shlex.join(args.command)}')
        return EXIT_CANCELLED
    # The slot may have been released between the last poll and now; check once more right before exec.
    try:
        started = client.call('GET', f'/v1/jobs/{job["id"]}')
    except (Unreachable, ApiError) as exc:
        say(f'could not confirm #{job["id"]} still holds its slot ({exc}); not running the command')
        try:
            release(client, job['id'], args.owner, note='could not confirm slot before start')
        except (Unreachable, ApiError):
            pass
        return EXIT_UNREACHABLE
    if started['state'] != 'running':
        say(f'{left_queue(started)}: {shlex.join(args.command)}')
        return EXIT_CANCELLED
    say(f'#{job["id"]} running on {", ".join(started["hosts"])}: {shlex.join(args.command)}')
    stop = threading.Event()

    def beat():
        while not stop.wait(HEARTBEAT_S):
            try:
                client.call('POST', f'/v1/jobs/{job["id"]}/heartbeat', {})
            except (Unreachable, ApiError) as exc:
                say(f'heartbeat for #{job["id"]} failed ({exc}); the command keeps running')

    threading.Thread(target=beat, daemon=True).start()
    code = execute(client, args, started['hosts'], {**os.environ, 'FLEETQ_JOB': str(job['id'])})
    stop.set()
    try:
        release(client, job['id'], args.owner, exit_code=code)
        say(f'#{job["id"]} done (exit {code})' + (
            f'; {", ".join(job["takes_down"])} must come back before others can use it'
            if job['takes_down'] else ''))
    except (Unreachable, ApiError) as exc:
        say(f'could not mark #{job["id"]} done ({exc}). Run: fleetq release {job["id"]}')
    return code


def unwatch(client, job_id):
    """Tell the server this waiter stops on purpose, so the lease keeps its place without heartbeats."""
    try:
        client.call('POST', f'/v1/jobs/{job_id}/unwatch', {})
    except (Unreachable, ApiError) as exc:
        say(f'could not tell the server #{job_id} should stay in line without a waiter ({exc}); '
            'it may be dropped from the line')


def cmd_acquire(client, args):
    # With --wait this client heartbeats the queued lease; if it is killed, the server drops the lease.
    job = client.call('POST', '/v1/jobs', {**job_payload(args, 'lease'), 'watch': bool(args.wait)})
    if args.wait and job['state'] == 'queued':
        timeout_s = args.wait_timeout * 60 if args.wait_timeout else None
        try:
            got = wait_until_running(client, job, timeout_s, heartbeat=True)
        except KeyboardInterrupt:
            unwatch(client, job['id'])
            say(f'stopped waiting; #{job["id"]} is still in line (fleetq release {job["id"]} to leave)')
            return 130
        if got is None:
            unwatch(client, job['id'])
            say(f'#{job["id"]} still in line after {args.wait_timeout} min; it keeps its place')
            job = client.call('GET', f'/v1/jobs/{job["id"]}')
            emit(args, job, describe(job))
            return EXIT_TIMEOUT
        if got['state'] != 'running':
            say(left_queue(got))
            emit(args, got, describe(got))
            return EXIT_CANCELLED
        job = got
    emit(args, job, describe(job) + (f'\n  waiting because: {job["reason"]}' if job['state'] == 'queued' else '')
         + f'\n  run work inside it: prefix commands with FLEETQ_JOB={job["id"]}'
         + f'\n  release when finished: fleetq release {job["id"]}')
    return 0


def cmd_wait(client, args):
    job = client.call('GET', f'/v1/jobs/{args.id}')
    timeout_s = args.timeout * 60 if args.timeout else None
    got = wait_until_running(client, job, timeout_s, heartbeat=False)
    if got is None:
        return EXIT_TIMEOUT
    emit(args, got, describe(got))
    return 0 if got['state'] == 'running' else 1


def cmd_inside(client, args):
    """Exit 0 if FLEETQ_JOB is a running job holding the requested host(s), else 1."""
    parent_id = os.environ.get('FLEETQ_JOB')
    if not parent_id:
        say('FLEETQ_JOB is not set')
        return 1
    ok, detail, _ = covers(client, parent_id, job_payload(args, 'lease'))
    say(f'inside #{detail["id"]} "{detail["label"]}"' if ok else detail)
    return 0 if ok else 1


def cmd_release(client, args):
    job = client.call('POST', f'/v1/jobs/{args.id}/release',
                      {'owner': args.owner, 'force': args.force, 'note': args.note or '',
                       'exit_code': args.exit_code})
    extra = ''
    if job['takes_down']:
        extra = f'\n  {", ".join(job["takes_down"])} now marked RESTORING until a health check sees it back'
    emit(args, job, describe(job) + extra)
    return 0


def cmd_show(client, args):
    job = client.call('GET', f'/v1/jobs/{args.id}')
    lines = [describe(job)]
    if job.get('reason'):
        lines.append(f'  waiting because: {job["reason"]}')
    lines += [f'  ! {f}' for f in job['flags']]
    if job['command']:
        lines.append(f'  command: {job["command"]}')
    emit(args, job, '\n'.join(lines))
    return 0


def cmd_status(client, args):
    st = client.call('GET', '/v1/status')
    if args.json:
        print(json.dumps(st, indent=2))
        return 0
    print(render_status(st, args.host, *terminal_style()))
    return 0


def cmd_roster(client, args):
    r = client.call('GET', '/v1/roster')
    if args.json:
        print(json.dumps(r, indent=2))
        return 0
    for h in r['hosts']:
        alias = f' (aka {", ".join(h["aliases"])})' if h['aliases'] else ''
        print(f'{h["name"]}{alias} on {h["machine"]}: {h["capacity_gb"]:g} GB'
              + (' · collector connected' if h['collector'] else ''))
        for s in (s for s in r['services'] if s['host'] == h['name']):
            roles = f' roles {", ".join(s["roles"])}' if s['roles'] else ''
            print(f'  {s["name"]:<24} {s["kind"]:<9} {s["mem_gb"]:>4g} GB{roles}'
                  + (f'  {s["url"]} [{", ".join(s["models"])}]' if s['url'] else ''))
    return 0


def cmd_models(client, args):
    query = []
    if args.all:
        query.append('all=1')
    caps = split_list(args.cap)
    if caps:
        query.append('cap=' + ','.join(caps))
    r = client.call('GET', '/v1/models' + ('?' + '&'.join(query) if query else ''))
    if args.json:
        print(json.dumps(r, indent=2))
        return 0
    print(render_models(r['models']))
    return 0


def render_models(models):
    """One line per (url, model): usable mark, model, capabilities, context, where it runs."""
    if not models:
        return 'no models match'
    rows = []
    for m in models:
        mark = '+' if m['usable'] else MARK.get(m['state'], '!')
        caps = ','.join(m['capabilities']) or 'caps unknown'
        ctx = f'{m["context"] // 1024}K ctx' if m.get('context') else ''
        if m['job']:
            where = f'job #{m["job"]["id"]} on {", ".join(m["hosts"])}'
        else:
            where = ', '.join(m['roles']) or m['service']
        state = '' if m['usable'] else f'  ({m["state"]}{": " + m["detail"] if m.get("detail") else ""})'
        rows.append((mark, m['model'] or '-', caps, ctx, m['url'], where + state))
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]) - 1)]
    return '\n'.join('  '.join(c.ljust(w) for c, w in zip(r, widths)) + '  ' + r[-1] for r in rows)


def cmd_log(client, args):
    for e in reversed(client.call('GET', f'/v1/events?limit={args.limit}')['events']):
        print(time.strftime('%m-%d %H:%M:%S', time.localtime(e['at'])), e['text'])
    return 0


def cmd_mark_up(client, args):
    client.call('POST', f'/v1/services/{args.service}/mark-up', {'owner': args.owner, 'note': args.note or ''})
    print(f'{args.service} confirmed back')
    return 0


def cmd_probe(client, args):
    st = client.call('POST', '/v1/probe', {})
    print(render_status(st, None, *terminal_style()))
    return 0


def emit(args, job, text):
    print(json.dumps(job, indent=2) if getattr(args, 'json', False) else text)


MARK = {'up': '+', 'held': '-', 'off': '-', 'restoring': '~', 'starting': '~', 'unknown': '?'}


def terminal_style():
    """(width, color) for printing to stdout: wrap and color only on a real terminal."""
    if not sys.stdout.isatty():
        return None, False
    return shutil.get_terminal_size().columns, not os.environ.get('NO_COLOR')


def render_status(st, only_host=None, width=None, color=False):
    """Human status board. width wraps long lines with a hanging indent; color adds ANSI styling."""
    now = st['now']

    def paint(code, text):
        return f'\033[{code}m{text}\033[0m' if color and text else text

    def block(text, lead, hang=None, style=None):
        """Wrap text under lead, continuation lines indented to hang (default: lead's width)."""
        hang = ' ' * len(lead) if hang is None else hang
        out = (textwrap.wrap(text, width, initial_indent=lead, subsequent_indent=hang,
                             break_on_hyphens=False) if width and len(lead) + len(text) > width
               else [lead + text])
        return [paint(style, line) if style else line for line in out]

    hosts = [h for h in st['hosts'] if not only_host
             or set(split_list([only_host])) & {h['name'], h['machine'], *h['aliases']}]
    on_screen = {j['id'] for h in hosts for j in h['running'] + h['queued']}

    def short(text):
        # The server names jobs as #id "label" (owner); once a job is listed here, its id is enough.
        return BRIEF_RE.sub(lambda m: f'#{m.group(1)}' if int(m.group(1)) in on_screen else m.group(0), text)

    lines = [f'fleetq · {time.strftime("%H:%M:%S", time.localtime(now))} · last health check '
             + (f'{ago(now, st["last_probe"])} ago' if st['last_probe'] else 'pending')]
    if st['warnings'] and not only_host:
        lines += ['', paint('1;31', 'Attention')]
        for w in st['warnings']:
            lines += block(short(w), '  ! ', style='31')
    shown = {}
    for h in hosts:
        state = {True: 'online', False: paint('1;31', 'OFFLINE'), None: 'not checked yet'}[h['online']]
        head = f'{h["name"]} ({h["machine"]}) · {state}'
        if h.get('gpu_util') is not None:
            head += f' · GPU {h["gpu_util"]:.0%} busy'
        mem = (f'{h["capacity_gb"]:g} GB: services {h["services_gb"]:g} · jobs {h["jobs_gb"]:g}'
               f' · free {h["free_gb"]:g}')
        if h['actual_free_gb'] is not None:
            mem += f'  (box reports {h["actual_free_gb"]:g} free)'
        lines += ['', paint('1', head)] + block(mem, '  memory    ')
        if h['services']:
            lines.append('  services')
        for s in h['services']:
            mark = '!' if s['warn'] else MARK.get(s['status'], '!')
            roles = ', '.join(s['roles']) or s['kind']
            flight = f' · {s["in_flight"]} in flight' if s['in_flight'] else ''
            lead = f'    {mark} {s["name"]:<22} {roles:<26} {s["mem_gb"]:>4g} GB  '
            out = block(short(s['text']) + flight, lead)
            if mark != '+':
                out[0] = out[0][:4] + paint('31' if mark in '!?' else '33', mark) + out[0][5:]
            lines += out
        if h['running']:
            lines.append(paint('32', '  running'))
        for j in h['running']:
            lead = f'    #{j["id"]}  '
            if j['id'] in shown:
                lines.append(paint('2', f'{lead}same job as on {shown[j["id"]]}'))
                continue
            shown[j['id']] = h['name']
            gb = (j.get('mem_map') or {}).get(h['name'], j['mem_gb'])
            facts = [j['owner'], f'for {ago(now, j["started_at"])}']
            if j['exclusive']:
                facts.append('exclusive')
            if gb:
                facts.append(f'{gb:g} GB' + (' here' if len(j['hosts']) > 1 else ''))
            if len(j['hosts']) > 1:
                facts.append('also on ' + ', '.join(x for x in j['hosts'] if x != h['name']))
            if j['uses']:
                facts.append('uses ' + ', '.join(j['uses']))
            if j['takes_down']:
                facts.append('takes down ' + ', '.join(j['takes_down']))
            if j.get('serves'):
                facts.append('serves ' + ', '.join(j['serves']))
            lines += block(j['label'], lead)
            lines += block(' · '.join(facts), ' ' * len(lead), style='2')
            for f in j['flags']:
                lines += block(short(f), ' ' * len(lead) + '! ', style='31')
        if h['queued']:
            lines.append(paint('33', '  queued'))
        for j in h['queued']:
            lead = f'    {j["position"]}. #{j["id"]}  '
            if j['id'] in shown:
                lines.append(paint('2', f'{lead}same job as on {shown[j["id"]]}'))
                continue
            shown[j['id']] = h['name']
            lines += block(j['label'], lead)
            lines += block(f'{j["owner"]} · waits: {short(j["reason"])}', ' ' * len(lead), style='2')
    if st['recent'] and not only_host:
        lines += ['', paint('1', 'Recent')]
        for j in st['recent'][:5]:
            exit_code = f', exit {j["exit_code"]}' if j['exit_code'] is not None else ''
            lines += block(f'{j["label"]}  ({j["owner"]}{exit_code})',
                           f'  #{j["id"]}  {j["state"]:<9} {ago(now, j["ended_at"]):>4} ago  ')
    return '\n'.join(lines)


def add_job_args(p):
    p.add_argument('--host', action='append', help='host, alias or machine (repeatable, or comma-separated)')
    p.add_argument('--for-url', dest='for_url', help='the box serving this URL (loopback means this machine)')
    p.add_argument('--mem', action='append',
                   help='GB this job adds on each host (default 0), or per host: spark1=100,spark2=90')
    p.add_argument('--exclusive', action='store_true', help='nothing else may run on the host meanwhile')
    p.add_argument('--uses', action='append', help='roles/services this job needs up (shared)')
    p.add_argument('--takes-down', action='append', dest='takes_down',
                   help='roles/services this job stops; they must be restored afterwards')
    p.add_argument('--label', required=True, help='what this is, shown to everyone waiting')
    p.add_argument('--note', help='optional detail')
    p.add_argument('--serves', action='append',
                   help='model server URL this job brings up (e.g. http://spark-4e77:8001/v1); fleetq models '
                        'lists what it serves while the job runs (repeatable)')
    p.add_argument('--caps', action='append',
                   help='what the models at --serves can do: text, image, video, audio, embed, rerank')


def parser():
    ap = argparse.ArgumentParser(prog='fleetq', description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--url', help='server URL (overrides FLEETQ_URL)')
    ap.add_argument('--owner', default=None, help='owner name (default FLEETQ_OWNER or user@host)')
    sub = ap.add_subparsers(dest='cmd', required=True)

    p = sub.add_parser('status', help='what is running and waiting on every box')
    p.add_argument('--host')
    p.add_argument('--json', action='store_true')
    p.set_defaults(fn=cmd_status)

    p = sub.add_parser('run', help='wait for a slot, run a command, release when it exits')
    add_job_args(p)
    p.add_argument('--wait-timeout', type=float, help='minutes to wait in line before giving up')
    p.add_argument('--no-queue', action='store_true', help='if the server is unreachable, run anyway')
    p.add_argument('--each', action='store_true',
                   help='run the command once per host, all at once, filling {host} {machine} {rank} {nhosts}')
    p.add_argument('--keep-going', action='store_true', help='with --each: do not stop the others when one fails')
    p.add_argument('--no-prefix', action='store_true', help='with --each: do not tag output lines with the host')
    p.add_argument('command', nargs=argparse.REMAINDER, help='-- command ...')
    p.set_defaults(fn=cmd_run)

    p = sub.add_parser('acquire', help='take a slot for multi-step work; release it yourself')
    add_job_args(p)
    p.add_argument('--expect', type=float, help='expected minutes (default 120); flagged overdue after')
    p.add_argument('--wait', action='store_true', help='block until the slot is granted')
    p.add_argument('--wait-timeout', type=float, help='minutes to wait (with --wait)')
    p.add_argument('--json', action='store_true')
    p.set_defaults(fn=cmd_acquire)

    p = sub.add_parser('wait', help='block until a queued job is running')
    p.add_argument('id', type=int)
    p.add_argument('--timeout', type=float, help='minutes')
    p.add_argument('--json', action='store_true')
    p.set_defaults(fn=cmd_wait)

    p = sub.add_parser('inside', help='exit 0 if FLEETQ_JOB already holds these hosts')
    p.add_argument('--host', action='append')
    p.add_argument('--for-url', dest='for_url')
    p.add_argument('--uses', action='append')
    p.add_argument('--takes-down', action='append', dest='takes_down')
    p.set_defaults(fn=cmd_inside, label='inside check', mem=None, exclusive=False, note='')

    p = sub.add_parser('release', help='mark a job finished, or leave the queue')
    p.add_argument('id', type=int)
    p.add_argument('--force', action='store_true', help='release a job someone else owns')
    p.add_argument('--note')
    p.add_argument('--exit-code', type=int)
    p.add_argument('--json', action='store_true')
    p.set_defaults(fn=cmd_release)

    p = sub.add_parser('show', help='one job and why it is waiting')
    p.add_argument('id', type=int)
    p.add_argument('--json', action='store_true')
    p.set_defaults(fn=cmd_show)

    p = sub.add_parser('roster', help='hosts, services and roles fleetq knows')
    p.add_argument('--json', action='store_true')
    p.set_defaults(fn=cmd_roster)

    p = sub.add_parser('models', help='models the fleet serves right now, with URL and capabilities')
    p.add_argument('--cap', action='append', help='only models with these capabilities (e.g. --cap image)')
    p.add_argument('--all', action='store_true', help='also models that are down, held or starting')
    p.add_argument('--json', action='store_true')
    p.set_defaults(fn=cmd_models)

    p = sub.add_parser('log', help='recent queue events')
    p.add_argument('--limit', type=int, default=30)
    p.set_defaults(fn=cmd_log)

    p = sub.add_parser('mark-up', help='confirm a taken-down service is back (for services without a health check)')
    p.add_argument('service')
    p.add_argument('--note')
    p.set_defaults(fn=cmd_mark_up)

    p = sub.add_parser('probe', help='run health checks now and show status')
    p.set_defaults(fn=cmd_probe)
    for p in sub.choices.values():
        # Accept --owner after the subcommand too; SUPPRESS keeps a top-level value intact.
        p.add_argument('--owner', default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    return ap


def main(argv=None):
    args = parser().parse_args(argv)
    args.owner = args.owner or default_owner()
    if getattr(args, 'command', None) and args.command[:1] == ['--']:
        args.command = args.command[1:]
    client = Client(args.url)
    try:
        return args.fn(client, args)
    except Unreachable as exc:
        say(str(exc))
        return EXIT_UNREACHABLE
    except ApiError as exc:
        say(f'error {exc.code}: {exc}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
