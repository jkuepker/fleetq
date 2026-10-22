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
from contextlib import contextmanager
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
from urllib.parse import quote, urlparse, urlunparse
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
    """--mem 30, --mem gpu-a=100,gpu-b=90, or both: (GB for every host, {host: GB})."""
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


def parse_duration(text):
    """Seconds from 90 (minutes), 90m, 2h, 1h30m, 1d or 45s."""
    t = str(text).strip().lower()
    if re.fullmatch(r'\d+(\.\d+)?', t):
        return float(t) * 60
    m = re.fullmatch(r'(?:(\d+(?:\.\d+)?)d)?(?:(\d+(?:\.\d+)?)h)?(?:(\d+(?:\.\d+)?)m(?:in)?)?'
                     r'(?:(\d+(?:\.\d+)?)s)?', t)
    if not t or not m:
        raise SystemExit(f'fleetq: cannot read the duration {text!r}; try 90m, 2h or 1h30m')
    d, h, mi, sec = (float(g) if g else 0.0 for g in m.groups())
    return d * 86400 + h * 3600 + mi * 60 + sec


def parse_at(text, now=None):
    """Epoch seconds for HH:MM (the next one), 'tomorrow HH:MM' or an ISO date and time, local time."""
    from datetime import datetime, timedelta
    base = datetime.fromtimestamp(now if now is not None else time.time())
    t, days = str(text).strip(), 0
    if t.lower().startswith('tomorrow'):
        t, days = t[len('tomorrow'):].strip(), 1
    m = re.fullmatch(r'(\d{1,2}):(\d{2})', t)
    try:
        if m:
            at = base.replace(hour=int(m.group(1)), minute=int(m.group(2)), second=0, microsecond=0)
            at += timedelta(days=days)
            if not days and at <= base:
                at += timedelta(days=1)
            return at.timestamp()
        if not days:
            return datetime.fromisoformat(t).timestamp()
    except ValueError:
        pass
    raise SystemExit(f'fleetq: cannot read the time {text!r}; try 14:30, "tomorrow 09:00" or 2026-10-23T14:30')


def start_in(args):
    """Seconds from now until --at / --in, or None. Sent relative, so the server's clock decides."""
    if getattr(args, 'at', None) and getattr(args, 'in_', None):
        raise SystemExit('fleetq: give --at or --in, not both')
    if getattr(args, 'at', None):
        return parse_at(args.at) - time.time()
    if getattr(args, 'in_', None):
        return parse_duration(args.in_)
    return None


def load_words(values):
    """--load / --quiet words as a list; 'none' means none; not given at all: None."""
    if not values:
        return None
    words = split_list(values)
    return [] if words == ['none'] else words


def job_payload(args, kind):
    mem, mem_map = parse_mem(args.mem)
    expect = getattr(args, 'expect', None)
    if getattr(args, 'length', None):
        expect = parse_duration(args.length) / 60
    return {'kind': kind, 'label': args.label, 'owner': args.owner, 'hosts': split_list(args.host),
            'for_url': local_url(getattr(args, 'for_url', None)), 'mem_map': mem_map,
            'mem_gb': mem, 'exclusive': args.exclusive, 'uses': split_list(args.uses),
            'takes_down': split_list(args.takes_down), 'note': args.note or '',
            'expect_min': expect, 'start_in_s': start_in(args),
            'load': load_words(getattr(args, 'load', None)), 'quiet': load_words(getattr(args, 'quiet', None)),
            'serves': [local_url(u) for u in split_list(getattr(args, 'serves', None))],
            'serves_caps': split_list(getattr(args, 'caps', None)),
            'notify': getattr(args, 'notify', None) or '',
            'command': shlex.join(args.command) if getattr(args, 'command', None) else ''}


def say(msg):
    try:
        sys.stderr.write(f'fleetq: {msg}\n')
        sys.stderr.flush()
    except OSError:
        pass  # the terminal is gone (hangup); keep going so the job is still released


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
    bits += load_facts(job)
    if job['state'] == 'queued' and (job.get('start_in_s') or 0) > 0:
        bits.append(f'reserved for {local_time(job["start_in_s"])}')
    if job.get('start_at') is not None and job.get('expect_s'):
        bits.append(f'booked for {span(job["expect_s"])}')
    if job['state'] == 'running':
        bits.append(f'for {ago(now, job["started_at"])}')
    if job['state'] == 'running' and job.get('deadline_in_s') is not None:
        bits.append(f'must end within {span(max(job["deadline_in_s"], 0))}')
    return ' '.join(bits)


def span(seconds):
    s = int(seconds)
    return f'{s}s' if s < 90 else f'{s // 60}m' if s < 5400 else f'{s // 3600}h{(s % 3600) // 60:02d}m'


def local_time(seconds_from_now, now=None):
    """A time seconds_from_now after now (default: this machine's clock), as 'Wed 14:30'."""
    return time.strftime('%a %H:%M', time.localtime((now if now is not None else time.time()) + seconds_from_now))


def load_facts(job):
    """--load / --quiet worth mentioning: anything but the default (loads the GPU, needs nothing quiet)."""
    out = []
    if job.get('load') is not None and job['load'] != ['gpu']:
        out.append('loads ' + (','.join(job['load']) or 'nothing'))
    if job.get('quiet'):
        out.append('needs ' + ','.join(job['quiet']) + ' quiet')
    return out


BRIEF_RE = re.compile(r'#(\d+) "[^"]*" \(([^)]*)\)')


def job_path(job_id, owner=None, **params):
    """/v1/jobs/<id>, naming the owner when known: only the owner looking claims a reservation."""
    if owner:
        params['owner'] = owner
    query = '&'.join(f'{k}={quote(str(v), safe="")}' for k, v in params.items())
    return f'/v1/jobs/{int(job_id)}' + ('?' + query if query else '')


def wait_until_running(client, job, timeout_s, heartbeat, owner=None):
    """Wait until the job runs. Returns the job, or None on timeout. Prints why it waits.

    Long-polls the server (GET ?wait=N answers as soon as the job changes); a server too old
    to know `wait` answers at once without `waited`, and then this polls every POLL_S.
    """
    last_reason, last_beat, start, down_since = None, time.time(), time.time(), None
    long_poll = True
    while job['state'] == 'queued':
        if job.get('reason') != last_reason and (job.get('start_in_s') or 0) > 0:
            say(f'#{job["id"]} reserved for {local_time(job["start_in_s"])}; waiting until then')
            last_reason = job.get('reason')
        elif job.get('reason') != last_reason:
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
                job = client.call('POST', f'/v1/jobs/{job["id"]}/heartbeat', {'owner': owner} if owner else {})
                last_beat = time.time()
            elif long_poll:
                wait = LONG_POLL_S
                if heartbeat:
                    wait = min(wait, last_beat + HEARTBEAT_S - time.time())
                if timeout_s is not None:
                    wait = min(wait, start + timeout_s - time.time())
                wait = max(wait, 0.0)
                job = client.call('GET', job_path(job['id'], owner, wait=f'{wait:.2f}', state='queued'),
                                  timeout=wait + client.timeout)
                long_poll = 'waited' in job
            else:
                job = client.call('GET', job_path(job['id'], owner))
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
        parent = client.call('GET', job_path(parent_id, payload.get('owner')))
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


# Watchdog for commands in their own sessions (see Stopper.supervised). Arguments: the pipe's
# read end, the grace seconds, the process group ids. It ignores the terminal's signals.
WATCHDOG = r'''
import os, signal, sys, time
for s in (signal.SIGINT, signal.SIGHUP):
    signal.signal(s, signal.SIG_IGN)
fd, grace, groups = int(sys.argv[1]), float(sys.argv[2]), [int(g) for g in sys.argv[3:]]
while os.read(fd, 4096):
    pass
def send(sig):
    left = []
    for g in groups:
        try:
            os.killpg(g, sig)
            left.append(g)
        except OSError:
            pass
    return left
if send(0):
    send(signal.SIGTERM)
    end = time.time() + grace
    while send(0) and time.time() < end:
        time.sleep(0.1)
    send(signal.SIGKILL)
'''


class Stopper:
    """Stops a job's processes: SIGTERM first, SIGKILL to whatever is left STOP_GRACE_S later.

    group=True: each process leads a session of its own (start_new_session) and signals go to
    its whole process group, so `bash -c ...` or `uv run ...` cannot leave the real work running.
    """

    def __init__(self, procs, group=False):
        self.procs, self.group, self.kill_at = procs, group, None

    def send(self, signum):
        for proc in self.procs:
            if self.group:
                try:
                    os.killpg(proc.pid, signum)
                except (ProcessLookupError, PermissionError):
                    pass
            elif proc.poll() is None:
                proc.send_signal(signum)

    def alive(self):
        """group=True: does any process group still have members?"""
        for proc in self.procs:
            try:
                os.killpg(proc.pid, 0)
                return True
            except (ProcessLookupError, PermissionError):
                pass
        return False

    def drain(self):
        """group=True, once the commands have exited: stop what they left running (the slot
        ends with the command), SIGKILL after the grace time, or at once on another Ctrl-C."""
        if not self.group or not self.alive():
            return
        say('stopping processes the command left running')
        try:
            self.stop()
            until = time.time() + STOP_GRACE_S + 5
            while self.alive() and time.time() < until:
                self.tick()
                time.sleep(0.1)
        except KeyboardInterrupt:
            say('Ctrl-C again: killing them now')
            self.send(signal.SIGKILL)

    @contextmanager
    def supervised(self):
        """While the commands run: pass SIGTERM on to them, and for commands in their own
        sessions (group=True) SIGHUP too, plus a watchdog that stops them if fleetq dies.

        The terminal's hangup and a supervisor's group kill (`timeout -s KILL`, an agent
        harness) no longer reach a command in its own session. The watchdog is a separate
        process in a session of its own, reading a pipe whose write end only this process
        holds: when fleetq exits by any means, even SIGKILL, the pipe closes and the watchdog
        stops what is left of the commands' groups (SIGTERM, SIGKILL after the grace time).
        """
        handled = [signal.SIGTERM] + ([signal.SIGHUP] if self.group else [])
        main_thread = threading.current_thread() is threading.main_thread()  # handlers need it
        previous = {}
        if main_thread:
            for signum in handled:
                previous[signum] = signal.signal(signum, lambda signum, frame: self.send(signum))
        watchdog, write_end = None, None
        if self.group:
            read_end, write_end = os.pipe()
            try:
                watchdog = subprocess.Popen(
                    [sys.executable, '-c', WATCHDOG, str(read_end), str(STOP_GRACE_S),
                     *(str(p.pid) for p in self.procs)],
                    pass_fds=(read_end,), start_new_session=True, stdin=subprocess.DEVNULL)
            except OSError as exc:
                say(f'could not start the watchdog ({exc}); if fleetq dies, stop the command yourself')
            finally:
                os.close(read_end)
        try:
            yield
        finally:
            if write_end is not None:
                os.close(write_end)  # the watchdog sees the end and leaves (the groups are gone)
            if watchdog is not None:
                try:
                    watchdog.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
            for signum, handler in previous.items():
                signal.signal(signum, handler)

    def stop(self):
        self.send(signal.SIGTERM)
        if self.kill_at is None:
            self.kill_at = time.time() + STOP_GRACE_S

    def tick(self):
        """Call often while waiting: kills what ignored the SIGTERM once the grace time is up."""
        if self.kill_at is not None and time.time() > self.kill_at:
            self.send(signal.SIGKILL)
            self.kill_at = None


class Deadline:
    """When a job that started ahead of the line must end, on this machine's clock. The server
    sends it relative (deadline_in_s); heartbeat answers keep it current."""

    def __init__(self, job=None):
        self.at, self.hit = None, False
        if job:
            self.update(job)

    def update(self, job):
        """Take the server's latest deadline from a job dict. None clears it: the server drops a
        deadline once nothing the job went ahead of is still waiting."""
        left = job.get('deadline_in_s')
        self.at = time.time() + left if left is not None else None

    def check(self, stopper):
        """Stop the processes once the deadline passes; returns whether it has."""
        if not self.hit and self.at is not None and time.time() >= self.at:
            self.hit = True
            say(f'deadline reached: stopping the command (SIGTERM, SIGKILL after {STOP_GRACE_S}s) so the '
                'job it went ahead of can start')
            stopper.stop()
        stopper.tick()
        return self.hit


def grouped(deadline):
    """Give the command a session of its own? Only for a job with a deadline: fleetq then stops
    it by itself and must reach every process it started. Other commands stay in the terminal's
    session, so prompts (/dev/tty) and job control keep working."""
    return deadline is not None and deadline.at is not None


def warn_ssh(argv, deadline):
    if grouped(deadline) and any(os.path.basename(a) == 'ssh' for a in argv[:1]):
        say('the deadline stops the local ssh; the remote command may keep running unless it ends with '
            'the connection (ssh -tt gives it a terminal that hangs up), so stop it yourself if it does not')


def run_one(command, env, deadline=None):
    """Run a command in the foreground, passing SIGTERM (e.g. from an agent harness) on to it,
    and stopping it at deadline (a Deadline) if one is given."""
    group = grouped(deadline)
    warn_ssh(command, deadline)
    child = subprocess.Popen(command, env=env, start_new_session=group)
    stopper = Stopper([child], group)
    with stopper.supervised():
        while True:
            try:
                code = child.wait(timeout=0.2 if deadline else None)
                break
            except subprocess.TimeoutExpired:
                deadline.check(stopper)
            except KeyboardInterrupt:
                if group:  # its own session: the terminal's Ctrl-C reached only fleetq
                    stopper.send(signal.SIGINT)
                # otherwise the terminal already sent SIGINT to the child; wait for it to exit
        stopper.drain()
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


def run_each(command, hosts, machines, env, prefix=True, keep_going=False, deadline=None):
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
        warn_ssh(argv, deadline)
        pipe = subprocess.PIPE if prefix else None
        proc = subprocess.Popen(argv, env=penv, stdout=pipe, stderr=pipe, stdin=subprocess.DEVNULL,
                                start_new_session=grouped(deadline))
        if prefix:
            tag = f'[{host:<{width}}] '.encode()
            for stream, sink in ((proc.stdout, sys.stdout.buffer), (proc.stderr, sys.stderr.buffer)):
                t = threading.Thread(target=relay, args=(stream, sink, tag, lock), daemon=True)
                t.start()
                relays.append(t)
        procs.append((host, proc))

    stopper = Stopper([proc for _, proc in procs], grouped(deadline))
    codes, first_failure = {}, None
    with stopper.supervised():
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
                            stopper.stop()
                if deadline:
                    deadline.check(stopper)
                stopper.tick()
                time.sleep(0.1)
            except KeyboardInterrupt:
                if stopper.group:  # their own sessions: the terminal's Ctrl-C reached only fleetq
                    stopper.send(signal.SIGINT)
                continue  # otherwise the terminal sent SIGINT to every command; wait for them to exit
        stopper.drain()
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


def execute(client, args, hosts, env, deadline=None):
    if not args.each:
        return run_one(args.command, env, deadline)
    if not hosts:
        say('--each needs hosts (--host, or roles and URLs that name them); running the command once')
        return run_one(args.command, env, deadline)
    return run_each(args.command, hosts, machines_for(client, hosts), env,
                    prefix=not args.no_prefix, keep_going=args.keep_going, deadline=deadline)


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
        if 'free' in exc.body:
            say(f'error {exc.code}: {exc}')
            say(free_text(exc.body['free'], exc.body['now']))
            return 1
        raise
    if not booked_as_asked(client, args, payload, job):
        return 1
    timeout_s = args.wait_timeout * 60 if args.wait_timeout else None
    try:
        started = wait_until_running(client, job, timeout_s, heartbeat=True, owner=args.owner)
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
        started = client.call('GET', job_path(job['id'], args.owner))
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
    deadline = Deadline(started)
    if deadline.at is not None:
        say(f'#{job["id"]} went ahead of the line and must end within {span(started["deadline_in_s"])} '
            f'(by {local_time(started["deadline_in_s"])}); it is stopped then')
    stop = threading.Event()

    def beat():
        while not stop.wait(HEARTBEAT_S):
            try:
                deadline.update(client.call('POST', f'/v1/jobs/{job["id"]}/heartbeat', {'owner': args.owner}))
            except (Unreachable, ApiError) as exc:
                say(f'heartbeat for #{job["id"]} failed ({exc}); the command keeps running')

    threading.Thread(target=beat, daemon=True).start()
    code = execute(client, args, started['hosts'], {**os.environ, 'FLEETQ_JOB': str(job['id'])}, deadline)
    stop.set()
    if deadline.hit:
        say(f'#{job["id"]} was stopped at its deadline (exit {code}); next time give a longer --expect, '
            'or queue it without one to wait its turn')
    try:
        release(client, job['id'], args.owner, exit_code=code,
                note='stopped at its deadline' if deadline.hit else '')
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
    return hold(client, args, job)


def cmd_reserve(client, args):
    if not (args.at or args.in_):
        raise SystemExit('fleetq: reserve needs a start: --at TIME or --in DURATION')
    try:
        job = client.call('POST', '/v1/jobs', {**job_payload(args, 'lease'), 'watch': bool(args.wait)})
    except ApiError as exc:
        if 'free' not in exc.body:
            raise
        say(f'error {exc.code}: {exc}')
        say(free_text(exc.body['free'], exc.body['now']))
        return 1
    if not booked_as_asked(client, args, job_payload(args, 'lease'), job):
        return 1
    if job.get('start_at') is None:  # under a minute ahead: queued as an ordinary lease
        return hold(client, args, job)
    return hold(client, args, job, '\n  claim it once it starts: fleetq wait {id}, or run work with FLEETQ_JOB={id};'
                                   ' unclaimed, it lapses after 15 min')


def booked_as_asked(client, args, payload, job):
    """A server from before reservations ignores the start time and would hold the slot now."""
    if (payload.get('start_in_s') or 0) < 60 or job.get('start_at') is not None:
        return True  # under a minute ahead the server queues it as an ordinary job
    release(client, job['id'], args.owner, note='server does not know reservations')
    say(f'the fleetq server does not know reservations (--at / --in); #{job["id"]} left the queue. '
        'Update the server, then book again.')
    return False


def free_text(free, now):
    """Free windows from a refused reservation, in this machine's local time."""
    if not free:
        return 'no free window found'
    parts = []
    for w in free:
        start = 'now' if w['start'] <= now + 60 else local_time(w['start'] - now)
        parts.append(start + (f' until {local_time(w["until"] - now)}' if w.get('until') else ' on'))
    return 'free: ' + '; '.join(parts)


def hold(client, args, job, hint=''):
    """A lease or reservation just queued: wait for it if asked, then say how to use it."""
    if args.wait and job['state'] == 'queued':
        timeout_s = args.wait_timeout * 60 if args.wait_timeout else None
        try:
            got = wait_until_running(client, job, timeout_s, heartbeat=True, owner=args.owner)
        except KeyboardInterrupt:
            unwatch(client, job['id'])
            say(f'stopped waiting; #{job["id"]} is still in line (fleetq release {job["id"]} to leave)')
            return 130
        if got is None:
            unwatch(client, job['id'])
            say(f'#{job["id"]} still in line after {args.wait_timeout} min; it keeps its place')
            job = client.call('GET', job_path(job['id'], args.owner))
            emit(args, job, describe(job))
            return EXIT_TIMEOUT
        if got['state'] != 'running':
            say(left_queue(got))
            emit(args, got, describe(got))
            return EXIT_CANCELLED
        job = got
    emit(args, job, describe(job) + (f'\n  waiting because: {job["reason"]}' if job['state'] == 'queued' else '')
         + hint.format(id=job['id'])
         + f'\n  run work inside it: prefix commands with FLEETQ_JOB={job["id"]}'
         + f'\n  release when finished: fleetq release {job["id"]}')
    return 0


def cmd_update(client, args):
    mem, mem_map = parse_mem(args.mem) if args.mem else (None, {})
    body = {'owner': args.owner, 'force': args.force, 'load': load_words(args.load),
            'quiet': load_words(args.quiet), 'mem_gb': mem, 'mem_map': mem_map}
    job = client.call('POST', f'/v1/jobs/{args.id}/update', body)
    emit(args, job, describe(job))
    return 0


def cmd_when(client, args):
    r = client.call('POST', '/v1/when', job_payload(args, 'lease' if args.lease else 'run'))
    if args.json:
        print(json.dumps(r, indent=2))
        return 0
    where = ', '.join(r['hosts'])
    if r['starts_now']:
        line = f'starts now on {where}'
        if r['deadline_in_s'] is not None:
            line += (f', ahead of the line: it must end within {span(r["deadline_in_s"])} '
                     f'(by {local_time(r["deadline_in_s"])})')
        print(line)
        return 0
    print(f'not now on {where}: {r["reason"]}')
    if r['start_in_s'] is not None:
        print(f'  estimated start: {local_time(r["start_in_s"])} (in {span(r["start_in_s"])}), '
              'if jobs ahead end when they expect to')
    else:
        print('  no estimate: what holds it up has no expected end (a service down, a job without --expect, '
              'load fleetq does not track)')
    return 0


def cmd_wait(client, args):
    job = client.call('GET', job_path(args.id, args.owner))  # the owner waiting claims a reservation
    timeout_s = args.timeout * 60 if args.timeout else None
    got = wait_until_running(client, job, timeout_s, heartbeat=False, owner=args.owner)
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
    job = client.call('GET', job_path(args.id, args.owner))  # someone else's look claims nothing
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


def dur(seconds):
    """Compact duration for the stats tables: 45s, 12m, 3h05m, 2d04h; '-' when unknown."""
    if seconds is None:
        return '-'
    s = int(seconds)
    if s < 90:
        return f'{s}s'
    if s < 5400:
        return f'{s // 60}m'
    if s < 172800:
        return f'{s // 3600}h{(s % 3600) // 60:02d}m'
    return f'{s // 86400}d{(s % 86400) // 3600:02d}h'


def table(header, rows):
    rows = [header, *rows]
    widths = [max(len(str(r[i])) for r in rows) for i in range(len(header))]
    return ['  ' + '  '.join(str(c).ljust(w) if i == 0 else str(c).rjust(w)
                             for i, (c, w) in enumerate(zip(r, widths))).rstrip() for r in rows]


def render_stats(st):
    when = lambda t: time.strftime('%m-%d %H:%M', time.localtime(t))
    out = [f'fleetq stats: last {st["days"]:.3g}d ({when(st["since"])} to {when(st["until"])}); '
           'waits are start minus submission (or booked start)']
    rows = [(h, d['jobs'], f'{d["busy_h"]:.1f}h', f'{d["busy_pct"]:.0f}%', f'{d["shared_pct"]:.0f}%',
             dur(d['wait']['median_s']), dur(d['wait']['p90_s']), d['backfilled'],
             d['with_deadline'], d['cancelled_waiting']) for h, d in st['hosts'].items()]
    out += ['', 'Hosts', *table(('host', 'jobs', 'busy', 'busy%', 'shared%', 'wait p50', 'p90',
                                 'backfilled', 'deadline', 'gave up'), rows)]
    longest = [(h, w) for h, d in st['hosts'].items() for w in d['longest_waits']]
    if longest:
        out += ['', 'Longest waits']
        for h, w in sorted(longest, key=lambda hw: -hw[1]['wait_s']):
            b = w['blocker']
            why = f'slot freed by #{b["id"]} "{b["label"]}" ({b["owner"]})' if b else w['reason']
            out.append(f'  {dur(w["wait_s"]):>6}  {h}  #{w["id"]} "{w["label"]}" ({w["owner"]})'
                       + (f'  {why}' if why else ''))
    if st['owners']:
        rows = [(o, d['jobs'], f'{d["held_h"]:.1f}h', dur(d['wait']['median_s']), dur(d['wait']['p90_s']))
                for o, d in sorted(st['owners'].items(), key=lambda od: -od[1]['held_h'])]
        out += ['', 'Owners (held = hours x hosts)', *table(('owner', 'jobs', 'held', 'wait p50', 'p90'), rows)]
    rows = [(k, d['n'], '-' if d['median_ratio'] is None else f'{d["median_ratio"]:.2f}',
             '-' if d['overdue_pct'] is None else f'{d["overdue_pct"]:.0f}%')
            for k, d in st['expect'].items()]
    out += ['', '--expect accuracy (finished jobs; ratio = actual / expected)',
            *table(('kind', 'n', 'median ratio', 'overdue'), rows)]
    if st['labels']:
        rows = [(d['prefix'], d['n'], dur(d['median_s']), dur(d['p90_s'])) for d in st['labels']]
        out += ['', 'Duration by label prefix (3+ runs)', *table(('prefix', 'n', 'median', 'p90'), rows)]
    return '\n'.join(out)


def cmd_stats(client, args):
    query = f'days={args.days:g}' + (f'&host={args.host}' if args.host else '')
    st = client.call('GET', f'/v1/stats?{query}')
    print(json.dumps(st, indent=2) if args.json else render_stats(st))
    return 0


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
            busy_note = f' · {s["in_flight"]} in flight' if s['in_flight'] else ''
            lead = f'    {mark} {s["name"]:<22} {roles:<26} {s["mem_gb"]:>4g} GB  '
            out = block(short(s['text']) + busy_note, lead)
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
            facts += load_facts(j)
            if j.get('deadline_in_s') is not None:
                facts.append(f'must end within {span(max(j["deadline_in_s"], 0))}')
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
        if h.get('backfill') and h['backfill']['fits_s'] >= 60:
            b = h['backfill']
            what = 'reservation #' if b['reservation'] else '#'
            lines += block(f'run jobs with --expect up to {span(b["fits_s"])} fit before {what}{b["before"]} '
                           f'(starts {local_time(b["start_in_s"], now)})', '  backfill  ', style='2')
        if h.get('reservations'):
            lines.append(paint('36', '  reserved'))
        for j in h.get('reservations', []):
            lead = f'    #{j["id"]}  '
            facts = [j['owner'], f'{local_time(j["start_in_s"], now)} for {span(j["expect_s"] or 0)}']
            gb = (j.get('mem_map') or {}).get(h['name'], j['mem_gb'])
            if j['exclusive']:
                facts.append('exclusive')
            if gb:
                facts.append(f'{gb:g} GB')
            if j['takes_down']:
                facts.append('takes down ' + ', '.join(j['takes_down']))
            facts += load_facts(j)
            if j['start_in_s'] <= 0:
                facts.append('due: ' + short(j['reason']))
            lines += block(j['label'], lead)
            lines += block(' · '.join(facts), ' ' * len(lead), style='2')
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
                   help='GB this job adds on each host (default 0), or per host: gpu-a=100,gpu-b=90')
    p.add_argument('--exclusive', action='store_true', help='nothing else may run on the host meanwhile')
    p.add_argument('--uses', action='append', help='roles/services this job needs up (shared)')
    p.add_argument('--takes-down', action='append', dest='takes_down',
                   help='roles/services this job stops; they must be restored afterwards')
    p.add_argument('--label', required=True, help='what this is, shown to everyone waiting')
    p.add_argument('--note', help='optional detail')
    p.add_argument('--serves', action='append',
                   help='model server URL this job brings up (e.g. http://gpu-a:8001/v1); fleetq models '
                        'lists what it serves while the job runs (repeatable)')
    p.add_argument('--caps', action='append',
                   help='what the models at --serves can do: text, image, video, audio, embed, rerank')
    p.add_argument('--notify', metavar='URL',
                   help='tell this URL when the job starts, goes overdue, finishes...: a webhook (JSON POST) '
                        'or an ntfy.sh topic like https://ntfy.sh/my-topic')
    add_load_args(p)


def add_load_args(p):
    p.add_argument('--load', action='append',
                   help='what this job loads heavily: gpu, cpu, net, disk (default gpu; none for nothing)')
    p.add_argument('--quiet', action='append',
                   help='what no other job may load while this runs: gpu, cpu, net, disk (e.g. a benchmark: gpu,cpu)')


def add_start_args(p):
    p.add_argument('--at', help='reserved start: 14:30, "tomorrow 09:00" or 2026-10-23T14:30 (local time)')
    p.add_argument('--in', dest='in_', help='reserved start from now: 90m, 2h, 1h30m')


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
    p.add_argument('--expect', type=float,
                   help='expected minutes; lets it go ahead of a waiting job if it fits (it is then stopped '
                        'at that job\'s start)')
    add_start_args(p)
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

    p = sub.add_parser('reserve', help='book a slot for later; others use the box until then')
    add_job_args(p)
    add_start_args(p)
    p.add_argument('--for', dest='length', required=True, help='how long: 90m, 2h, 1h30m')
    p.add_argument('--wait', action='store_true', help='block until the reserved slot starts')
    p.add_argument('--wait-timeout', type=float, help='minutes to wait (with --wait)')
    p.add_argument('--json', action='store_true')
    p.set_defaults(fn=cmd_reserve)

    p = sub.add_parser('update', help='lower a job\'s claims at once: fewer --load/--quiet words, less --mem')
    p.add_argument('id', type=int)
    add_load_args(p)
    p.add_argument('--mem', action='append', help='GB, or per host: gpu-a=100,gpu-b=90')
    p.add_argument('--force', action='store_true', help='update a job someone else owns')
    p.add_argument('--json', action='store_true')
    p.set_defaults(fn=cmd_update)

    p = sub.add_parser('when', help='would a job like this start now, and if not, when?')
    p.add_argument('--host', action='append')
    p.add_argument('--for-url', dest='for_url')
    p.add_argument('--mem', action='append')
    p.add_argument('--exclusive', action='store_true')
    p.add_argument('--uses', action='append')
    p.add_argument('--takes-down', action='append', dest='takes_down')
    add_load_args(p)
    p.add_argument('--expect', type=float, help='expected minutes')
    p.add_argument('--lease', action='store_true', help='as a lease (acquire) rather than a run job')
    p.add_argument('--json', action='store_true')
    p.set_defaults(fn=cmd_when, label='when', note='')

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

    p = sub.add_parser('stats', help='queue waits, busy time and --expect accuracy over recent days')
    p.add_argument('--days', type=float, default=7)
    p.add_argument('--host', help='only jobs on this host')
    p.add_argument('--json', action='store_true')
    p.set_defaults(fn=cmd_stats)

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
