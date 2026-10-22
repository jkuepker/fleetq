"""Outbound notifications: tell people when a job starts, overruns, goes stale, and so on.

Events are written by the server (see Store.event). Once the transaction that wrote them has
committed, Fleet hands each one to a Notifier, which finds who wants it and queues a POST for a
background thread. Nothing here may block or fail a request: the queue is bounded, a full queue
drops the message, and a webhook that keeps failing is logged and given up on.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from fnmatch import fnmatchcase
import json
from pathlib import Path
import queue
import sys
import threading
import time
from urllib.parse import urlparse
import urllib.request

# What a [[notify]] entry (or a job's own notify URL) hears about unless `kinds` narrows it.
DEFAULT_KINDS = ('started', 'done', 'overdue', 'stale', 'unclaimed', 'overrun', 'lapsed', 'abandoned',
                 'not_restored', 'wedged', 'wedge-cleared', 'restored')
# Every kind of event the server writes; `kinds` may name any of them.
ALL_KINDS = DEFAULT_KINDS + ('queued', 'reserved', 'cancelled', 'updated', 'restore-pending', 'marked-up',
                             'deadline-cleared')
# Kinds somebody should look at, not just hear about: ntfy delivers these with high priority.
PROBLEM_KINDS = frozenset(('overdue', 'stale', 'unclaimed', 'overrun', 'lapsed', 'abandoned', 'not_restored',
                           'wedged'))
FORMATS = ('auto', 'json', 'ntfy')
RULE_KEYS = {'url', 'kinds', 'owners', 'format', 'token_file'}
TIMEOUT_S = 5       # per socket operation; a server that drips bytes would outlast it, hence CAP_S
CAP_S = 10          # wall-clock limit for one attempt
TRIES = 3
BACKOFF_S = (1.0, 3.0)  # waits before the 2nd and 3rd try
MAX_QUEUE = 200
GIVE_UP_AFTER = 3   # messages given up on in a row before a URL is left alone for a while
COOLDOWN_S = (60, 600, 3600)  # how long: after the 3rd, 4th, 5th+ message given up on


@dataclass
class Rule:
    url: str
    fmt: str            # json | ntfy
    kinds: frozenset
    owners: tuple = ()  # globs on the job's owner; empty: any owner, jobless events included
    token: str = field(default='', repr=False)  # sent as a Bearer token; read from token_file, never logged

    def wants(self, kind, owner):
        if kind not in self.kinds:
            return False
        return not self.owners or (owner is not None and any(fnmatchcase(owner, g) for g in self.owners))


def check_url(url, where):
    """The URL if it is a plain http(s) URL with a host and a valid port, else ValueError."""
    bad = ValueError(f'{where}: url must be an http(s) URL like https://ntfy.sh/my-topic, not {url!r}')
    if not isinstance(url, str) or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in url):
        raise bad
    try:
        parsed = urlparse(url)
        parsed.port  # raises ValueError when out of range or not a number
    except ValueError:
        raise bad from None
    if parsed.scheme not in ('http', 'https') or not parsed.hostname:
        raise bad
    return url


def format_for(url):
    host = urlparse(url).hostname or ''
    return 'ntfy' if host == 'ntfy.sh' or host.endswith('.ntfy.sh') else 'json'


def parse_rules(entries):
    """Rules from the [[notify]] entries of the roster; ValueError says which entry is wrong."""
    rules = []
    for i, e in enumerate(entries, 1):
        where = f'notify entry {i}'
        if not isinstance(e, dict):
            raise ValueError(f'{where}: must be a table')
        if 'token' in e:
            raise ValueError(f'{where}: put the token in a file and name it with token_file; '
                             'tokens are not accepted inline')
        unknown = sorted(set(e) - RULE_KEYS)
        if unknown:
            raise ValueError(f'{where}: unknown key {", ".join(unknown)} (known: {", ".join(sorted(RULE_KEYS))})')
        url = check_url(e.get('url'), where)
        fmt = e.get('format', 'auto')
        if fmt not in FORMATS:
            raise ValueError(f'{where}: format must be one of {", ".join(FORMATS)}, not {fmt!r}')
        kinds = e.get('kinds', list(DEFAULT_KINDS))
        owners = e.get('owners', [])
        if isinstance(owners, str):
            owners = [owners]
        if not (isinstance(kinds, list) and kinds and all(isinstance(k, str) for k in kinds)):
            raise ValueError(f'{where}: kinds must be a non-empty list of event kinds')
        if not (isinstance(owners, list) and all(isinstance(o, str) and o for o in owners)):
            raise ValueError(f'{where}: owners must be a list of owner globs like ["alice", "ci-*"]')
        bad = [k for k in kinds if k not in ALL_KINDS]
        if bad:
            raise ValueError(f'{where}: unknown kind {", ".join(bad)} (known: {", ".join(ALL_KINDS)})')
        token = read_token(e['token_file'], where) if 'token_file' in e else ''
        rules.append(Rule(url, format_for(url) if fmt == 'auto' else fmt, frozenset(kinds), tuple(owners), token))
    return rules


def read_token(path, where):
    """The Bearer token in a token_file; ValueError names the file, never what is in it."""
    if not isinstance(path, str) or not path:
        raise ValueError(f'{where}: token_file must be a path')
    try:
        token = Path(path).expanduser().read_text().strip()
    except (OSError, UnicodeDecodeError) as exc:
        raise ValueError(f'{where}: cannot read token_file {path}: {exc.__class__.__name__}') from None
    if not token:
        raise ValueError(f'{where}: token_file {path} is empty')
    if any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in token):
        raise ValueError(f'{where}: token_file {path} must hold one token with no spaces or line breaks')
    return token


def where(url):
    """A URL for the log without its path: an ntfy topic is as good as a password."""
    try:
        p = urlparse(url)
        return f'{p.scheme}://{p.netloc}/…'
    except ValueError:
        return '<unparseable url>'


def request_for(url, fmt, event, token=''):
    """The POST for one event: JSON for webhooks, a plain-text body with a title for ntfy."""
    if fmt == 'ntfy':
        # Header values must be latin-1; the title is built from ASCII only, the text goes in the body.
        title = f'fleetq {event["kind"]}' + (f' #{event["job"]}' if event['job'] else '')
        headers = {'Title': title, 'Tags': 'warning' if event['kind'] in PROBLEM_KINDS else 'white_check_mark',
                   'Content-Type': 'text/plain; charset=utf-8'}
        if event['kind'] in PROBLEM_KINDS:
            headers['Priority'] = 'high'
    else:
        headers = {'Content-Type': 'application/json'}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    data = event['text'].encode() if fmt == 'ntfy' else json.dumps(event).encode()
    return urllib.request.Request(url, data=data, headers=headers, method='POST')


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A webhook that answers with a redirect is a failure: following one would let it aim the
    server's POST (then a GET) at some other host."""

    def redirect_request(self, *args, **kwargs):
        return None


# No environment proxies either: notifications go straight to the host that was named.
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect)


def post(url, fmt, event, token=''):
    with OPENER.open(request_for(url, fmt, event, token), timeout=TIMEOUT_S) as r:
        r.read(1024)


class Lane:
    """One bounded queue and the thread that drains it."""

    def __init__(self, name, size):
        self.name, self.queue, self.thread = name, queue.Queue(size), None


class Notifier:
    """Roster targets and per-job URLs travel in separate lanes, so a job's slow or dead URL can never
    hold back the roster's alerts. Each attempt has a wall-clock cap, and a URL whose messages keep
    being given up on is left alone for a while (1, 10, then 60 minutes)."""

    def __init__(self, rules=(), send=post, backoff=BACKOFF_S, max_queue=MAX_QUEUE, tries=TRIES, sleep=time.sleep,
                 cap_s=None, clock=time.monotonic):
        self.rules = list(rules)
        self.send, self.backoff, self.tries, self.sleep, self.clock = send, backoff, tries, sleep, clock
        self.cap_s = CAP_S if cap_s is None else cap_s
        self.lanes = {n: Lane(n, max_queue) for n in ('roster', 'job')}
        self.failed = {}  # url -> (messages given up on in a row, quiet until)
        self.lock = threading.Lock()

    def targets(self, kind, owner):
        """Distinct (url, format, token) of the roster entries that want an event."""
        out = {}
        for r in self.rules:
            if r.wants(kind, owner):
                out.setdefault(r.url, (r.fmt, r.token))
        return [(url, fmt, token) for url, (fmt, token) in out.items()]

    def notify(self, event, owner=None, job_url=''):
        """Queue an event for everyone who wants it. Never blocks, never raises: the roster's copies
        are queued first, and a bad job URL only costs the job its own copy."""
        roster = []
        try:
            roster = self.targets(event['kind'], owner)
            for url, fmt, token in roster:
                self._enqueue('roster', url, fmt, event, token)
        except Exception as exc:
            sys.stderr.write(f'notify: {exc!r}\n')
        try:
            if job_url and event['kind'] in DEFAULT_KINDS and job_url not in [t[0] for t in roster]:
                self._enqueue('job', job_url, format_for(job_url), event)
        except Exception as exc:
            sys.stderr.write(f'notify: job url: {exc!r}\n')

    def _enqueue(self, lane, url, fmt, event, token=''):
        lane = self.lanes[lane]
        with self.lock:
            if lane.thread is None:
                lane.thread = threading.Thread(target=self._run, args=(lane,), name=f'fleetq-notify-{lane.name}',
                                               daemon=True)
                lane.thread.start()
        try:
            lane.queue.put_nowait((url, fmt, event, token))
        except queue.Full:
            sys.stderr.write(f'notify: {lane.name} queue full, dropped {event["kind"]} for {where(url)}\n')

    def _run(self, lane):
        while True:
            url, fmt, event, token = lane.queue.get()
            try:
                self._deliver(url, fmt, event, token)
            except Exception as exc:
                sys.stderr.write(f'notify: {exc!r}\n')
            finally:
                lane.queue.task_done()

    def _attempt(self, url, fmt, event, token=''):
        """One send, abandoned (its thread left to die on its own) after cap_s."""
        outcome = []

        def run():
            try:
                self.send(url, fmt, event, token) if token else self.send(url, fmt, event)
                outcome.append(None)
            except Exception as exc:
                outcome.append(exc)

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        worker.join(self.cap_s)
        if not outcome:
            raise TimeoutError(f'no answer within {self.cap_s:g}s')
        if outcome[0] is not None:
            raise outcome[0]

    def _deliver(self, url, fmt, event, token=''):
        with self.lock:
            quiet_until = self.failed.get(url, (0, 0))[1]
        if self.clock() < quiet_until:
            return  # logged when the URL went quiet
        for attempt in range(1, self.tries + 1):
            try:
                self._attempt(url, fmt, event, token)
                with self.lock:
                    self.failed.pop(url, None)
                return
            except Exception as exc:
                sys.stderr.write(f'notify: {event["kind"]} to {where(url)} failed '
                                 f'(try {attempt}/{self.tries}): {self._scrub(repr(exc), token)}\n')
                if attempt < self.tries:
                    self.sleep(self.backoff[min(attempt - 1, len(self.backoff) - 1)] if self.backoff else 0)
        with self.lock:
            count = self.failed.get(url, (0, 0))[0] + 1
            wait = COOLDOWN_S[min(count - GIVE_UP_AFTER, len(COOLDOWN_S) - 1)] if count >= GIVE_UP_AFTER else 0
            self.failed[url] = (count, self.clock() + wait)
        sys.stderr.write(f'notify: gave up on {event["kind"]} for {where(url)}'
                         + (f'; leaving it alone for {wait // 60} min' if wait else '') + '\n')

    @staticmethod
    def _scrub(text, token):
        """Belt and braces: an error message that somehow echoed the token would not reach the log."""
        return text.replace(token, '***') if token else text

    def flush(self):
        """Block until everything queued so far has been delivered or given up on (for tests)."""
        for lane in self.lanes.values():
            lane.queue.join()
