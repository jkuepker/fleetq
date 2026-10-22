"""fleetq server: one queue for every job that runs on the fleet's GPU boxes."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import threading
import time
import tomllib
from urllib.parse import parse_qs, urlparse

from . import core, probes
from .core import DONE, CANCELLED, QUEUED, RUNNING, Host, Job, Restore, Service
from .store import Store

DEFAULTS = {'listen': '0.0.0.0', 'port': 8470, 'token_file': '/etc/fleetq/token',
            'db': '/var/lib/fleetq/fleetq.db', 'probe_interval_s': 30, 'stale_heartbeat_s': 180,
            'restore_grace_s': 600, 'machine_probe_port': 22, 'default_expect_min': 120,
            'untracked_gb_warn': 6, 'wedge_after_s': 180, 'fast_probe_s': 5}
MAX_WAIT_S = 30  # longest a GET /v1/jobs/<id>?wait=N holds the request open


def load_config(path):
    raw = tomllib.loads(Path(path).read_text())
    server = {**DEFAULTS, **raw.get('server', {})}
    hosts, host_cfg = {}, {}
    for h in raw.get('host', []):
        hosts[h['name']] = Host(h['name'], h.get('machine', h['name']), float(h['capacity_gb']),
                                list(h.get('aliases', [])), h.get('note', ''),
                                list(h.get('addresses', [])), bool(h.get('default', False)))
        host_cfg[h['name']] = h
    services = []
    for s in raw.get('service', []):
        if s['host'] not in hosts:
            raise ValueError(f'service {s["name"]}: unknown host {s["host"]}')
        where = f'service {s["name"]}'
        caps = core.check_capabilities(s.get('capabilities', []), where)
        per_model = {m: core.check_capabilities(c, f'{where} model {m}')
                     for m, c in s.get('model_capabilities', {}).items()}
        services.append(Service(s['name'], s['host'], float(s.get('mem_gb', 0)), s.get('url', ''),
                                list(s.get('models', [])), list(s.get('roles', [])),
                                s.get('kind', 'fleet'), s.get('probe_tcp', ''), s.get('note', ''),
                                caps, per_model))
    names = [s.name for s in services]
    if len(set(names)) != len(names):
        raise ValueError('duplicate service names')
    machines = {h.machine: host_cfg[n].get('machine_address', h.machine) for n, h in hosts.items()}
    return server, hosts, host_cfg, services, machines


class Fleet:
    """All queue state and rules. Every public method takes the lock."""

    def __init__(self, config_path, store=None, clock=time.time):
        (self.cfg, self.hosts, self.host_cfg, self.services,
         self.machines) = load_config(config_path)
        self.store = store or Store(self.cfg['db'])
        self.clock = clock
        self.lock = threading.RLock()
        # Long-polls wait on this, never on self.lock; step() bumps version after every pass.
        self.changed = threading.Condition()
        self.version = 0
        self.obs = {}             # service name -> Observation
        self.job_obs = {}         # (job id, URL) -> Observation of a model server a running job brought up
        self.machine_online = {}  # machine -> bool
        self.online_at = {}       # machine -> when machine_online was observed
        self.actual = {}          # host name -> (collector reading, observed at)
        self.last_probe = None
        self.collectors = {}
        for name, h in self.host_cfg.items():
            token_file = h.get('collector_token_file')
            if h.get('collector') and token_file and os.path.exists(token_file):
                self.collectors[name] = (h, Path(token_file).read_text().strip())

    # ---- derived views -------------------------------------------------
    def host_online(self):
        return {n: self.machine_online.get(h.machine) for n, h in self.hosts.items()}

    def _reading(self, key, now):
        fresh = 3 * self.cfg['probe_interval_s']
        return {h: r[key] for h, (r, at) in self.actual.items() if now - at < fresh and r.get(key) is not None}

    def actual_free(self, now):
        return self._reading('free_gib', now)

    def actual_util(self, now):
        return self._reading('util', now)

    def _view(self, now):
        jobs = self.store.active_jobs()
        running = [j for j in jobs if j.state == RUNNING]
        restores = self.store.restores()
        statuses = core.all_statuses(self.services, self.obs, running, restores, self.host_online(),
                                     now, self.cfg['restore_grace_s'], self.cfg['wedge_after_s'])
        return jobs, running, restores, statuses

    def _flags(self, job, now):
        flags = []
        if job.state == RUNNING and job.kind == 'run' and job.heartbeat_at \
                and now - job.heartbeat_at > self.cfg['stale_heartbeat_s']:
            flags.append(f'stale: no heartbeat for {human(now - job.heartbeat_at)}; '
                         'the client may be gone, the work may still be running')
        if job.state == RUNNING and job.expect_s and job.started_at \
                and now > job.started_at + job.expect_s:
            flags.append(f'overdue: expected {human(job.expect_s)}, running {human(now - job.started_at)}')
        return flags

    def _job_dict(self, job, now, position=None):
        d = asdict(job)
        d['flags'] = self._flags(job, now)
        d['roles_taken_down'] = sorted({r for s in self.services if s.name in job.takes_down for r in s.roles})
        if position is not None:
            d['position'] = position
        return d

    # ---- scheduling ----------------------------------------------------
    def step(self):
        """Clear confirmed restores, drop abandoned queued runs, start whatever may start."""
        now = self.clock()
        for name, rest in self.store.restores().items():
            if core.restored(self.obs.get(name), rest):
                self.store.clear_restore(name)
                self.store.event(now, 'restored', f'{name} is back after #{rest.job_id}', rest.job_id, name)
        for job in self.store.active_jobs():
            if job.state == QUEUED and job.kind == 'run' and job.heartbeat_at \
                    and now - job.heartbeat_at > self.cfg['stale_heartbeat_s']:
                self.store.update_job(job.id, state=CANCELLED, ended_at=now,
                                      note='client stopped waiting (no heartbeat)')
                self.store.event(now, 'abandoned', f'#{job.id} dropped from the queue: its client went away', job.id)
        jobs, running, restores, statuses = self._view(now)
        to_start, reasons = core.schedule(jobs, self.hosts, self.services, statuses, self.obs,
                                          self.host_online(), self.actual_free(now), self.actual_util(now))
        for job in jobs:
            if job.id in to_start:
                self.store.update_job(job.id, state=RUNNING, started_at=now, heartbeat_at=now, reason='')
                self.store.event(now, 'started', f'#{job.id} started on {", ".join(job.hosts)}: {job.label}', job.id)
            elif job.id in reasons and reasons[job.id] != job.reason:
                self.store.update_job(job.id, reason=reasons[job.id])
        with self.changed:
            self.version += 1
            self.changed.notify_all()

    def wait_job(self, job_id, wait_s, state=None):
        """The job once its state differs from state (default: its state now) or its reason
        changes, or after wait_s seconds. Holds no Fleet lock while it waits."""
        deadline = time.monotonic() + wait_s
        with self.changed:
            seen = self.version
        first = self.get(job_id)
        if state is not None and first['state'] != state:
            return first
        job = first
        while (left := deadline - time.monotonic()) > 0:
            with self.changed:
                self.changed.wait_for(lambda: self.version != seen, left)
                seen = self.version
            job = self.get(job_id)
            if job['state'] != first['state'] or job['reason'] != first['reason']:
                break
        return job

    def apply_probe(self, obs, online, free, now, job_obs=None, full=True):
        """Store a probe's results. A result older than the one already stored is dropped:
        a slow probe that started first must not overwrite what a later one saw."""
        with self.lock:
            for name, o in obs.items():
                prev = self.obs.get(name)
                if prev is None or o.at >= prev.at:
                    self.obs[name] = core.carry_stall(prev, o)
            # A job released while its URL was being checked must not come back with it.
            live = {j.id for j in self.store.active_jobs() if j.state == RUNNING}
            merged = dict(self.job_obs)
            for k, o in (job_obs or {}).items():
                if k not in merged or o.at >= merged[k].at:
                    merged[k] = o
            self.job_obs = {k: o for k, o in merged.items() if k[0] in live}
            for m, up in online.items():
                if now >= self.online_at.get(m, now):
                    self.machine_online[m], self.online_at[m] = up, now
            for h, reading in free.items():
                if h not in self.actual or now >= self.actual[h][1]:
                    self.actual[h] = (reading, now)
            if full:
                self.last_probe = max(now, self.last_probe or now)
            self.step()

    def _job_targets(self, jobs):
        return {f'job {j.id} {url}': (j.id, url, Service(f'job {j.id} {url}', j.hosts[0], url=url))
                for j in jobs for url in j.serves}

    def _probe(self, services, targets, machines, collectors, full):
        now = self.clock()
        obs, online, free = probes.probe_all(services + [t[2] for t in targets.values()],
                                             machines, collectors, now, self.cfg['machine_probe_port'])
        job_obs = {(t[0], t[1]): obs.pop(name) for name, t in targets.items() if name in obs}
        self.apply_probe(obs, online, free, now, job_obs, full)

    def probe_now(self):
        with self.lock:
            targets = self._job_targets(j for j in self.store.active_jobs() if j.state == RUNNING)
        self._probe(self.services, targets, self.machines, self.collectors, True)

    def fast_targets(self):
        """What a job may be waiting to see come up: services restoring after a takedown,
        and --serves URLs of running jobs that no check has seen serve a model yet."""
        with self.lock:
            _, running, _, statuses = self._view(self.clock())
            services = [s for s in self.services if s.probed
                        and statuses[s.name].code in ('restoring', 'not_restored')]
            targets = {k: t for k, t in self._job_targets(running).items()
                       if (o := self.job_obs.get(t[:2])) is None or o.state not in ('up', 'unverified')}
            return services, targets

    def probe_fast(self):
        """Check just what fast_targets names; returns whether there was anything to check."""
        services, targets = self.fast_targets()
        if services or targets:
            self._probe(services, targets, {}, {}, False)
        return bool(services or targets)

    # ---- API operations ------------------------------------------------
    def resolve(self, p):
        """Canonical hosts and services for a request's --host / --for-url / --uses / --takes-down."""
        try:
            uses = core.resolve_names(p.get('uses') or [], self.services, 'role or service')
            takes = core.resolve_names(p.get('takes_down') or [], self.services, 'role or service')
            hosts = core.resolve_hosts(p.get('hosts') or [], self.hosts)
        except ValueError as exc:
            raise ApiError(400, str(exc)) from None
        mem_map = {}
        for name, gb in (p.get('mem_map') or {}).items():
            try:
                pools = core.resolve_hosts([name], self.hosts)
                gb = float(gb)
            except ValueError as exc:
                raise ApiError(400, str(exc) if 'host' in str(exc) else f'memory for {name} must be a number') from None
            if len(pools) != 1:
                raise ApiError(400, f'{name} has several memory pools ({", ".join(pools)}); give memory for each pool')
            mem_map[pools[0]] = gb
            hosts += [h for h in pools if h not in hosts]
        url_service = None
        urls = [(key, p[key]) for key in ('for_url', 'uses_url') if p.get(key)]
        urls += [('serves', u) for u in p.get('serves') or []]
        for key, url in urls:
            url_hosts, found = core.resolve_url(url, self.hosts, self.services)
            if key != 'serves':
                url_service = found
            if not url_hosts and not hosts and not uses and not takes:
                raise ApiError(422, f'{url} is not on a fleet box; nothing to queue', not_fleet=True)
            hosts += [h for h in url_hosts if h not in hosts]
            if key == 'uses_url' and url_service and url_service not in uses:
                # Using a known service through its URL is a shared lease on that service.
                uses.append(url_service)
        for name in uses + takes:
            h = next(s.host for s in self.services if s.name == name)
            if h not in hosts:
                hosts.append(h)
        if not hosts:
            raise ApiError(400, 'name a host (--host), a URL (--for-url) or a role/service (--uses / --takes-down)')
        return {'hosts': hosts, 'uses': uses, 'takes_down': takes, 'url_service': url_service, 'mem_map': mem_map}

    def submit(self, p):
        with self.lock:
            now = self.clock()
            label, owner = str(p.get('label', '')).strip(), str(p.get('owner', '')).strip()
            kind = p.get('kind', 'lease')
            if not label or not owner:
                raise ApiError(400, 'label and owner are required')
            if kind not in ('run', 'lease'):
                raise ApiError(400, 'kind must be run or lease')
            r = self.resolve(p)
            hosts, uses, takes = r['hosts'], r['uses'], r['takes_down']
            overlap = set(uses) & set(takes)
            if overlap:
                raise ApiError(400, f'cannot both use and take down {", ".join(sorted(overlap))}')
            mem, mem_map = float(p.get('mem_gb') or 0), r['mem_map']
            need = {h: mem_map.get(h, mem) for h in hosts}
            if any(gb < 0 for gb in need.values()):
                raise ApiError(400, 'memory must be >= 0')
            too_big = [f'{need[h]:g} GB on {h}' for h in hosts if need[h] > self.hosts[h].capacity_gb]
            if too_big:
                raise ApiError(400, f'can never fit: {", ".join(too_big)}')
            serves = [str(u).strip() for u in p.get('serves') or [] if str(u).strip()]
            bad = [u for u in serves if urlparse(u).scheme not in ('http', 'https') or not urlparse(u).hostname]
            if bad:
                raise ApiError(400, f'--serves takes a model server URL like http://box:8000/v1, not {bad[0]}')
            try:
                serves_caps = core.check_capabilities(p.get('serves_caps') or [], '--caps')
            except ValueError as exc:
                raise ApiError(400, str(exc)) from None
            if serves_caps and not serves:
                raise ApiError(400, '--caps describes the models at --serves URLs; give --serves too')
            expect_s = p.get('expect_min')
            if kind == 'lease' and expect_s is None:
                expect_s = self.cfg['default_expect_min']
            job = Job(0, QUEUED, kind, owner, label[:200], hosts, mem, bool(p.get('exclusive')),
                      uses, takes, created_at=now, heartbeat_at=now if kind == 'run' else None,
                      expect_s=float(expect_s) * 60 if expect_s else None,
                      command=str(p.get('command', ''))[:500], note=str(p.get('note', ''))[:500],
                      mem_map={h: gb for h, gb in mem_map.items() if gb != mem},
                      serves=serves[:8], serves_caps=serves_caps)
            self.store.add_job(job)
            self.store.event(now, 'queued', f'#{job.id} queued by {owner} on {", ".join(hosts)}: {job.label}', job.id)
            self.step()
            return self.get(job.id)

    def get(self, job_id):
        with self.lock:
            job = self.store.job(job_id)
            if not job:
                raise ApiError(404, f'no job #{job_id}')
            position = None
            if job.state == QUEUED:
                queued = [j for j in self.store.active_jobs() if j.state == QUEUED]
                position = 1 + sum(1 for j in queued if j.id < job.id and set(j.hosts) & set(job.hosts))
            return self._job_dict(job, self.clock(), position)

    def heartbeat(self, job_id):
        with self.lock:
            job = self.store.job(job_id)
            if not job:
                raise ApiError(404, f'no job #{job_id}')
            if job.state in (QUEUED, RUNNING):
                self.store.update_job(job_id, heartbeat_at=self.clock())
            return self.get(job_id)

    def release(self, job_id, p):
        with self.lock:
            now = self.clock()
            job = self.store.job(job_id)
            if not job:
                raise ApiError(404, f'no job #{job_id}')
            if job.state not in (QUEUED, RUNNING):
                return self.get(job_id)
            who = str(p.get('owner', '')).strip()
            if who != job.owner and not p.get('force'):
                raise ApiError(409, f'#{job_id} belongs to {job.owner}; pass force to release it anyway')
            note = str(p.get('note', ''))[:500]
            if job.state == QUEUED:
                self.store.update_job(job_id, state=CANCELLED, ended_at=now, note=note or job.note)
                self.store.event(now, 'cancelled', f'#{job_id} left the queue ({who})', job_id)
            else:
                code = p.get('exit_code')
                self.store.update_job(job_id, state=DONE, ended_at=now, note=note or job.note,
                                      exit_code=int(code) if code is not None else None)
                forced = ' (forced by ' + who + ')' if who != job.owner else ''
                self.store.event(now, 'done', f'#{job_id} finished{forced}: {job.label}', job_id)
                self.job_obs = {k: o for k, o in self.job_obs.items() if k[0] != job_id}
                for name in job.takes_down:
                    self.store.add_restore(Restore(name, job_id, job.owner, job.label, now))
                    self.store.event(now, 'restore-pending', f'{name} must come back after #{job_id}', job_id, name)
            self.step()
            return self.get(job_id)

    def mark_up(self, service, p):
        with self.lock:
            if service not in {s.name for s in self.services}:
                raise ApiError(404, f'no service {service}')
            now = self.clock()
            self.store.clear_restore(service)
            self.store.event(now, 'marked-up', f'{service} confirmed back by {p.get("owner", "?")}: '
                             f'{p.get("note", "")}'.rstrip(': '), service=service)
            self.step()
            return self.status()

    def status(self):
        with self.lock:
            now = self.clock()
            jobs, running, restores, statuses = self._view(now)
            online, actual, util = self.host_online(), self.actual_free(now), self.actual_util(now)
            queued = [j for j in jobs if j.state == QUEUED]
            out, warnings = [], []
            for name, host in self.hosts.items():
                reserved, jobs_gb, free = core.host_memory(host, self.services, statuses, running)
                svcs = []
                for s in (s for s in self.services if s.host == name):
                    st, o = statuses[s.name], self.obs.get(s.name)
                    svcs.append({'name': s.name, 'roles': s.roles, 'kind': s.kind, 'mem_gb': s.mem_gb,
                                 'status': st.code, 'text': st.text, 'warn': st.warn, 'url': s.url,
                                 'served': (o.served or [{'id': m} for m in o.models]) if o else [],
                                 'seen_at': o.at if o else None,
                                 'in_flight': o.in_flight if o else None,
                                 'stuck_s': round(now - o.stuck_since) if o and o.stuck_since else None,
                                 'note': s.note})
                    if st.warn:
                        warnings.append(f'{s.name} on {name}: {st.text}')
                if name in actual and free - actual[name] > self.cfg['untracked_gb_warn']:
                    warnings.append(f'{name}: about {free - actual[name]:.0f} GB in use that fleetq does not '
                                    f'track (box reports {actual[name]:.0f} GB free, fleetq expects {free:g})')
                if name in util and util[name] > 0.25 and not any(name in j.hosts for j in running):
                    warnings.append(f'{name}: GPU {util[name]:.0%} busy with no fleetq job running there')
                busy = core.untracked_inflight(name, self.services, self.obs, running, statuses)
                if busy:
                    warnings.append(f'{name}: untracked requests in flight on {", ".join(busy)}')
                here_q = [j for j in queued if name in j.hosts]
                out.append({'name': name, 'machine': host.machine, 'aliases': host.aliases,
                            'online': online.get(name), 'capacity_gb': host.capacity_gb,
                            'services_gb': reserved, 'jobs_gb': jobs_gb, 'free_gb': free,
                            'actual_free_gb': round(actual[name], 1) if name in actual else None,
                            'gpu_util': util.get(name),
                            'collector': name in self.collectors, 'untracked_busy': busy,
                            'services': svcs,
                            'running': [self._job_dict(j, now) for j in running if name in j.hosts],
                            'queued': [self._job_dict(j, now, i + 1) for i, j in enumerate(here_q)]})
            for j in running:
                for flag in self._flags(j, now):
                    warnings.append(f'{j.brief()}: {flag}')
            return {'now': now, 'last_probe': self.last_probe, 'hosts': out,
                    'warnings': list(dict.fromkeys(warnings)),
                    'recent': [self._job_dict(j, now) for j in self.store.recent_jobs(8)]}

    def models(self, include_all=False, caps=()):
        """What a harness can send requests to: one entry per (url, model), see core.model_entries."""
        with self.lock:
            now = self.clock()
            jobs, _, _, statuses = self._view(now)
            entries = core.model_entries(self.services, statuses, self.obs, self.hosts, jobs,
                                         self.job_obs, now, include_all)
            try:
                core.check_capabilities(caps, 'cap')
            except ValueError as exc:
                raise ApiError(400, str(exc)) from None
            entries = [e for e in entries if all(c in e['capabilities'] for c in caps)]
            return {'now': now, 'last_probe': self.last_probe, 'models': entries}

    def roster(self):
        return {'hosts': [asdict(h) | {'collector': n in self.collectors} for n, h in self.hosts.items()],
                'services': [asdict(s) for s in self.services]}

    def events(self, limit):
        with self.lock:
            return self.store.events(limit)


class ApiError(Exception):
    def __init__(self, code, message, **extra):
        super().__init__(message)
        self.code = code
        self.extra = extra


def human(seconds):
    seconds = int(seconds)
    if seconds < 90:
        return f'{seconds}s'
    if seconds < 5400:
        return f'{seconds // 60}m'
    return f'{seconds // 3600}h{(seconds % 3600) // 60:02d}m'


# What a read-only token may call: the dashboards' reads, nothing that changes the queue.
READONLY_ROUTES = {('status',), ('models',), ('roster',), ('events',)}


def make_handler(fleet, token, readonly_token=None):
    class Handler(BaseHTTPRequestHandler):
        server_version = 'fleetq/0.1'

        def log_message(self, fmt, *args):
            sys.stderr.write('%s %s\n' % (self.address_string(), fmt % args))

        def _send(self, code, body):
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass  # the client gave up first, e.g. a Ctrl-C during a long-poll

        def _scope(self):
            given = self.headers.get('Authorization', '')
            if hmac.compare_digest(given, 'Bearer ' + token):
                return 'full'
            if readonly_token and hmac.compare_digest(given, 'Bearer ' + readonly_token):
                return 'read'
            return None

        def _body(self):
            length = int(self.headers.get('Content-Length') or 0)
            if length > 65536:
                raise ApiError(413, 'request too large')
            try:
                return json.loads(self.rfile.read(length) or b'{}')
            except ValueError:
                raise ApiError(400, 'body must be JSON') from None

        def _dispatch(self, method):
            url = urlparse(self.path)
            parts = [p for p in url.path.split('/') if p]
            if method == 'GET' and parts == ['healthz']:
                return self._send(200, {'ok': True, 'last_probe': fleet.last_probe})
            scope = self._scope()
            if scope is None:
                return self._send(401, {'error': 'missing or wrong token'})
            if scope == 'read' and not (method == 'GET' and parts[:1] == ['v1']
                                        and tuple(parts[1:]) in READONLY_ROUTES):
                return self._send(403, {'error': 'read-only token: only GET /v1/status, /v1/models, '
                                                 '/v1/roster, /v1/events and /healthz'})
            try:
                if parts[:1] != ['v1']:
                    raise ApiError(404, 'not found')
                route = parts[1:]
                if method == 'GET' and route == ['status']:
                    return self._send(200, fleet.status())
                if method == 'GET' and route == ['models']:
                    q = parse_qs(url.query)
                    caps = [c for v in q.get('cap', []) for c in v.split(',') if c]
                    return self._send(200, fleet.models(q.get('all', ['0'])[0] not in ('0', ''), caps))
                if method == 'GET' and route == ['roster']:
                    return self._send(200, fleet.roster())
                if method == 'GET' and route == ['events']:
                    limit = int(parse_qs(url.query).get('limit', ['30'])[0])
                    return self._send(200, {'events': fleet.events(min(max(limit, 1), 500))})
                if method == 'POST' and route == ['probe']:
                    fleet.probe_now()
                    return self._send(200, fleet.status())
                if method == 'POST' and route == ['resolve']:
                    with fleet.lock:
                        return self._send(200, fleet.resolve(self._body()))
                if method == 'POST' and route == ['jobs']:
                    return self._send(200, fleet.submit(self._body()))
                if len(route) >= 2 and route[0] == 'jobs' and route[1].isdigit():
                    job_id = int(route[1])
                    if method == 'GET' and len(route) == 2:
                        q = parse_qs(url.query)
                        if 'wait' not in q:
                            return self._send(200, fleet.get(job_id))
                        try:
                            wait_s = min(max(float(q['wait'][0]), 0.0), MAX_WAIT_S)
                        except ValueError:
                            raise ApiError(400, 'wait must be a number of seconds') from None
                        state = q.get('state', [None])[0]
                        return self._send(200, fleet.wait_job(job_id, wait_s, state) | {'waited': wait_s})
                    if method == 'POST' and route[2:] == ['heartbeat']:
                        return self._send(200, fleet.heartbeat(job_id))
                    if method == 'POST' and route[2:] == ['release']:
                        return self._send(200, fleet.release(job_id, self._body()))
                if method == 'POST' and len(route) == 3 and route[0] == 'services' and route[2] == 'mark-up':
                    return self._send(200, fleet.mark_up(route[1], self._body()))
                raise ApiError(404, 'not found')
            except ApiError as exc:
                return self._send(exc.code, {'error': str(exc), **exc.extra})

        def do_GET(self):
            self._dispatch('GET')

        def do_POST(self):
            self._dispatch('POST')

    return Handler


def probe_loop(fleet, stop):
    """A full check every probe_interval_s; in between, every fast_probe_s, a check of just
    what a job may be waiting to see come up (see Fleet.fast_targets)."""
    full_s, fast_s = fleet.cfg['probe_interval_s'], fleet.cfg['fast_probe_s']
    next_full = 0.0
    while not stop.is_set():
        try:
            if time.monotonic() >= next_full:
                next_full = time.monotonic() + full_s
                fleet.probe_now()
            elif fast_s:
                fleet.probe_fast()
        except Exception as exc:  # keep the loop alive; the next cycle retries
            sys.stderr.write(f'probe cycle failed: {exc!r}\n')
        left = next_full - time.monotonic()
        stop.wait(min(fast_s, left) if fast_s and fast_s < full_s else left)


def read_token(parser, path):
    path = Path(path)
    if path.stat().st_mode & 0o077:
        parser.error(f'{path} must not be readable by group or others')
    token = path.read_text().strip()
    if len(token) < 20:
        parser.error(f'{path}: token must be at least 20 characters')
    return token


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=os.environ.get('FLEETQ_CONFIG', '/etc/fleetq/fleet.toml'))
    parser.add_argument('--check', action='store_true', help='validate the config and exit')
    args = parser.parse_args(argv)
    if args.check:
        _, hosts, _, services, machines = load_config(args.config)
        print(f'ok: {len(hosts)} hosts on {len(machines)} machines, {len(services)} services')
        return 0
    fleet = Fleet(args.config)
    token = read_token(parser, fleet.cfg['token_file'])
    readonly_token = None
    if fleet.cfg.get('readonly_token_file'):
        readonly_token = read_token(parser, fleet.cfg['readonly_token_file'])
        if readonly_token == token:
            parser.error('readonly_token_file must hold a different token than token_file')
    stop = threading.Event()
    threading.Thread(target=probe_loop, args=(fleet, stop), daemon=True).start()
    server = ThreadingHTTPServer((fleet.cfg['listen'], fleet.cfg['port']), make_handler(fleet, token, readonly_token))
    sys.stderr.write(f'fleetq listening on {fleet.cfg["listen"]}:{fleet.cfg["port"]}\n')
    try:
        server.serve_forever()
    finally:
        stop.set()
    return 0


if __name__ == '__main__':
    sys.exit(main())
