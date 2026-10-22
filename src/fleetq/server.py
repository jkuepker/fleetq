"""fleetq server: one queue for every job that runs on the fleet's GPU boxes."""
from __future__ import annotations

import argparse
from dataclasses import asdict
from fnmatch import fnmatchcase
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

from . import core, learn, notify, probes, stats
from .core import ACTIVE, DONE, CANCELLED, QUEUED, RUNNING, Host, Job, Restore, Service
from .store import Store

DEFAULTS = {'listen': '0.0.0.0', 'port': 8470, 'token_file': '/etc/fleetq/token',
            'db': '/var/lib/fleetq/fleetq.db', 'probe_interval_s': 30, 'stale_heartbeat_s': 180,
            'restore_grace_s': 600, 'machine_probe_port': 22, 'default_expect_min': 120,
            'untracked_gb_warn': 6, 'wedge_after_s': 180, 'events_keep_days': 180,
            'unclaimed_after_s': 600, 'fast_probe_s': 5, 'noshow_after_s': 900,
            'job_notify': False, 'job_notify_hosts': ['ntfy.sh'], 'learn_days': 30,
            'learn_min_samples': 3, 'learn_quantile': 0.8, 'learn_factor': 1.25}
MAX_STATS_DAYS = 3650
MAX_WAIT_S = 30  # longest a GET /v1/jobs/<id>?wait=N holds the request open
LEARN_CACHE_S = 300  # how long finished-job history is reused between submits


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
    server['notify'] = notify.parse_rules(raw.get('notify', []))
    if not isinstance(server['job_notify'], bool):
        raise ValueError(f'server job_notify must be true or false, not {server["job_notify"]!r}')
    hooks = server['job_notify_hosts']
    if not (isinstance(hooks, list) and all(isinstance(h, str) and h for h in hooks)):
        raise ValueError('server job_notify_hosts must be a list of host names or globs like ["ntfy.sh"]')
    server['job_notify_hosts'] = [h.lower() for h in hooks]
    machines = {h.machine: host_cfg[n].get('machine_address', h.machine) for n, h in hosts.items()}
    return server, hosts, host_cfg, services, machines


class Fleet:
    """All queue state and rules. Every public method takes the lock."""

    def __init__(self, config_path, store=None, clock=time.time):
        (self.cfg, self.hosts, self.host_cfg, self.services,
         self.machines) = load_config(config_path)
        self.store = store or Store(self.cfg['db'])
        self.clock = clock
        self.notifier = notify.Notifier(self.cfg['notify'])
        self.store.on_commit = self._publish
        self.lock = threading.RLock()
        # Long-polls wait on this, never on self.lock; step() bumps version after every pass.
        self.changed = threading.Condition()
        self.version = 0
        self.obs = {}             # service name -> Observation
        self.obs_n = {}           # service name -> checks seen since this server started
        self.job_obs = {}         # (job id, URL) -> Observation of a model server a running job brought up
        self.machine_online = {}  # machine -> bool
        self.online_at = {}       # machine -> when machine_online was observed
        self.actual = {}          # host name -> (collector reading, observed at)
        self.last_probe = None
        self.last_prune = None
        self.stepped_at = None    # server clock at the last scheduling pass
        self.learned = {}         # job kind -> (read at, learn.index of its finished jobs)
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

    def _plan(self, now, jobs, statuses):
        return core.plan(jobs, self.hosts, self.services, statuses, self.obs, self.host_online(),
                         self.actual_free(now), self.actual_util(now), now, self.cfg['default_expect_min'] * 60)

    def _context(self, now):
        """What _flags needs about the whole queue, fetched once per status or job view."""
        jobs, running, _, statuses = self._view(now)
        due = [j for j in jobs if j.state == QUEUED and j.start_at is not None and j.start_at <= now]
        return {'running': running, 'due': due, 'statuses': statuses, 'util': self.actual_util(now)}

    def _flags(self, job, now, ctx=None):
        ctx = ctx or self._context(now)
        flags = []
        if job.state == RUNNING and job.kind == 'run' and job.heartbeat_at \
                and now - job.heartbeat_at > self.cfg['stale_heartbeat_s']:
            flags.append(f'stale: no heartbeat for {human(now - job.heartbeat_at)}; '
                         'the client may be gone, the work may still be running')
        if job.state == RUNNING and job.overdue_after() and job.started_at \
                and now > job.started_at + job.overdue_after():
            flags.append(f'overdue: expected {human(job.estimate_s())}{self._guess(job)}, '
                         f'running {human(now - job.started_at)}')
        if self._unclaimed(job, now):
            flags.append(f'unclaimed: started {human(now - job.started_at)} ago, no client has checked on it '
                         f'for {human(now - job.seen_at)}; whoever waited for it may be gone')
        if self._unclaimed(job, now, reserved=True):
            flags.append(f'no-show: reserved slot started {human(now - job.started_at)} ago and nobody has '
                         f'claimed it; it lapses after {human(self.cfg["noshow_after_s"])}')
        if job.state != RUNNING:
            return flags
        if job.deadline is not None and now > job.deadline:
            flags.append(f'past its backfill deadline by {human(now - job.deadline)}; its client should have '
                         'stopped it')
        others = [o for o in ctx['running'] if o.id != job.id]
        for r in ctx['due']:
            if core.hinders(job, r, others, self.hosts, self.services, ctx['statuses']):
                flags.append(f'overrunning into reservation {r.brief()}, due {human(now - r.start_at)} ago')
        if 'gpu' not in job.loads():
            for h in job.hosts:
                util = ctx['util'].get(h)
                if util is not None and util > 0.25 \
                        and not any(h in o.hosts and 'gpu' in o.loads() for o in ctx['running']):
                    flags.append(f'GPU on {h} is {util:.0%} busy, but no job there declares --load gpu; if this '
                                 'job uses the GPU, queue it again with --load gpu')
        return flags

    @staticmethod
    def _guess(job):
        """' (learned from 9 runs)' after a learned expect, else nothing."""
        return f' (learned from {job.expect_n} runs)' if job.expect_source == core.LEARNED else ''

    def _unclaimed(self, job, now, reserved=False):
        """A running lease nobody has looked at since it started. Plain leases: last seen
        unclaimed_after_s ago. Reservations (reserved=True), booked long before they start:
        started at all and still unclaimed; they lapse after noshow_after_s (see _lapsed)."""
        if not (job.state == RUNNING and job.kind == 'lease' and job.started_at is not None
                and (job.start_at is not None) == reserved):
            return False
        if reserved:
            return job.seen_at is None or job.seen_at < job.started_at
        return (job.seen_at is not None and job.seen_at < job.started_at
                and now - job.seen_at > self.cfg['unclaimed_after_s'])

    def _lapsed(self, job, now):
        return self._unclaimed(job, now, reserved=True) and now - job.started_at > self.cfg['noshow_after_s']

    def _job_dict(self, job, now, position=None, ctx=None):
        d = asdict(job)
        d['notify'] = bool(job.notify)  # the URL can be a secret (an ntfy topic) and /v1/status is world-readable
        d['flags'] = self._flags(job, now, ctx)
        # Relative times: the client acts on them by its own clock, which may differ from the server's.
        d['start_in_s'] = job.start_at - now if job.start_at is not None else None
        d['deadline_in_s'] = job.deadline - now if job.deadline is not None else None
        d['roles_taken_down'] = sorted({r for s in self.services if s.name in job.takes_down for r in s.roles})
        if position is not None:
            d['position'] = position
        return d

    # ---- scheduling ----------------------------------------------------
    def step(self):
        """Clear confirmed restores, drop abandoned waiters, start whatever may start, flag unclaimed leases."""
        now = self.clock()
        for name, rest in self.store.restores().items():
            if core.restored(self.obs.get(name), rest):
                self.store.clear_restore(name)
                self.store.event(now, 'restored', f'{name} is back after #{rest.job_id}', rest.job_id, name)
        for job in self.store.active_jobs():
            if job.state == QUEUED and (job.kind == 'run' or job.watch) and job.heartbeat_at \
                    and now - job.heartbeat_at > self.cfg['stale_heartbeat_s']:
                self.store.update_job(job.id, state=CANCELLED, ended_at=now,
                                      note='client stopped waiting (no heartbeat)')
                self.store.event(now, 'abandoned', f'#{job.id} dropped from the queue: its client went away', job.id)
            elif self._lapsed(job, now):
                # Nobody claimed it, so it never stopped anything: nothing to wait to see restored.
                self._end(job, now, CANCELLED, 'no-show: nobody claimed the reservation', restore=False)
                self.store.event(now, 'lapsed', f'#{job.id} lapsed: nobody claimed the reserved slot within '
                                 f'{human(self.cfg["noshow_after_s"])} of its start: {job.label}', job.id)
        jobs, running, restores, statuses = self._view(now)
        p = self._plan(now, jobs, statuses)
        for job in jobs:
            if job.id in p.to_start:
                deadline = p.deadlines.get(job.id)
                self.store.update_job(job.id, state=RUNNING, started_at=now, heartbeat_at=now, reason='',
                                      deadline=deadline)
                ahead = (f' (ahead of the line: must end within {human(deadline - now)})'
                         if deadline is not None else '')
                self.store.event(now, 'started', f'#{job.id} started on {", ".join(job.hosts)}{ahead}: '
                                 f'{job.label}', job.id)
            elif job.id in p.reasons and p.reasons[job.id] != job.reason:
                self.store.update_job(job.id, reason=p.reasons[job.id])
        self._flag_events(now, restores, statuses)
        ctx = self._context(now)
        for job in ctx['running']:
            if job.deadline is not None and not core.still_protects(
                    job, self.store.active_jobs(), self.hosts, self.services, ctx['statuses']):
                job.deadline = None
                self.store.update_job(job.id, deadline=None)
                self.store.event(now, 'deadline-cleared', f'#{job.id} no longer has a deadline: nothing it went '
                                 f'ahead of is still waiting: {job.label}', job.id)
            if self._unclaimed(job, now) and not self.store.has_event(job.id, 'unclaimed'):
                self.store.event(now, 'unclaimed', f'#{job.id} started but no client has checked on it for '
                                 f'{human(now - job.seen_at)}: {job.label}', job.id)
            others = [o for o in ctx['running'] if o.id != job.id]
            hit = [r for r in ctx['due'] if core.hinders(job, r, others, self.hosts, self.services, ctx['statuses'])]
            if hit and not self.store.has_event(job.id, 'overrun'):
                self.store.event(now, 'overrun', f'#{job.id} is overrunning into reservation {hit[0].brief()}: '
                                 f'{job.label}', job.id)

        self.stepped_at = now
        with self.changed:
            self.version += 1
            self.changed.notify_all()

    def _flag_events(self, now, restores, statuses):
        """Events for states that otherwise only show as a flag, once per time the state begins. What
        already fired is read back from the events table, so a restart does not announce them again."""
        for job in self.store.active_jobs():
            if job.state != RUNNING:
                continue
            if job.kind == 'run' and job.heartbeat_at and now - job.heartbeat_at > self.cfg['stale_heartbeat_s'] \
                    and not self.store.has_event_since('stale', job.heartbeat_at, job.id):
                self.store.event(now, 'stale', f'#{job.id} has sent no heartbeat for '
                                 f'{human(now - job.heartbeat_at)}; its client may be gone: {job.label}', job.id)
            if job.overdue_after() and job.started_at and now > job.started_at + job.overdue_after() \
                    and not self.store.has_event(job.id, 'overdue'):
                self.store.event(now, 'overdue', f'#{job.id} expected {human(job.estimate_s())}{self._guess(job)}, running '
                                 f'{human(now - job.started_at)}: {job.label}', job.id)
        for name, st in statuses.items():
            if st.code == 'not_restored' and name in restores:
                rest = restores[name]
                if not self.store.has_event_since('not_restored', rest.since, service=name):
                    self.store.event(now, 'not_restored', f'{name} is not back after #{rest.job_id} '
                                     f'"{rest.label}" ({rest.owner}): {st.text}', rest.job_id, name)
            elif st.code == 'wedged':
                if self.store.last_event_kind(name, ('wedged', 'wedge-cleared')) != 'wedged':
                    self.store.event(now, 'wedged', f'{name}: {st.text}', service=name)
            elif self._unwedged(name) and self.store.last_event_kind(name, ('wedged', 'wedge-cleared')) == 'wedged':
                self.store.event(now, 'wedge-cleared', f'{name} is no longer wedged', service=name)

    def _unwedged(self, name):
        """Has a check seen the stall end: the service idle or down, or its engine moving again?
        The first check after a restart proves nothing either way: a stall in progress looks like
        a healthy busy server until a second check shows the counter still frozen."""
        o = self.obs.get(name)
        if o is None:
            return False
        if o.state not in ('up', 'unverified') or not o.in_flight:
            return True
        return self.obs_n.get(name, 0) >= 2 and o.stuck_since is None

    def _publish(self, events):
        """Hand committed events to the notifier, with the owner and notify URL of the job they concern."""
        for e in events:
            job = self.store.job(e['job']) if e['job'] else None
            url = job.notify if job and self._job_url_ok(job.notify) else ''
            self.notifier.notify(e, job.owner if job else None, url)

    def _job_url_ok(self, url):
        """May a job's own notify URL be used: job_notify is on and the host is on the allowlist.
        Checked when a job is queued and again when sending, so turning it off stops existing jobs' too."""
        if not url or not self.cfg['job_notify']:
            return False
        try:
            host = (urlparse(url).hostname or '').lower()
        except ValueError:
            return False
        return any(fnmatchcase(host, g) for g in self.cfg['job_notify_hosts'])

    def tick(self):
        """A scheduling pass with no new probe results: reservations come due, no-shows lapse."""
        with self.lock, self.store.transaction():
            self.step()

    def next_due(self):
        """Next time (server clock) a pass must run without waiting for a probe: a reservation's
        start, or the moment an unclaimed one lapses, that no pass has seen yet; None if none."""
        with self.lock:
            last, times = self.stepped_at or 0.0, []
            for j in self.store.active_jobs():
                if j.state == QUEUED and j.start_at is not None:
                    times.append(j.start_at)
                elif self._unclaimed(j, self.clock(), reserved=True):
                    times.append(j.started_at + self.cfg['noshow_after_s'] + 1)
            return min((t for t in times if t > last), default=None)

    def wait_job(self, job_id, wait_s, state=None, owner=None):
        """The job once its state differs from state (default: its state now) or its reason
        changes, or after wait_s seconds. Holds no Fleet lock while it waits."""
        deadline = time.monotonic() + wait_s
        with self.changed:
            seen = self.version
        first = self.get(job_id, seen=True, owner=owner)
        if state is not None and first['state'] != state:
            return first
        job = first
        while (left := deadline - time.monotonic()) > 0:
            with self.changed:
                self.changed.wait_for(lambda: self.version != seen, left)
                seen = self.version
            job = self.get(job_id, seen=True, owner=owner)
            if job['state'] != first['state'] or job['reason'] != first['reason']:
                break
        return job

    def apply_probe(self, obs, online, free, now, job_obs=None, full=True):
        """Store a probe's results. A result older than the one already stored is dropped:
        a slow probe that started first must not overwrite what a later one saw."""
        with self.lock, self.store.transaction():
            for name, o in obs.items():
                prev = self.obs.get(name)
                if prev is None or o.at >= prev.at:
                    self.obs[name] = core.carry_stall(prev, o)
                    self.obs_n[name] = self.obs_n.get(name, 0) + 1
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
        self.prune_events(self.clock())

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

    def prune_events(self, now):
        """Drop events older than events_keep_days, at most once a day (0 keeps everything)."""
        days = self.cfg['events_keep_days']
        with self.lock:
            if days > 0 and (self.last_prune is None or now - self.last_prune >= 86400):
                self.last_prune = now
                self.store.prune_events(now - days * 86400)

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

    def _build(self, p, now):
        """A new queued Job from a request, checked but not stored."""
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
        try:
            load = core.check_loads(['gpu'] if p.get('load') is None else p['load'], '--load')
            quiet = core.check_loads(p.get('quiet') or [], '--quiet')
        except ValueError as exc:
            raise ApiError(400, str(exc)) from None
        expect_s = p.get('expect_min')
        start_at = None
        try:
            if p.get('start_in_s') is not None:
                start_at = now + float(p['start_in_s'])
            elif p.get('start_at') is not None:
                start_at = float(p['start_at'])
            if expect_s is not None:
                expect_s = float(expect_s)
        except (TypeError, ValueError):
            raise ApiError(400, 'start_at, start_in_s and expect_min must be numbers') from None
        if expect_s is not None and expect_s <= 0:
            raise ApiError(400, '--expect / --for must be more than 0 minutes')
        if start_at is not None and start_at < now + core.RESERVE_MIN_LEAD_S:
            start_at = None  # starts now or in the past: an ordinary queued job, not a booking
        if start_at is not None:
            if expect_s is None:
                raise ApiError(400, 'a reservation needs a length: --for (reserve) or --expect (run --at)')
        source, learned_n, learned_s, refused = core.GIVEN, 0, None, None
        if expect_s is None:
            source = core.DEFAULT
            auto = None
            if p.get('expect_auto'):
                auto, refused = self.auto_expect(kind, label, owner, hosts, now)
            if auto:
                expect_s, source, learned_n = auto.seconds / 60, core.GIVEN, auto.n  # asked for: as good as said
            else:
                # A learned estimate goes in expect_learned_s only. expect_s keeps what an older scheduler
                # would read it as (the lease default, or nothing), so a rollback never stops a job on a guess.
                if kind == 'lease':
                    expect_s = self.cfg['default_expect_min']
                guess = self.learned_expect(kind, label, owner, hosts, now)
                learned_n = guess.n
                if guess.source == core.LEARNED:
                    source, learned_s = core.LEARNED, guess.seconds
        watch = kind == 'lease' and bool(p.get('watch'))
        notify_url = str(p.get('notify') or '').strip()
        if notify_url:
            if not self.cfg['job_notify']:
                raise ApiError(400, 'this server does not take per-job notify URLs (job_notify is off)')
            try:
                notify.check_url(notify_url, '--notify')
            except ValueError as exc:
                raise ApiError(400, str(exc)) from None
            if not self._job_url_ok(notify_url):
                raise ApiError(400, f'--notify may only name {", ".join(self.cfg["job_notify_hosts"])} '
                                    f'(job_notify_hosts), not {urlparse(notify_url).hostname}')
        built = Job(0, QUEUED, kind, owner, label[:200], hosts, mem, bool(p.get('exclusive')),
                   uses, takes, created_at=now, heartbeat_at=now if kind == 'run' or watch else None,
                   expect_s=float(expect_s) * 60 if expect_s else None,
                   command=str(p.get('command', ''))[:500], note=str(p.get('note', ''))[:500],
                   mem_map={h: gb for h, gb in mem_map.items() if gb != mem},
                   serves=serves[:8], serves_caps=serves_caps, watch=watch, seen_at=now,
                   load=load, quiet=quiet, start_at=start_at, notify=notify_url,
                   expect_source=source, expect_n=learned_n, expect_learned_s=learned_s)
        built.auto_refused = refused  # told to the client, not stored
        return built

    def history(self, kind, label, now):
        """Samples of finished jobs of this kind with this label's prefix (see learn.index). They are
        read once per kind and reused for LEARN_CACHE_S, so a submit costs a dictionary lookup."""
        read_at, idx = self.learned.get(kind, (None, None))
        if read_at is None or now - read_at > LEARN_CACHE_S or now < read_at:
            idx = learn.index(self.store.finished_rows(kind, now - self.cfg['learn_days'] * 86400))
            self.learned[kind] = (now, idx)
        return idx.get(stats.label_prefix(label))

    def auto_expect(self, kind, label, owner, hosts, now):
        """(Estimate, None) if --expect auto may take the learned estimate as given, else (None, why not)."""
        if self.cfg['learn_days'] <= 0:
            return None, 'learning is turned off on this server'
        return learn.auto_estimate(self.history(kind, label, now), stats.label_prefix(label), owner, hosts,
                                   self.cfg['default_expect_min'] * 60, self.cfg['learn_quantile'],
                                   self.cfg['learn_factor'])

    def learned_expect(self, kind, label, owner, hosts, now):
        """learn.Estimate for a new job from finished jobs like it. Their history is read once per
        kind and reused for LEARN_CACHE_S, so a submit costs a dictionary lookup. learn_days = 0 turns it off."""
        if self.cfg['learn_days'] <= 0:
            return learn.Estimate(None, core.DEFAULT)
        return learn.estimate(self.history(kind, label, now), owner, hosts,
                              self.cfg['default_expect_min'] * 60, self.cfg['learn_min_samples'],
                              self.cfg['learn_quantile'], self.cfg['learn_factor'])

    def submit(self, p):
        with self.lock, self.store.transaction():
            now = self.clock()
            job = self._build(p, now)
            if job.start_at is not None:
                self._check_booking(job, now)
            self.store.add_job(job)
            booked = f' for {time.strftime("%a %H:%M", time.localtime(job.start_at))}' if job.start_at else ''
            self.store.event(now, 'reserved' if booked else 'queued', f'#{job.id} {"reserved" if booked else "queued"}'
                             f' by {job.owner} on {", ".join(job.hosts)}{booked}: {job.label}', job.id)
            self.step()
            out = self.get(job.id)
            if job.auto_refused:
                out['auto_refused'] = job.auto_refused
            return out

    def _check_booking(self, job, now):
        """Refuse a reservation whose window clashes with another reservation's, or with the
        estimated window of a job already waiting in line; name free windows."""
        jobs, _, _, statuses = self._view(now)
        default_s = self.cfg['default_expect_min'] * 60
        windows = core.booked_windows(jobs, now, default_s)
        windows += core.line_windows(jobs, self._plan(now, jobs, statuses), now, default_s)
        hit = core.window_clash(job, job.start_at, job.expect_s, windows, self.hosts, self.services, statuses)
        if not hit:
            return
        culprits, why = hit
        free = core.free_windows(job, job.expect_s, windows, self.hosts, self.services, statuses, now)
        named = ', '.join(('reservation ' if b.start_at is not None else '') + b.brief()
                          + ('' if b.start_at is not None else ' (waiting in line)') for b in culprits)
        raise ApiError(409, f'collides with {named}: {why}',
                       now=now, conflicts=[b.id for b in culprits],
                       free=[{'start': s, 'until': u} for s, u in free])

    def get(self, job_id, seen=False, owner=None):
        """The job with flags and place in line; seen=True records that a client looked at it.
        A look naming someone other than the owner is not a claim, so a third party's `fleetq
        show` cannot keep an unclaimed reservation from lapsing. A look naming no owner counts:
        older clients send none, and their `fleetq wait` must keep claiming."""
        with self.lock:
            job = self.store.job(job_id)
            if not job:
                raise ApiError(404, f'no job #{job_id}')
            if owner is not None and owner != job.owner:
                seen = False
            if seen and job.state in (QUEUED, RUNNING):
                job.seen_at = self.clock()
                self.store.update_job(job_id, seen_at=job.seen_at)
            now, position = self.clock(), None
            if job.state == QUEUED and not (job.start_at is not None and job.start_at > now):
                # Line order as in core.plan: due reservations first, then by id; booked ones wait apart.
                def key(j):
                    return (j.start_at is None, j.start_at or 0, j.id)
                queued = [j for j in self.store.active_jobs() if j.state == QUEUED
                          and not (j.start_at is not None and j.start_at > now)]
                position = 1 + sum(1 for j in queued if key(j) < key(job) and set(j.hosts) & set(job.hosts))
            return self._job_dict(job, now, position)

    def heartbeat(self, job_id, owner=None):
        """A client is still there. One that names an owner other than the job's keeps nothing
        alive and claims nothing; one that names none (older clients) counts as the owner."""
        with self.lock, self.store.transaction():
            job = self.store.job(job_id)
            if not job:
                raise ApiError(404, f'no job #{job_id}')
            if job.state in (QUEUED, RUNNING) and (owner is None or owner == job.owner):
                now = self.clock()
                self.store.update_job(job_id, heartbeat_at=now, seen_at=now)
            return self.get(job_id)

    def unwatch(self, job_id):
        """The waiting client stops on purpose (Ctrl-C, --wait-timeout): the lease keeps its place."""
        with self.lock:
            job = self.store.job(job_id)
            if not job:
                raise ApiError(404, f'no job #{job_id}')
            if job.watch:
                self.store.update_job(job_id, watch=0)
            return self.get(job_id, seen=True, owner=job.owner)  # only the waiting client unwatches

    def release(self, job_id, p):
        with self.lock, self.store.transaction():
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
                forced = ' (forced by ' + who + ')' if who != job.owner else ''
                self.store.event(now, 'done', f'#{job_id} finished{forced}: {job.label}', job_id)
                self._end(job, now, DONE, note or job.note, int(code) if code is not None else None)
            self.step()
            return self.get(job_id)

    def _end(self, job, now, state, note, exit_code=None, restore=True):
        """Close a running job; what it took down must be seen back before others rely on it
        (restore=False: it never ran anything, so nothing was taken down)."""
        self.store.update_job(job.id, state=state, ended_at=now, note=note, exit_code=exit_code)
        self.job_obs = {k: o for k, o in self.job_obs.items() if k[0] != job.id}
        for name in job.takes_down if restore else []:
            self.store.add_restore(Restore(name, job.id, job.owner, job.label, now))
            self.store.event(now, 'restore-pending', f'{name} must come back after #{job.id}', job.id, name)

    def update(self, job_id, p):
        """Lower a job's claims (fewer loads or quiet needs, less memory), effective at once."""
        with self.lock, self.store.transaction():
            now = self.clock()
            job = self.store.job(job_id)
            if not job:
                raise ApiError(404, f'no job #{job_id}')
            if job.state not in ACTIVE:
                raise ApiError(409, f'#{job_id} is {job.state}; nothing to update')
            who = str(p.get('owner', '')).strip()
            if who != job.owner and not p.get('force'):
                raise ApiError(409, f'#{job_id} belongs to {job.owner}; pass force to update it anyway')
            more = '; to claim more, queue a new job (fleetq run / acquire) and release this one'
            changes, said = {}, []
            for key, what in (('load', 'load'), ('quiet', 'need quiet')):
                if p.get(key) is None:
                    continue
                try:
                    words = core.check_loads(p[key], f'--{key}')
                except ValueError as exc:
                    raise ApiError(400, str(exc)) from None
                extra = [w for w in words if w not in getattr(job, key)]
                if extra:
                    raise ApiError(409, f'can only lower claims: #{job_id} does not {what} {",".join(extra)} '
                                        f'now{more}')
                changes[key] = words
                said.append(f'{key} {",".join(words) or "none"}')
            if p.get('mem_gb') is not None or p.get('mem_map'):
                try:
                    mem = float(p['mem_gb']) if p.get('mem_gb') is not None else job.mem_gb
                    per_host = {}
                    for name, gb in (p.get('mem_map') or {}).items():
                        for h in core.resolve_hosts([name], self.hosts):
                            per_host[h] = float(gb)
                except ValueError as exc:
                    raise ApiError(400, str(exc)) from None
                if p.get('mem_gb') is None:
                    per_host = {**job.mem_map, **per_host}
                stray = [h for h in per_host if h not in job.hosts]
                if stray:
                    raise ApiError(400, f'#{job_id} is not on {", ".join(stray)}')
                need = {h: per_host.get(h, mem) for h in job.hosts}
                if any(gb < 0 for gb in need.values()):
                    raise ApiError(400, 'memory must be >= 0')
                up = [f'{need[h]:g} GB on {h} (holds {job.mem_on(h):g})' for h in job.hosts
                      if need[h] > job.mem_on(h) + 1e-9]
                if up:
                    raise ApiError(409, f'can only lower claims: {", ".join(up)}{more}')
                changes['mem_gb'] = mem
                changes['mem_map'] = {h: gb for h, gb in per_host.items() if gb != mem}
                said.append(', '.join(f'{need[h]:g} GB on {h}' for h in job.hosts))
            if not changes:
                raise ApiError(400, 'nothing to change: give --load, --quiet or --mem')
            self.store.update_job(job_id, **changes)
            self.store.event(now, 'updated', f'#{job_id} lowered its claims ({who}): {"; ".join(said)}', job_id)
            self.step()
            return self.get(job_id)

    def when(self, p):
        """Would a job like this start now, and if not, when (an estimate)? Queues nothing."""
        with self.lock:
            now = self.clock()
            job = self._build({'label': 'when', 'owner': '?', 'kind': 'run', **p}, now)
            jobs, _, restores, statuses = self._view(now)
            job.id = max((j.id for j in jobs), default=0) + 10 ** 9
            pl = self._plan(now, jobs + [job], statuses)
            out = {'now': now, 'hosts': job.hosts, 'starts_now': job.id in pl.to_start, 'reason': None,
                   'start_in_s': 0.0 if job.id in pl.to_start else None, 'deadline_in_s': None,
                   'expect_s': job.estimate_s(), 'expect_source': job.expect_source, 'expect_n': job.expect_n}
            if job.auto_refused:
                out['auto_refused'] = job.auto_refused
            deadline = pl.deadlines.get(job.id)
            if not out['starts_now']:
                out['reason'] = pl.reasons.get(job.id)
                online = self.host_online()

                def statuses_for(running, t):
                    return core.all_statuses(self.services, self.obs, running, restores, online, t,
                                             self.cfg['restore_grace_s'], self.cfg['wedge_after_s'])
                start, deadline = core.forecast(jobs + [job], job.id, self.hosts, self.services, statuses_for,
                                                self.obs, online, self.actual_free(now), self.actual_util(now),
                                                now, self.cfg['default_expect_min'] * 60)
                out['start_in_s'] = start - now if start is not None else None
            if deadline is not None:
                out['deadline_in_s'] = deadline - now
            return out

    def mark_up(self, service, p):
        with self.lock, self.store.transaction():
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
            ctx = self._context(now)
            pl = self._plan(now, jobs, statuses)
            by_id = {j.id: j for j in jobs}
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
                booked = sorted((j for j in queued if name in j.hosts and j.start_at is not None),
                                key=lambda j: j.start_at)
                here_q = [j for j in queued if name in j.hosts and not (j.start_at is not None and j.start_at > now)]
                here_q.sort(key=lambda j: (j.start_at is None, j.start_at or 0, j.id))
                # The tightest start a run job jumping the line must end before: the head of this
                # host's line (when its start can be estimated) or the next reservation.
                before = []
                head = next((by_id[i] for i in pl.line if name in by_id[i].hosts), None)
                if head is not None and pl.starts.get(head.id) is not None:
                    before.append((pl.starts[head.id], head, False))
                before += [(j.start_at, j, True) for j in booked if j.start_at > now][:1]
                backfill = None
                if before:
                    at, j, is_res = min(before, key=lambda b: b[0])
                    backfill = {'before': j.id, 'label': j.label, 'owner': j.owner, 'reservation': is_res,
                                'start_in_s': at - now, 'fits_s': max(core.backfill_fits(at, now), 0.0)}
                out.append({'name': name, 'machine': host.machine, 'aliases': host.aliases,
                            'online': online.get(name), 'capacity_gb': host.capacity_gb,
                            'services_gb': reserved, 'jobs_gb': jobs_gb, 'free_gb': free,
                            'actual_free_gb': round(actual[name], 1) if name in actual else None,
                            'gpu_util': util.get(name),
                            'collector': name in self.collectors, 'untracked_busy': busy,
                            'services': svcs,
                            'running': [self._job_dict(j, now, ctx=ctx) for j in running if name in j.hosts],
                            'queued': [self._job_dict(j, now, i + 1, ctx) for i, j in enumerate(here_q)],
                            'reservations': [self._job_dict(j, now, ctx=ctx) for j in booked],
                            'backfill': backfill})
            for j in running:
                for flag in self._flags(j, now, ctx):
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

    def stats(self, days, host=None):
        """Queue statistics over the last `days` days, for one host if given."""
        try:
            only = core.resolve_hosts([host], self.hosts) if host else []
        except ValueError as exc:
            raise ApiError(400, str(exc)) from None
        now = self.clock()
        since = now - days * 86400
        with self.lock:
            rows = self.store.stats_rows(since)
        return stats.compute(rows, now, since, list(self.hosts), only)


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
READONLY_ROUTES = {('status',), ('models',), ('roster',), ('events',), ('stats',)}


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
                                                 '/v1/roster, /v1/events, /v1/stats and /healthz'})
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
                if method == 'GET' and route == ['stats']:
                    q = parse_qs(url.query)
                    try:
                        days = float(q.get('days', ['7'])[0])
                    except ValueError:
                        days = 0.0
                    if not 0 < days <= MAX_STATS_DAYS:
                        raise ApiError(400, f'days must be a number between 0 and {MAX_STATS_DAYS}')
                    return self._send(200, fleet.stats(days, q.get('host', [''])[0] or None))
                if method == 'POST' and route == ['probe']:
                    fleet.probe_now()
                    return self._send(200, fleet.status())
                if method == 'POST' and route == ['resolve']:
                    with fleet.lock:
                        return self._send(200, fleet.resolve(self._body()))
                if method == 'POST' and route == ['jobs']:
                    return self._send(200, fleet.submit(self._body()))
                if method == 'POST' and route == ['when']:
                    return self._send(200, fleet.when(self._body()))
                if len(route) >= 2 and route[0] == 'jobs' and route[1].isdigit():
                    job_id = int(route[1])
                    if method == 'GET' and len(route) == 2:
                        q = parse_qs(url.query)
                        owner = q.get('owner', [None])[0]
                        if 'wait' not in q:
                            return self._send(200, fleet.get(job_id, seen=True, owner=owner))
                        try:
                            wait_s = min(max(float(q['wait'][0]), 0.0), MAX_WAIT_S)
                        except ValueError:
                            raise ApiError(400, 'wait must be a number of seconds') from None
                        state = q.get('state', [None])[0]
                        return self._send(200, fleet.wait_job(job_id, wait_s, state, owner) | {'waited': wait_s})
                    if method == 'POST' and route[2:] == ['heartbeat']:
                        return self._send(200, fleet.heartbeat(job_id, self._body().get('owner')))
                    if method == 'POST' and route[2:] == ['unwatch']:
                        return self._send(200, fleet.unwatch(job_id))
                    if method == 'POST' and route[2:] == ['release']:
                        return self._send(200, fleet.release(job_id, self._body()))
                    if method == 'POST' and route[2:] == ['update']:
                        return self._send(200, fleet.update(job_id, self._body()))
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
        due = None
        try:
            if time.monotonic() >= next_full:
                next_full = time.monotonic() + full_s
                fleet.probe_now()
            elif fast_s:
                fleet.probe_fast()
            # Reservations come due and no-shows lapse on time, not at the next probe.
            due = fleet.next_due()
            if due is not None and fleet.clock() >= due:
                fleet.tick()
                due = fleet.next_due()
        except Exception as exc:  # keep the loop alive; the next cycle retries
            sys.stderr.write(f'probe cycle failed: {exc!r}\n')
        left = next_full - time.monotonic()
        wait = min(fast_s, left) if fast_s and fast_s < full_s else left
        if due is not None:
            wait = min(wait, max(due - fleet.clock(), 0.0) + 0.05)
        stop.wait(wait)


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
        server, hosts, _, services, machines = load_config(args.config)
        hooks = f', {len(server["notify"])} notify targets' if server['notify'] else ''
        print(f'ok: {len(hosts)} hosts on {len(machines)} machines, {len(services)} services{hooks}')
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
