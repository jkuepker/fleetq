"""Scheduling rules for fleetq.

Pure functions over plain data: no clock reads, no I/O. The server feeds in the
roster, live observations and jobs; these functions decide what may start and
explain, in words an operator can act on, why everything else is waiting.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import ipaddress
from urllib.parse import urlparse

# Job states
QUEUED, RUNNING, DONE, CANCELLED = 'queued', 'running', 'done', 'cancelled'
ACTIVE = (QUEUED, RUNNING)

# What a model can be used for. text/image/video/audio: inputs a chat model accepts;
# embed and rerank: models that only do that task. Servers do not report this (vLLM's
# /models has no modality field), so the roster or the job that serves the model declares it.
CAPABILITIES = ('text', 'image', 'video', 'audio', 'embed', 'rerank')


@dataclass
class Host:
    """A schedulable memory pool: a whole unified-memory box, or one GPU of a box."""
    name: str
    machine: str
    capacity_gb: float
    aliases: list = field(default_factory=list)
    note: str = ''
    addresses: list = field(default_factory=list)  # LAN / tailnet IPs of the machine
    default: bool = False  # ad-hoc URLs on a multi-pool machine land here (e.g. the benchmark GPU)


@dataclass
class Service:
    """A long-running process that occupies memory on a host."""
    name: str
    host: str
    mem_gb: float = 0.0
    url: str = ''                 # OpenAI-compatible base URL, e.g. http://spark-fa00:8000/v1
    models: list = field(default_factory=list)
    roles: list = field(default_factory=list)
    kind: str = 'fleet'           # fleet | standing | optional
    probe_tcp: str = ''           # host:port liveness check when there is no model URL
    note: str = ''
    capabilities: list = field(default_factory=list)        # for every model it serves
    model_capabilities: dict = field(default_factory=dict)  # model id -> capabilities, overrides the above

    @property
    def expected(self):
        return self.kind != 'optional'

    @property
    def probed(self):
        return bool(self.url or self.probe_tcp)


@dataclass
class Observation:
    """Latest health check of one service.

    state: up | unverified (answers but needs auth) | wrong (other models) |
    down | assumed (nothing to probe)
    """
    state: str
    models: list = field(default_factory=list)
    in_flight: int | None = None
    detail: str = ''
    at: float = 0.0
    progress: float | None = None     # engine step counter; None when the server does not report one
    stuck_since: float | None = None  # first check that saw requests in flight and this counter frozen
    served: list = field(default_factory=list)  # {id, root, max_model_len} per model, from /models


@dataclass
class Job:
    id: int
    state: str
    kind: str                     # run (CLI wrapper, heartbeats) | lease (acquire/release)
    owner: str
    label: str
    hosts: list
    mem_gb: float = 0.0
    exclusive: bool = False
    uses: list = field(default_factory=list)
    takes_down: list = field(default_factory=list)
    created_at: float = 0.0
    started_at: float | None = None
    ended_at: float | None = None
    heartbeat_at: float | None = None
    expect_s: float | None = None        # leases: expected duration, flagged overdue after
    command: str = ''
    reason: str = ''
    exit_code: int | None = None
    note: str = ''
    mem_map: dict = field(default_factory=dict)  # host -> GB where it differs from mem_gb
    serves: list = field(default_factory=list)       # model server URLs this job brings up
    serves_caps: list = field(default_factory=list)  # capabilities of the models at those URLs

    def brief(self):
        return f'#{self.id} "{self.label}" ({self.owner})'

    def mem_on(self, host):
        """GB this job adds on one of its hosts."""
        return self.mem_map.get(host, self.mem_gb)


@dataclass
class Restore:
    """A service a takedown job released and that has not been seen healthy since."""
    service: str
    job_id: int
    owner: str
    label: str
    since: float


@dataclass
class ServiceStatus:
    code: str        # up | held | restoring | not_restored | wedged | down | wrong | offline | off | unknown
    text: str
    usable: bool     # may a job --use it right now
    reserved: bool   # does its memory count against the host
    warn: bool = False


def restored(obs, restore):
    """A taken-down service counts as back only once a health check sees it after the release.

    Services without a health check never qualify; an operator confirms them with mark-up.
    """
    return obs is not None and obs.state in ('up', 'unverified') and obs.at > restore.since


def carry_stall(prev, new):
    """Set new.stuck_since from the previous check of the same service; returns new.

    A stall runs while consecutive checks both see requests in flight and an unchanged
    progress counter. Any change counts as progress (counters restart with the server).
    """
    stalled = (prev is not None and new.in_flight and prev.in_flight
               and new.progress is not None and new.progress == prev.progress)
    new.stuck_since = (prev.stuck_since if prev.stuck_since is not None else prev.at) if stalled else None
    return new


def wedged(obs, now, after_s):
    """Requests in flight while the engine has not taken a step for after_s seconds.

    Seen on vLLM: the engine freezes mid-request with no error, the client times out and
    leaves, and the server keeps reporting one request running forever.
    """
    return (obs is not None and obs.state in ('up', 'unverified') and bool(obs.in_flight)
            and obs.stuck_since is not None and now - obs.stuck_since >= after_s)


def service_status(svc, obs, running, restore, host_online, now, restore_grace_s, wedge_after_s=180):
    """Decide what a service is doing and whether jobs may rely on it."""
    holder = next((j for j in running if svc.name in j.takes_down), None)
    if holder:
        return ServiceStatus('held', f'DOWN, taken down by {holder.brief()}', False, False)
    if host_online is False:
        return ServiceStatus('offline', 'host offline', False, svc.expected, warn=svc.expected)
    if restore and not restored(obs, restore):
        who = f'#{restore.job_id} "{restore.label}" ({restore.owner})'
        if now - restore.since > restore_grace_s:
            return ServiceStatus('not_restored', f'NOT RESTORED, taken down by {who} and not back yet',
                                 False, True, warn=True)
        return ServiceStatus('restoring', f'RESTORING after {who}', False, True)
    if obs is None:
        if not svc.probed:
            return ServiceStatus('up', 'assumed up (no health check)', True, svc.expected)
        return ServiceStatus('unknown', 'not checked yet', False, svc.expected)
    if wedged(obs, now, wedge_after_s):
        return ServiceStatus('wedged', f'WEDGED, {obs.in_flight} in flight but no engine progress for '
                             f'{int(now - obs.stuck_since) // 60} min; restart it in a job with '
                             f'--takes-down {svc.name} (it goes to the front of the line)',
                             False, True, warn=True)
    if obs.state == 'up':
        return ServiceStatus('up', 'up', True, True)
    if obs.state == 'unverified':
        return ServiceStatus('up', f'up, model not verified ({obs.detail})', True, True)
    if obs.state == 'assumed':
        return ServiceStatus('up', 'assumed up (no health check)', True, svc.expected)
    if not svc.expected:
        return ServiceStatus('off', 'off (optional)', False, False)
    if obs.state == 'wrong':
        return ServiceStatus('wrong', f'WRONG MODEL, untracked: {obs.detail}', False, True, warn=True)
    return ServiceStatus('down', f'DOWN, untracked: {obs.detail}' if obs.detail else 'DOWN, untracked',
                         False, True, warn=True)


def all_statuses(services, obs, running, restores, host_online, now, restore_grace_s, wedge_after_s=180):
    return {s.name: service_status(s, obs.get(s.name), running, restores.get(s.name),
                                   host_online.get(s.host), now, restore_grace_s, wedge_after_s)
            for s in services}


def host_memory(host, services, statuses, running):
    """(reserved by services, reserved by running jobs, free) in GB, from declared sizes."""
    reserved = sum(s.mem_gb for s in services if s.host == host.name and statuses[s.name].reserved)
    jobs = sum(j.mem_on(host.name) for j in running if host.name in j.hosts)
    return reserved, jobs, host.capacity_gb - reserved - jobs


def untracked_inflight(host_name, services, obs, running, statuses=None):
    """Services on this host answering requests that no running fleetq job accounts for.

    A wedged service is not working on anything, so its stuck requests do not count here.
    """
    busy = []
    for s in services:
        o = obs.get(s.name)
        if s.host != host_name or o is None or not o.in_flight:
            continue
        if statuses and statuses[s.name].code == 'wedged':
            continue
        if any(s.name in j.uses or s.name in j.takes_down or host_name in j.hosts and j.exclusive
               for j in running):
            continue
        busy.append(f'{s.name} ({o.in_flight} in flight)')
    return busy


def blocker(job, hosts, services, statuses, obs, running, host_online, actual_free,
            actual_util=None, tolerance_gb=2.0, busy_util=0.25):
    """Why this job cannot start right now, or None if it can."""
    by_name = {s.name: s for s in services}
    for h in job.hosts:
        host = hosts[h]
        if host_online.get(h) is None:
            return f'waiting for the first health check of {h}'
        if host_online.get(h) is False:
            return f'{h} is offline'
        here = [j for j in running if h in j.hosts]
        holder = next((j for j in here if j.exclusive), None)
        if holder:
            return f'{h} is held exclusively by {holder.brief()}'
        if job.exclusive and here:
            return (f'needs {h} to itself; running there: '
                    + ', '.join(j.brief() for j in here))
        if job.exclusive:
            busy = untracked_inflight(h, services, obs, running, statuses)
            if busy:
                return (f'needs {h} quiet, but requests not tracked by fleetq are in flight: '
                        + ', '.join(busy))
            stuck = [s.name for s in services if s.host == h and statuses[s.name].code == 'wedged']
            left = [name for name in stuck if name not in job.takes_down]
            if left:
                return (f'needs {h} quiet, but {", ".join(left)} is wedged (a job that takes it down '
                        'and restarts it goes ahead of this one)')
            # A wedged engine spins the GPU; this job takes it down, so that load is about to stop.
            util = (actual_util or {}).get(h)
            if util is not None and util > busy_util and not stuck:
                return f'needs {h} quiet, but its GPU is {util:.0%} busy with work fleetq does not track'
        freed = sum(by_name[s].mem_gb for s in job.takes_down
                    if by_name[s].host == h and statuses[s].reserved)
        need = max(0.0, job.mem_on(h) - freed)
        if need > 0:
            _, _, free = host_memory(host, services, statuses, running)
            if need > free + 1e-9:
                return f'needs {need:g} GB on {h}; {max(free, 0):g} GB free of {host.capacity_gb:g}'
            actual = actual_free.get(h)
            if actual is not None and need > actual + tolerance_gb:
                return (f'needs {need:g} GB on {h}; the box reports only {actual:.0f} GB actually free '
                        f'(memory used by something fleetq does not track)')
    for name in job.uses:
        st = statuses[name]
        if not st.usable:
            return f'{name} is {st.text}'
    for name in job.takes_down:
        users = [j for j in running if name in j.uses]
        if users:
            return f'{name} is in use by ' + ', '.join(j.brief() for j in users)
    return None


def schedule(jobs, hosts, services, statuses, obs, host_online, actual_free, actual_util=None):
    """Decide which queued jobs start now.

    Strict first-come order per host: once the oldest queued job on a host cannot
    start, nothing behind it on that host starts either, so a large job is never
    starved by a stream of small ones. Ordering is global by job id, which also
    keeps multi-host jobs free of deadlock.

    One exception: a job that takes down a wedged service skips the line on that
    service's host. Jobs ahead of it may be waiting for that very service to go quiet,
    which a wedged engine never does.

    Returns (to_start, reasons): job ids to start, and a reason for every job left waiting.
    """
    running = [j for j in jobs if j.state == RUNNING]
    queued = sorted((j for j in jobs if j.state == QUEUED), key=lambda j: j.id)
    free = dict(actual_free)
    first_waiting = {}
    to_start, reasons = [], {}
    for job in queued:
        clears = {s.host for s in services if s.name in job.takes_down and statuses[s.name].code == 'wedged'}
        lines = [h for h in job.hosts if h in first_waiting and h not in clears]
        if lines:
            ahead = first_waiting[lines[0]]
            reasons[job.id] = f'in line behind {ahead.brief()} on {", ".join(lines)}'
            for h in job.hosts:
                first_waiting.setdefault(h, ahead)
            continue
        why = blocker(job, hosts, services, statuses, obs, running, host_online, free, actual_util)
        if why:
            reasons[job.id] = why
            for h in job.hosts:
                first_waiting.setdefault(h, job)
            continue
        to_start.append(job.id)
        running.append(job)
        for h in job.hosts:
            if free.get(h) is not None:
                free[h] -= job.mem_on(h)
    return to_start, reasons


def check_capabilities(caps, where):
    """caps as a list; raise ValueError on a word outside CAPABILITIES."""
    unknown = [c for c in caps if c not in CAPABILITIES]
    if unknown:
        raise ValueError(f'{where}: unknown capability {", ".join(unknown)}; known: {", ".join(CAPABILITIES)}')
    return list(caps)


def capabilities_for(svc, model_id):
    """(capabilities, source) for one model of a service; source is config or unknown."""
    if model_id in svc.model_capabilities:
        return list(svc.model_capabilities[model_id]), 'config'
    if svc.capabilities:
        return list(svc.capabilities), 'config'
    return [], 'unknown'


def model_entries(services, statuses, obs, hosts, jobs, job_obs, now, include_all=False):
    """One entry per (url, model) the fleet serves, for harnesses that pick a model.

    Only usable services count unless include_all: a held, restoring or wedged service
    still has its last observation, which would advertise models nobody can reach.
    Two services behind one URL (the ranks of a TP=2 server) make one entry.
    Running jobs add the models at their --serves URLs once a check sees them.
    """
    out, by_key = [], {}
    for s in services:
        if not s.url:
            continue
        st, o = statuses[s.name], obs.get(s.name)
        if not st.usable and not include_all:
            continue
        if o is not None and o.state in ('up', 'wrong') and (o.served or o.models):
            models, verified = o.served or [{'id': m} for m in o.models], True
        else:
            models, verified = [{'id': m} for m in s.models], False
        for m in models:
            key = (s.url.rstrip('/'), m['id'])
            if key in by_key:
                e = by_key[key]
                e['services'].append(s.name)
                e['hosts'] += [s.host] if s.host not in e['hosts'] else []
                e['roles'] += [r for r in s.roles if r not in e['roles']]
                if e['usable'] and not st.usable:
                    e['state'], e['usable'] = st.code, False
                continue
            caps, source = capabilities_for(s, m['id'])
            e = {'model': m['id'], 'url': s.url, 'service': s.name, 'services': [s.name],
                 'host': s.host, 'hosts': [s.host], 'machine': hosts[s.host].machine,
                 'roles': list(s.roles), 'state': st.code, 'usable': st.usable, 'verified': verified,
                 'capabilities': caps, 'capabilities_source': source,
                 'context': m.get('max_model_len'), 'root': m.get('root'),
                 'seen_at': o.at if o else None, 'age_s': round(now - o.at) if o else None, 'job': None}
            by_key[key] = e
            out.append(e)
    for j in jobs:
        if j.state != RUNNING:
            continue
        for url in j.serves:
            o = job_obs.get((j.id, url))
            up = o is not None and o.state in ('up', 'unverified')
            models = (o.served or [{'id': m} for m in o.models]) if up else []
            if not models and not include_all:
                continue
            base = {'url': url, 'service': None, 'services': [], 'host': j.hosts[0], 'hosts': list(j.hosts),
                    'machine': hosts[j.hosts[0]].machine, 'roles': [],
                    'capabilities': list(j.serves_caps),
                    'capabilities_source': 'job' if j.serves_caps else 'unknown',
                    'seen_at': o.at if o else None, 'age_s': round(now - o.at) if o else None,
                    'job': {'id': j.id, 'label': j.label, 'owner': j.owner}}
            if not models:
                detail = o.detail if o else 'not checked yet'
                out.append(base | {'model': None, 'state': 'starting', 'usable': False, 'verified': False,
                                   'context': None, 'root': None, 'detail': detail})
            for m in models:
                out.append(base | {'model': m['id'], 'state': 'up', 'usable': True, 'verified': o.state == 'up',
                                   'context': m.get('max_model_len'), 'root': m.get('root')})
    return out


def resolve_names(names, services, kind):
    """Map role or service names to service names; raise ValueError on unknown ones."""
    out = []
    for name in names:
        match = [s.name for s in services if s.name == name] or \
                [s.name for s in services if name in s.roles]
        if not match:
            known = sorted({s.name for s in services} | {r for s in services for r in s.roles})
            raise ValueError(f'unknown {kind} "{name}"; known: {", ".join(known)}')
        for m in match:
            if m not in out:
                out.append(m)
    return out


def resolve_hosts(names, hosts):
    """Map host names, aliases or machine names to host names."""
    out = []
    for name in names:
        match = [h.name for h in hosts.values() if name == h.name or name in h.aliases] or \
                [h.name for h in hosts.values() if name == h.machine]
        if not match:
            known = sorted(set(hosts) | {a for h in hosts.values() for a in h.aliases}
                           | {h.machine for h in hosts.values()})
            raise ValueError(f'unknown host "{name}"; known: {", ".join(known)}')
        for m in match:
            if m not in out:
                out.append(m)
    return out


def _machine_of(hostname, hosts):
    hostname = (hostname or '').lower()
    try:
        ipaddress.ip_address(hostname)
        short = hostname
    except ValueError:
        short = hostname.split('.')[0]
    for h in hosts.values():
        if short in (h.name, h.machine, *h.aliases) or hostname in h.addresses:
            return h.machine
    return None


def resolve_url(url, hosts, services):
    """Which host(s) a URL's work lands on: (host names, matching service name or None).

    Matches a known service by machine and port first. An unknown port on a machine with
    several memory pools goes to the pool marked default. ([], None) means not a fleet box.
    """
    u = urlparse(url if '://' in url else 'http://' + url)
    machine = _machine_of(u.hostname, hosts)
    if machine is None:
        return [], None
    port = u.port or (443 if u.scheme == 'https' else 80)
    for s in services:
        if s.url and hosts[s.host].machine == machine:
            su = urlparse(s.url)
            if (su.port or 80) == port and _machine_of(su.hostname, hosts) == machine:
                return [s.host], s.name
    pools = [h.name for h in hosts.values() if h.machine == machine]
    defaults = [h for h in pools if hosts[h].default]
    return (pools if len(pools) == 1 else defaults or pools), None
