"""Scheduling rules for fleetq.

Pure functions over plain data: no clock reads, no I/O. The server feeds in the
roster, live observations and jobs; these functions decide what may start and
explain, in words an operator can act on, why everything else is waiting.
"""
from __future__ import annotations
import math

from dataclasses import dataclass, field, replace
import ipaddress
from urllib.parse import urlparse

# Job states
QUEUED, RUNNING, DONE, CANCELLED = 'queued', 'running', 'done', 'cancelled'
ACTIVE = (QUEUED, RUNNING)

# What a model can be used for. text/image/video/audio: inputs a chat model accepts;
# embed and rerank: models that only do that task. Servers do not report this (vLLM's
# /models has no modality field), so the roster or the job that serves the model declares it.
CAPABILITIES = ('text', 'image', 'video', 'audio', 'embed', 'rerank')

# What a job can load heavily (--load) or need left alone (--quiet) on its hosts.
LOADS = ('gpu', 'cpu', 'net', 'disk')
# A run job may start ahead of a waiting job or a reservation if it should end well before
# that start: now + expect * BACKFILL_SLACK + BACKFILL_MARGIN_S <= start.
BACKFILL_SLACK, BACKFILL_MARGIN_S = 1.2, 300
# A reservation must start at least this far ahead; anything sooner is an ordinary queued job,
# so a booking cannot be used to go straight to the front of the line.
RESERVE_MIN_LEAD_S = 60
# Where a job's expect_s came from: what its owner said (given), what finished jobs like it took
# (learned), or the server's flat default. Only a given expect may set a deadline: nothing is
# stopped on a guess. A learned one lives in its own field (expect_learned_s), never in expect_s,
# so a scheduler without this feature reads such a job as it always read one with no --expect.
# It is held to a margin everywhere else it is used.
GIVEN, LEARNED, DEFAULT = 'given', 'learned', 'default'
LEARNED_OVERDUE_X = 2.0  # a job with a learned expect is flagged overdue at this multiple of it
LEARNED_PROTECT_X = 1.5  # ...and starts ahead of a reservation only if this multiple of it fits


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
    url: str = ''                 # OpenAI-compatible base URL, e.g. http://gpu-b:8000/v1
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
    watch: bool = False                  # leases: a waiting client heartbeats it while queued
    seen_at: float | None = None         # last client contact (submit, heartbeat, GET)
    serves: list = field(default_factory=list)       # model server URLs this job brings up
    serves_caps: list = field(default_factory=list)  # capabilities of the models at those URLs
    load: list = field(default_factory=lambda: ['gpu'])  # what it loads heavily on its hosts
    quiet: list = field(default_factory=list)            # what no other job may load meanwhile
    start_at: float | None = None        # reservations: waits until then, then goes first in line
    deadline: float | None = None        # run jobs started ahead of a waiting job: stopped by then
    notify: str = ''                     # URL told about this job's events, besides the roster's [[notify]]
    expect_source: str = ''              # given | learned | default; '' (older rows): given if expect_s is set
    expect_n: int = 0                    # learned: how many finished jobs it comes from
    expect_learned_s: float | None = None  # learned: the server's estimate; expect_s stays the stated one or None
    keepalive: bool = False              # leases: a keepalive process heartbeats it while its session lives

    def __post_init__(self):
        if not self.expect_source:
            self.expect_source = GIVEN if self.expect_s else DEFAULT

    def learned(self):
        return self.expect_source == LEARNED and bool(self.expect_learned_s)

    def estimate_s(self):
        """Best guess of how long it runs: the learned one, else expect_s; None if neither."""
        return self.expect_learned_s if self.learned() else (self.expect_s or None)

    def overdue_after(self):
        """Seconds after its start that this job is flagged overdue, or None if it has no estimate."""
        est = self.estimate_s()
        return est * (LEARNED_OVERDUE_X if self.learned() else 1) if est else None

    def protect_s(self, default_expect_s):
        """How long to assume it runs when deciding whether it fits ahead of a reservation. A
        learned estimate gets the margin backfill_fits asks of a stated one, on top of its own
        factor, and never more than the default would."""
        if self.learned():
            return min(self.expect_learned_s * LEARNED_PROTECT_X * BACKFILL_SLACK + BACKFILL_MARGIN_S,
                       default_expect_s)
        return self.expect_s or default_expect_s

    def window_s(self, default_expect_s):
        """How long its slot is taken to last when keeping a booking clear of it: a learned
        estimate is too soft to shrink the window below the default."""
        if self.learned():
            return max(self.expect_learned_s, default_expect_s)
        return self.expect_s or default_expect_s

    def brief(self):
        return f'#{self.id} "{self.label}" ({self.owner})'

    def loads(self):
        """What this job loads: its --load, plus the GPU behind any service it sends requests to."""
        return set(self.load) | ({'gpu'} if self.uses else set())

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
    A running job that holds the host or its GPU quiet is taken to be the one sending them.
    """
    busy = []
    for s in services:
        o = obs.get(s.name)
        if s.host != host_name or o is None or not o.in_flight:
            continue
        if statuses and statuses[s.name].code == 'wedged':
            continue
        if any(s.name in j.uses or s.name in j.takes_down
               or host_name in j.hosts and (j.exclusive or 'gpu' in j.quiet) for j in running):
            continue
        busy.append(f'{s.name} ({o.in_flight} in flight)')
    return busy


def check_loads(words, where):
    """words as a list; raise ValueError on a word outside LOADS."""
    unknown = [w for w in words if w not in LOADS]
    if unknown:
        raise ValueError(f'{where}: unknown load {", ".join(unknown)}; known: {", ".join(LOADS)}')
    return list(dict.fromkeys(words))


def collision(job, other):
    """Why job and other cannot run at the same time, memory aside; None if they can.

    They collide when one needs quiet what the other loads, when either is exclusive on a
    host they share, or when one takes down a service the other uses or takes down.
    Worded from job's side; whether they collide does not depend on the order.
    """
    for name in job.takes_down:
        if name in other.uses:
            return f'{name} is in use by {other.brief()}'
        if name in other.takes_down:
            return f'{name} is also taken down by {other.brief()}'
    for name in job.uses:
        if name in other.takes_down:
            return f'{name} is DOWN, taken down by {other.brief()}'
    shared = [h for h in job.hosts if h in other.hosts]
    if not shared:
        return None
    h = shared[0]
    if other.exclusive:
        return f'{h} is held exclusively by {other.brief()}'
    if job.exclusive:
        return f'needs {h} to itself; {other.brief()} is there'
    words = [w for w in job.quiet if w in other.loads()]
    if words:
        return f'needs {",".join(words)} quiet on {h}, but {other.brief()} loads it'
    words = [w for w in other.quiet if w in job.loads()]
    if words:
        return f'{other.brief()} needs {",".join(words)} quiet on {h}, and this job loads it'
    return None


def need_gb(job, host, services, statuses):
    """GB job adds on host, less what the services it takes down there give back."""
    freed = sum(s.mem_gb for s in services
                if s.name in job.takes_down and s.host == host and statuses[s.name].reserved)
    return max(0.0, job.mem_on(host) - freed)


def room(job, others, host, services, statuses):
    """GB left on host with job and others on it, from declared sizes (negative: does not fit)."""
    return host_memory(host, services, statuses, others)[2] - need_gb(job, host.name, services, statuses)


def clash(job, others, hosts, services, statuses):
    """Why job cannot run alongside others, or None if it can.

    The one collision rule, used by the scheduler (others: running jobs), by backfill (others:
    what still runs when a waiting job is due) and by reservations (others: reservations whose
    window overlaps). A clash is a collision with any of them (see collision) or memory that
    does not fit with all of them together.
    """
    for h in job.hosts:
        here = [o for o in others if h in o.hosts]
        holder = next((o for o in here if o.exclusive), None)
        if holder:
            return f'{h} is held exclusively by {holder.brief()}'
        if job.exclusive and here:
            return f'needs {h} to itself; running there: ' + ', '.join(o.brief() for o in here)
    for o in others:
        why = collision(job, o)
        if why:
            return why
    for h in job.hosts:
        host = hosts[h]
        left = room(job, others, host, services, statuses)
        if left < -1e-9:
            need = need_gb(job, h, services, statuses)
            return f'needs {need:g} GB on {h}; {max(left + need, 0):g} GB free of {host.capacity_gb:g}'
    return None


def hinders(job, waiting, others, hosts, services, statuses):
    """Would job, running alongside others, keep waiting from starting?

    others: what may still run when waiting is due. Waiting starts once enough of them have
    ended, and which ones end first is often unknown (no --expect), so job hinders it if it
    would not fit in the room left in the tightest case: the fullest set of others that
    waiting can start alongside. Taking only "all of others" would make every job with
    memory hinder a waiting job that is short of memory now, though it fits beside job once
    the job in its way ends. If waiting cannot fit even with all others gone, job hinders it.
    """
    if collision(waiting, job):
        return True
    for h in waiting.hosts:
        gb = job.mem_on(h) if h in job.hosts else 0
        if gb > 1e-9 and gb > _tightest_room(waiting, others, hosts[h], services, statuses) + 1e-9:
            return True
    return False


def _tightest_room(waiting, others, host, services, statuses):
    """Least GB left on host when waiting starts alongside any set of others it can run with
    (negative: it does not fit even with all of them gone)."""
    stay = [o for o in others if host.name in o.hosts and not collision(waiting, o)]
    base = room(waiting, [], host, services, statuses)
    if base < -1e-9:
        return base
    mems = [o.mem_on(host.name) for o in stay]
    if not all(math.isfinite(m) for m in mems + [base]) or len(stay) > 16:
        return room(waiting, stay, host, services, statuses)   # the old, all-of-them rule
    # Largest true total of others that still leaves room for waiting (exact subset sums).
    sums = {0.0}
    for m in mems:
        sums |= {s + m for s in sums if s + m <= base + 1e-9}
    return base - max(sums)


def blocker(job, hosts, services, statuses, obs, running, host_online, actual_free,
            actual_util=None, tolerance_gb=2.0, busy_util=0.25):
    """Why this job cannot start right now, or None if it can."""
    for h in job.hosts:
        if host_online.get(h) is None:
            return f'waiting for the first health check of {h}'
        if host_online.get(h) is False:
            return f'{h} is offline'
    why = clash(job, running, hosts, services, statuses)
    if why:
        return why
    for h in job.hosts:
        if job.exclusive or 'gpu' in job.quiet:
            # Load fleetq cannot see: requests nobody queued, a wedged engine, a busy GPU.
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
            gpu_jobs = [j for j in running if h in j.hosts and 'gpu' in j.loads()]
            if util is not None and util > busy_util and not stuck and not gpu_jobs:
                return f'needs {h} quiet, but its GPU is {util:.0%} busy with work fleetq does not track'
        need = need_gb(job, h, services, statuses)
        actual = actual_free.get(h)
        if need > 0 and actual is not None and need > actual + tolerance_gb:
            return (f'needs {need:g} GB on {h}; the box reports only {actual:.0f} GB actually free '
                    f'(memory used by something fleetq does not track)')
    for name in job.uses:
        st = statuses[name]
        if not st.usable:
            return f'{name} is {st.text}'
    return None


def expected_end(job, now):
    """When a running job (or one starting now) should be done, never before now; None if unknown."""
    start = job.started_at if job.started_at is not None else now
    est = job.estimate_s()
    ends = [t for t in (start + est if est else None, job.deadline) if t is not None]
    return max(min(ends), now) if ends else None


def earliest_start(job, running, ends, hosts, services, statuses):
    """When job could start as running jobs finish at their expected ends, or None if unknown:
    something other than running jobs holds it up, or one in its way gave no --expect."""
    others = list(running)
    if clash(job, others, hosts, services, statuses) is None:
        return None
    for o in sorted(running, key=lambda o: (ends[o.id] is None, ends[o.id] or 0)):
        if ends[o.id] is None:
            return None
        others.remove(o)
        if clash(job, others, hosts, services, statuses) is None:
            return ends[o.id]
    return None


def backfill_fits(start, now):
    """Longest --expect a run job may give to start now and still end before start (seconds)."""
    return (start - now - BACKFILL_MARGIN_S) / BACKFILL_SLACK


@dataclass
class Plan:
    to_start: list                                 # job ids that start now, in order
    reasons: dict                                  # job id -> why it waits
    deadlines: dict = field(default_factory=dict)  # job id -> time a run job started ahead must end by
    starts: dict = field(default_factory=dict)     # waiting job id -> estimated start (None: unknown);
                                                   # reservations not yet due -> start_at
    line: list = field(default_factory=list)       # waiting job ids, first in line first


def _share_hint(job, hosts, services, statuses, obs, running, host_online, free, actual_util):
    """For an exclusive job that cannot start: name the --quiet that would let it start now, if
    one would. Most exclusive jobs only need the box quiet, and sharing it is the larger saving."""
    for words in (['gpu', 'cpu'], ['gpu']):
        if blocker(replace(job, exclusive=False, quiet=words), hosts, services, statuses, obs,
                   running, host_online, free, actual_util) is None:
            q = ','.join(words)
            return f'; if it only needs {q} quiet, --quiet {q} instead of --exclusive would start it now'
    return ''


def _sure_end(job, ends, now, default_expect_s):
    """When a running job is surely off its box, for deciding whether it is still there when a
    waiting job or reservation starts. A learned estimate has no deadline behind it, so it is
    held to its margin (protect_s) from its start, and once past that its end is unknown. Any
    other job ends at its expected end (see expected_end)."""
    if job.learned() and job.deadline is None:
        end = (job.started_at if job.started_at is not None else now) + job.protect_s(default_expect_s)
        return end if end > now else None
    return ends[job.id]


def _ahead_ok(job, protected, booked_ids, running, ends, starts, now, default_expect_s,
              hosts, services, statuses):
    """May job start now without holding up anything it would jump? (why not, deadline)

    protected: jobs waiting ahead of it, and reservations not yet due, on its hosts. For each
    one it would hold up (see hinders): a run job with --expect goes first only if it should
    end well before that one's start (backfill_fits), and must end by then (its deadline);
    a run job with a learned expect goes first if its margin (protect_s) ends before that
    start, with no deadline, since nothing is stopped on a guess; other jobs go before a
    reservation only if their --expect (or the default) ends first.
    A due reservation is held to its booked window, not to now: booking checked that it ends
    before later reservations start, and the scheduling pass always runs a little after its
    start. If it then runs late, that is the overrun flag, not a reason to wait.
    """
    deadline = None
    for w in protected:
        at = starts.get(w.id)
        still = [o for o in running if at is None or (e := _sure_end(o, ends, now, default_expect_s)) is None or e > at]
        if not hinders(job, w, still, hosts, services, statuses):
            continue
        booked = w.id in booked_ids
        if booked and job.start_at is not None and job.start_at + (job.expect_s or default_expect_s) <= at:
            continue
        if job.kind == 'run' and job.expect_s and job.expect_source == GIVEN:
            if at is not None and job.expect_s <= backfill_fits(at, now):
                deadline = at if deadline is None else min(deadline, at)
                continue
        elif (booked or job.kind == 'run' and job.learned()) and at is not None \
                and now + job.protect_s(default_expect_s) <= at:
            continue
        if booked:
            why = f'would still be running when reservation {w.brief()} starts'
        else:
            shared = [h for h in job.hosts if h in w.hosts] or w.hosts
            why = f'in line behind {w.brief()} on {", ".join(shared)}'
        if job.kind != 'run':
            pass
        elif at is None:
            unknown = [o for o in running if ends[o.id] is None and set(o.hosts) & set(w.hosts)]
            why += (f'; #{w.id} has no estimated start'
                    + (f' ({", ".join("#" + str(o.id) for o in unknown)} gave no --expect)' if unknown else '')
                    + ', so nothing can go first by ending before it')
        elif job.expect_source != GIVEN:
            why += f'; with --expect it may go first if it ends before {"that" if booked else "#" + str(w.id)} starts'
        return why, None
    return None, deadline


def plan(jobs, hosts, services, statuses, obs, host_online, actual_free, actual_util=None,
         now=0.0, default_expect_s=7200.0):
    """Decide which queued jobs start now, why the others wait, and when they might start.

    First-come order per host, with backfill: a job may start ahead of jobs waiting before it
    on its hosts only if it does not hold any of them up, or (run jobs with --expect) it
    should end before the first of them can start, by a deadline it is held to. A large job
    is never starved by a stream of small ones. Ordering is global by job id, which also
    keeps multi-host jobs free of deadlock.

    Reservations (start_at) wait until their time, then go first in line on their hosts.
    Until then they protect their window like a waiting job (see _ahead_ok).

    One exception: a job that takes down a wedged service skips the line on that
    service's host. Jobs ahead of it may be waiting for that very service to go quiet,
    which a wedged engine never does.
    """
    running = [j for j in jobs if j.state == RUNNING]
    ends = {j.id: expected_end(j, now) for j in running}
    queued = [j for j in jobs if j.state == QUEUED]
    booked = [j for j in queued if j.start_at is not None and j.start_at > now]
    booked_ids = {j.id for j in booked}
    order = sorted((j for j in queued if j.id not in booked_ids),
                   key=lambda j: (j.start_at is None, j.start_at or 0, j.id))
    free = dict(actual_free)
    p = Plan([], {})
    for r in booked:
        p.reasons[r.id] = 'reserved; waits for its start time'
        p.starts[r.id] = r.start_at
    waiting = []
    for job in order:
        clears = {s.host for s in services if s.name in job.takes_down and statuses[s.name].code == 'wedged'}
        ahead = [w for w in waiting if set(w.hosts) & (set(job.hosts) - clears)]
        why = blocker(job, hosts, services, statuses, obs, running, host_online, free, actual_util)
        deadline = None
        if why is None:
            protected = ahead + [r for r in booked if set(r.hosts) & set(job.hosts) or collision(job, r)]
            why, deadline = _ahead_ok(job, protected, booked_ids, running, ends, p.starts, now,
                                      default_expect_s, hosts, services, statuses)
        elif job.exclusive and not job.takes_down and not ahead:
            why += _share_hint(job, hosts, services, statuses, obs, running, host_online, free, actual_util)
        elif ahead:
            lines = [h for h in job.hosts if h in ahead[0].hosts and h not in clears]
            why = f'in line behind {ahead[0].brief()} on {", ".join(lines)}'
            held = next((o for o in running if o.exclusive and set(o.hosts) & set(lines)), None)
            if held:                                    # name what holds everyone up, not just the line
                h = next(h for h in lines if h in held.hosts)
                why = f'{h} is held exclusively by {held.brief()}; then {why}'
        if why:
            p.reasons[job.id] = why
            p.starts[job.id] = earliest_start(job, running, ends, hosts, services, statuses)
            p.line.append(job.id)
            waiting.append(job)
            continue
        p.to_start.append(job.id)
        if deadline is not None:
            p.deadlines[job.id] = deadline
        running.append(job)
        ends[job.id] = expected_end(replace(job, started_at=now, deadline=deadline), now)
        for h in job.hosts:
            if free.get(h) is not None:
                free[h] -= job.mem_on(h)
    return p


def schedule(jobs, hosts, services, statuses, obs, host_online, actual_free, actual_util=None,
             now=0.0, default_expect_s=7200.0):
    """(to_start, reasons) from plan: job ids to start now, and why every other job waits."""
    p = plan(jobs, hosts, services, statuses, obs, host_online, actual_free, actual_util, now, default_expect_s)
    return p.to_start, p.reasons


def line_windows(jobs, p, now, default_expect_s=7200.0):
    """(job, start, end) for every job waiting in line (p: its Plan): from its estimated start,
    or from now when there is no estimate, for its --expect (or the default). A new
    reservation must not land on these, or a chain of bookings could starve the line."""
    by_id = {j.id: j for j in jobs}
    out = []
    for i in p.line:
        start = max(p.starts.get(i) or now, now)
        out.append((by_id[i], start, start + by_id[i].window_s(default_expect_s)))
    return out


def still_protects(job, jobs, hosts, services, statuses):
    """Does a running job started ahead of the line still hold up a job it went ahead of: one
    queued before it (lower id) or a reservation booked before it started? If not, its
    deadline protects nobody any more."""
    running = [o for o in jobs if o.state == RUNNING and o.id != job.id]
    jumped = [w for w in jobs if w.state == QUEUED and (
        w.id < job.id or w.start_at is not None and w.created_at <= job.started_at)]
    return any(hinders(job, w, running, hosts, services, statuses) for w in jumped)


def booked_windows(jobs, now, default_expect_s=7200.0):
    """(job, start, end) for every active reservation: queued ones at their start_at, running
    ones from when they started."""
    out = []
    for j in jobs:
        if j.start_at is None or j.state not in ACTIVE:
            continue
        start = j.started_at if j.state == RUNNING else j.start_at
        out.append((j, start, start + j.window_s(default_expect_s)))
    return out


def window_clash(job, start, length, windows, hosts, services, statuses):
    """(reservations in the way, why) if job booked for [start, start+length) clashes with a
    reservation in windows (see booked_windows) while both would run; None if it fits."""
    end = start + length
    over = [(b, s, e) for b, s, e in windows if b.id != job.id and s < end and start < e]
    for t in sorted({start} | {s for _, s, _ in over if s > start}):
        present = [b for b, s, e in over if s <= t < e]
        why = clash(job, present, hosts, services, statuses)
        if why:
            culprits = [b for b in present if collision(job, b) or set(b.hosts) & set(job.hosts)]
            return culprits or present, why
    return None


def free_windows(job, length, windows, hosts, services, statuses, now, count=3):
    """Up to count (start, until) times at which job could be booked for length seconds;
    until is the start of the next reservation it would clash with, or None."""
    out = []
    for c in sorted({now} | {e for _, _, e in windows if e > now}):
        if window_clash(job, c, length, windows, hosts, services, statuses):
            continue
        later = [s for b, s, _ in windows if s >= c + length and clash(job, [b], hosts, services, statuses)]
        out.append((c, min(later) if later else None))
        if len(out) == count:
            break
    return out


FORECAST_OVERDUE_S = 300  # forecast: a job past its expected end is taken to finish this much later


def forecast(jobs, target_id, hosts, services, statuses_for, obs, host_online, actual_free,
             actual_util, now, default_expect_s=7200.0, horizon_s=14 * 86400):
    """Estimated (start, deadline) of a queued job, simulating the line forward; (None, None)
    if it does not start within horizon_s, e.g. because a service it needs is down.

    Running jobs end at their expected end (no --expect: started + default_expect_s; past
    it: FORECAST_OVERDUE_S from now). Live load fleetq does not track (untracked requests,
    GPU busy, memory the box reports used) counts only at the start; it is taken to go away.
    statuses_for(running, t) gives the service statuses with those jobs running at time t.
    """
    sim = [replace(j) for j in jobs if j.state in ACTIVE]
    calm = {k: replace(o, in_flight=0, stuck_since=None) for k, o in obs.items()}
    end = {}

    def finish(j, t):
        e = expected_end(j, t)
        if e is None:
            e = (j.started_at if j.started_at is not None else t) + default_expect_s
        return e if e > t else t + FORECAST_OVERDUE_S

    for j in sim:
        if j.state == RUNNING:
            end[j.id] = finish(j, now)
    t, first = now, True
    for _ in range(1000):
        running = [j for j in sim if j.state == RUNNING]
        p = plan(sim, hosts, services, statuses_for(running, t), obs if first else calm, host_online,
                 actual_free if first else {}, actual_util if first else {}, t, default_expect_s)
        if target_id in p.to_start:
            return t, p.deadlines.get(target_id)
        for j in sim:
            if j.id in p.to_start:
                j.state, j.started_at, j.deadline = RUNNING, t, p.deadlines.get(j.id)
                end[j.id] = finish(j, t)
        first = False
        upcoming = [end[j.id] for j in sim if j.state == RUNNING]
        upcoming += [j.start_at for j in sim if j.state == QUEUED and j.start_at and j.start_at > t]
        if not upcoming:
            return None, None
        t = min(upcoming)
        if t - now > horizon_s:
            return None, None
        for j in sim:
            if j.state == RUNNING and end[j.id] <= t:
                j.state = DONE
    return None, None


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
