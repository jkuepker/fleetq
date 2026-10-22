import pytest

from fleetq import core
from fleetq.core import QUEUED, RUNNING, Host, Job, Observation, Restore, Service

NOW = 1000.0


def roster(cap=100, svc_mem=40):
    hosts = {'a': Host('a', 'ma', cap, ['alpha']), 'b': Host('b', 'mb', cap),
             't1': Host('t1', 'tat', 12), 't2': Host('t2', 'tat', 32)}
    services = [Service('a-llm', 'a', svc_mem, url='http://a/v1', models=['m'], roles=['chat-a', 'reason-a']),
                Service('b-llm', 'b', 0, url='http://b/v1', models=['m'], roles=['embed-a']),
                Service('a-side', 'a', 0, kind='standing')]
    return hosts, services


def job(id, state=QUEUED, **kw):
    kw.setdefault('hosts', ['a'])
    return Job(id, state, kw.pop('kind', 'lease'), kw.pop('owner', 'o'), kw.pop('label', f'j{id}'), **kw)


class World:
    def __init__(self, cap=100, svc_mem=40):
        self.hosts, self.services = roster(cap, svc_mem)
        self.obs = {'a-llm': Observation('up', ['m'], 0, at=NOW - 5),
                    'b-llm': Observation('up', ['m'], 0, at=NOW - 5)}
        self.online = {'a': True, 'b': True, 't1': True, 't2': True}
        self.actual = {}
        self.restores = {}

    def statuses(self, jobs, now=NOW):
        running = [j for j in jobs if j.state == RUNNING]
        return core.all_statuses(self.services, self.obs, running, self.restores, self.online, now, 600)

    def plan(self, jobs, now=NOW):
        return core.schedule(jobs, self.hosts, self.services, self.statuses(jobs, now), self.obs,
                             self.online, self.actual)


def test_waits_until_enough_memory_frees_then_later_jobs_follow_in_order():
    w = World()  # 100 GB host, 40 held by a-llm: 60 free for jobs
    a, b = job(1, RUNNING, mem_gb=25), job(2, RUNNING, mem_gb=25)
    c, d = job(3, mem_gb=30), job(4, mem_gb=5)
    start, why = w.plan([a, b, c, d])
    assert start == []
    assert why[3] == 'needs 30 GB on a; 10 GB free of 100'
    assert why[4].startswith('in line behind #3')  # small job does not jump the big one
    a.state = core.DONE
    start, why = w.plan([a, b, c, d])
    assert start == [3, 4]  # 35 free: c takes 30, d fits in the remaining 5


def test_bigger_job_keeps_waiting_when_one_finisher_is_not_enough():
    w = World()
    a, b, c = job(1, RUNNING, mem_gb=25), job(2, RUNNING, mem_gb=25), job(3, mem_gb=45)
    a.state = core.DONE
    start, why = w.plan([a, b, c])
    assert start == [] and why[3] == 'needs 45 GB on a; 35 GB free of 100'
    b.state = core.DONE
    assert w.plan([a, b, c])[0] == [3]


def test_exclusive_needs_the_host_to_itself_and_blocks_what_comes_after():
    w = World()
    shared = job(1, RUNNING, uses=['a-llm'])
    excl, later = job(2, exclusive=True), job(3)
    start, why = w.plan([shared, excl, later])
    assert start == []
    assert 'needs a to itself' in why[2] and '#1' in why[2]
    assert why[3].startswith('in line behind #2')
    shared.state = core.DONE
    assert w.plan([shared, excl, later])[0] == [2]
    excl.state = RUNNING
    assert 'held exclusively by #2' in w.plan([excl, later])[1][3]


def test_other_hosts_are_independent():
    w = World()
    start, _ = w.plan([job(1, RUNNING, exclusive=True), job(2), job(3, hosts=['b'])])
    assert start == [3]


def test_takedown_waits_for_users_frees_memory_then_requires_restore():
    w = World()
    user = job(1, RUNNING, uses=['a-llm'])
    down = job(2, takes_down=['a-llm'], mem_gb=90)  # 90 > 60 free, but taking a-llm down frees 40
    start, why = w.plan([user, down])
    assert start == [] and why[2] == 'a-llm is in use by #1 "j1" (o)'
    user.state = core.DONE
    assert w.plan([user, down])[0] == [2]
    down.state = RUNNING
    wants = job(3, uses=['a-llm'])
    assert w.plan([down, wants])[1][3] == 'a-llm is DOWN, taken down by #2 "j2" (o)'
    # The takedown job finishes; the service must be seen healthy again after that.
    down.state = core.DONE
    w.restores = {'a-llm': Restore('a-llm', 2, 'o', 'j2', since=NOW)}
    w.obs['a-llm'] = Observation('down', detail='unreachable', at=NOW + 10)
    assert 'RESTORING after #2' in w.plan([down, wants], now=NOW + 20)[1][3]
    w.obs['a-llm'] = Observation('up', ['m'], at=NOW - 1)  # a stale "up" from before the release does not count
    assert w.plan([down, wants], now=NOW + 20)[0] == []
    w.obs['a-llm'] = Observation('up', ['m'], at=NOW + 30)
    assert core.restored(w.obs['a-llm'], w.restores['a-llm'])


def test_unrestored_service_is_flagged_after_the_grace_period():
    w = World()
    w.restores = {'a-llm': Restore('a-llm', 2, 'o', 'swap test', since=NOW)}
    w.obs['a-llm'] = Observation('down', at=NOW + 700)
    st = w.statuses([], now=NOW + 700)['a-llm']
    assert st.code == 'not_restored' and st.warn and not st.usable and st.reserved


def test_service_without_health_check_needs_manual_confirmation_to_be_restored():
    w = World()
    w.restores = {'a-side': Restore('a-side', 5, 'o', 'x', since=NOW)}
    assert not core.restored(w.obs.get('a-side'), w.restores['a-side'])
    assert w.statuses([])['a-side'].code == 'restoring'


def test_untracked_requests_block_exclusive_jobs_only():
    w = World()
    w.obs['a-llm'].in_flight = 2
    start, why = w.plan([job(1, exclusive=True)])
    assert start == [] and 'not tracked by fleetq' in why[1] and 'a-llm (2 in flight)' in why[1]
    assert w.plan([job(1)])[0] == [1]
    # In-flight work attributed to a running job that uses the service is not "untracked".
    assert core.untracked_inflight('a', w.services, w.obs, [job(9, RUNNING, uses=['a-llm'])]) == []


def stalled(in_flight, progress, at, prev=None):
    return core.carry_stall(prev, Observation('up', ['m'], in_flight, at=at, progress=progress))


def test_stall_starts_at_the_first_frozen_check_and_resets_on_any_change():
    o1 = stalled(1, 500.0, NOW)
    assert o1.stuck_since is None  # nothing to compare against yet
    o2 = stalled(1, 500.0, NOW + 30, o1)
    o3 = stalled(1, 500.0, NOW + 60, o2)
    assert o2.stuck_since == o3.stuck_since == NOW
    assert stalled(1, 501.0, NOW + 90, o3).stuck_since is None  # a step was taken
    assert stalled(1, 3.0, NOW + 90, o3).stuck_since is None    # counter reset by a restart
    assert stalled(0, 500.0, NOW + 90, o3).stuck_since is None  # idle, not stuck
    assert stalled(1, None, NOW + 90, stalled(1, None, NOW)).stuck_since is None  # no counter


def test_wedged_service_after_the_threshold_is_flagged_and_unusable():
    w = World()
    w.obs['a-llm'] = Observation('up', ['m'], 1, at=NOW, progress=500.0, stuck_since=NOW - 179)
    assert w.statuses([])['a-llm'].code == 'up'
    w.obs['a-llm'].stuck_since = NOW - 600
    st = w.statuses([])['a-llm']
    assert st.code == 'wedged' and st.warn and not st.usable and st.reserved
    assert 'no engine progress for 10 min' in st.text and '--takes-down a-llm' in st.text
    assert 'a-llm is WEDGED' in w.plan([job(1, uses=['a-llm'])])[1][1]


def test_wedged_service_blocks_other_exclusive_jobs_but_not_one_that_takes_it_down():
    w = World()
    w.obs['a-llm'] = Observation('up', ['m'], 1, at=NOW, progress=500.0, stuck_since=NOW - 600)
    assert core.untracked_inflight('a', w.services, w.obs, [], w.statuses([])) == []
    start, why = w.plan([job(1, exclusive=True)])
    assert start == [] and why[1] == ('needs a quiet, but a-llm is wedged (a job that takes it down '
                                      'and restarts it goes ahead of this one)')
    # Its spinning kernel reads as a busy GPU; a job that takes the service down ignores that.
    down = job(2, exclusive=True, takes_down=['a-llm'])
    start, _ = core.schedule([down], w.hosts, w.services, w.statuses([down]), w.obs, w.online, {}, {'a': 0.96})
    assert start == [2]


def test_live_requests_still_block_a_job_that_takes_the_service_down():
    w = World()
    w.obs['a-llm'] = Observation('up', ['m'], 1, at=NOW, progress=500.0, stuck_since=NOW - 60)
    start, why = w.plan([job(1, exclusive=True, takes_down=['a-llm'])])
    assert start == [] and 'a-llm (1 in flight)' in why[1]  # someone may be mid-request


def test_takedown_of_a_wedged_service_goes_to_the_front_of_that_hosts_line():
    w = World()
    w.obs['a-llm'] = Observation('up', ['m'], 1, at=NOW, progress=500.0, stuck_since=NOW - 600)
    bench = job(1, exclusive=True)                      # waits for a to go quiet
    other = job(2, hosts=['a'], mem_gb=5)               # ordinary job: stays behind #1
    restart = job(3, takes_down=['a-llm'])              # clears the wedge: skips the line
    both = job(4, hosts=['a', 'b'], takes_down=['a-llm'])
    b_first = job(5, RUNNING, hosts=['b'], exclusive=True)
    start, why = w.plan([bench, other, restart])
    assert start == [3] and why[2].startswith('in line behind #1')
    # The skip is per host: on b, where nothing is wedged, #4 still waits its turn.
    b_line = job(0, hosts=['b'], exclusive=True)
    start, why = w.plan([b_line, bench, both, b_first])
    assert 4 not in start and why[4].startswith('in line behind #0') and why[4].endswith('on b')


def test_offline_host_and_unchecked_host_block():
    w = World()
    w.online['a'] = False
    assert w.plan([job(1)])[1][1] == 'a is offline'
    w.online['a'] = None
    assert 'first health check' in w.plan([job(1)])[1][1]


def test_real_free_memory_below_declared_blocks_with_explanation():
    w = World()
    w.actual = {'a': 10.0}
    start, why = w.plan([job(1, mem_gb=30)])
    assert start == [] and 'actually free' in why[1]
    w.actual = {'a': 29.0}  # within the 2 GB tolerance
    assert w.plan([job(1, mem_gb=30)])[0] == [1]


def test_actual_free_memory_is_reduced_by_jobs_started_in_the_same_pass():
    w = World()
    w.actual = {'a': 40.0}
    start, why = w.plan([job(1, mem_gb=30), job(2, mem_gb=20)])
    assert start == [1] and 'actually free' in why[2]


def test_multi_host_jobs_keep_global_order_without_deadlock():
    w = World()
    blocker = job(1, RUNNING, hosts=['b'], exclusive=True)
    both = job(2, hosts=['a', 'b'], mem_gb=10)
    only_a = job(3, hosts=['a'], mem_gb=55)  # 60 free on a: would leave #2 too little
    start, why = w.plan([blocker, both, only_a])
    assert start == [] and 'held exclusively by #1' in why[2]
    assert why[3] == 'in line behind #2 "j2" (o) on a'
    blocker.state = core.DONE
    assert w.plan([blocker, both, only_a])[0] == [2]
    small = job(4, hosts=['a'], mem_gb=5)  # holds nobody up: goes ahead of #2
    assert w.plan([job(1, RUNNING, hosts=['b'], exclusive=True), both, small])[0] == [4]


def test_optional_service_memory_counts_only_while_it_answers():
    w = World()
    w.services.append(Service('a-opt', 'a', 30, url='http://o/v1', kind='optional'))
    w.obs['a-opt'] = Observation('down', at=NOW)
    assert core.host_memory(w.hosts['a'], w.services, w.statuses([]), [])[2] == 60
    w.obs['a-opt'] = Observation('unverified', detail='HTTP 401', at=NOW)
    assert core.host_memory(w.hosts['a'], w.services, w.statuses([]), [])[2] == 30


def test_wrong_model_is_flagged_and_unusable():
    w = World()
    w.obs['a-llm'] = Observation('wrong', ['other'], detail='serving other; expected m', at=NOW)
    st = w.statuses([])['a-llm']
    assert st.code == 'wrong' and st.warn and not st.usable


def test_name_resolution():
    hosts, services = roster()
    assert core.resolve_names(['reason-a', 'embed-a', 'a-llm'], services, 'role') == ['a-llm', 'b-llm']
    assert core.resolve_hosts(['alpha', 'tat'], hosts) == ['a', 't1', 't2']
    with pytest.raises(ValueError, match='unknown role "nope"'):
        core.resolve_names(['nope'], services, 'role')
    with pytest.raises(ValueError, match='unknown host "zz"'):
        core.resolve_hosts(['zz'], hosts)


def test_busy_gpu_with_no_fleetq_job_blocks_exclusive_jobs_only():
    w = World()
    start, why = core.schedule([job(1, exclusive=True), job(2, hosts=['b'], exclusive=True)], w.hosts,
                               w.services, w.statuses([]), w.obs, w.online, {}, {'a': 0.9, 'b': 0.1})
    assert start == [2] and why[1] == 'needs a quiet, but its GPU is 90% busy with work fleetq does not track'
    start, _ = core.schedule([job(1)], w.hosts, w.services, w.statuses([]), w.obs, w.online, {}, {'a': 0.9})
    assert start == [1]


def test_resolve_url_places_work_by_service_port_then_default_pool():
    hosts = {'s1': Host('s1', 'gpu-a', 116, ['gpu-a'], addresses=['192.0.2.11', '100.64.0.11']),
             'gpu-c1': Host('gpu-c1', 'gpu-c', 12, addresses=['100.64.0.30']),
             'gpu-c0': Host('gpu-c0', 'gpu-c', 32, addresses=['100.64.0.30'], default=True)}
    services = [Service('s1-vllm', 's1', 70, url='http://gpu-a:8000/v1'),
                Service('chat-small', 'gpu-c1', 9, url='http://gpu-c:8010/v1')]
    assert core.resolve_url('http://100.64.0.11:8000/v1', hosts, services) == (['s1'], 's1-vllm')
    assert core.resolve_url('http://gpu-a.tailnet.example:9999/v1', hosts, services) == (['s1'], None)
    assert core.resolve_url('http://gpu-c:8010/v1', hosts, services) == (['gpu-c1'], 'chat-small')
    assert core.resolve_url('http://gpu-c:8095/v1', hosts, services) == (['gpu-c0'], None)
    assert core.resolve_url('http://api.example.com/v1', hosts, services) == ([], None)


def test_per_host_memory_is_checked_on_each_host():
    w = World()  # a: 60 free (a-llm holds 40), b: 100 free
    both = job(1, hosts=['a', 'b'], mem_map={'a': 50, 'b': 90})
    assert w.plan([both])[0] == [1]
    other = job(2, RUNNING, hosts=['b'], mem_gb=20)
    start, why = w.plan([other, both])
    assert start == [] and why[1] == 'needs 90 GB on b; 80 GB free of 100'
    both.state = RUNNING
    statuses = w.statuses([both])
    assert core.host_memory(w.hosts['a'], w.services, statuses, [both])[1] == 50
    assert core.host_memory(w.hosts['b'], w.services, statuses, [both])[1] == 90


def entries(w, jobs=(), job_obs=None, include_all=False):
    return core.model_entries(w.services, w.statuses(list(jobs)), w.obs, w.hosts, list(jobs),
                              job_obs or {}, NOW, include_all)


def test_models_lists_each_usable_url_and_model_with_declared_capabilities():
    w = World()
    w.services[0].capabilities = ['text', 'image']
    w.obs['a-llm'].served = [{'id': 'm', 'root': 'org/M', 'max_model_len': 32768}]
    got = {(e['url'], e['model']): e for e in entries(w)}
    a, b = got[('http://a/v1', 'm')], got[('http://b/v1', 'm')]
    assert (a['capabilities'], a['capabilities_source'], a['context'], a['root']) == (['text', 'image'], 'config', 32768, 'org/M')
    assert (b['capabilities'], b['capabilities_source'], b['context']) == ([], 'unknown', None)
    assert a['usable'] and a['verified'] and a['roles'] == ['chat-a', 'reason-a'] and a['age_s'] == 5


def test_per_model_capabilities_override_the_service_list():
    svc = Service('s', 'h', capabilities=['text'], model_capabilities={'emb': ['embed']})
    assert core.capabilities_for(svc, 'emb') == (['embed'], 'config')
    assert core.capabilities_for(svc, 'chat') == (['text'], 'config')
    with pytest.raises(ValueError, match='unknown capability vison'):
        core.check_capabilities(['text', 'vison'], 'service s')


def test_models_skips_held_and_wedged_services_unless_asked_for_all():
    w = World()
    held = job(1, RUNNING, takes_down=['a-llm'])
    w.obs['b-llm'] = Observation('up', ['m'], 1, at=NOW, progress=5.0, stuck_since=NOW - 600)
    assert entries(w, [held]) == []
    every = {e['service']: e for e in entries(w, [held], include_all=True)}
    assert (every['a-llm']['state'], every['a-llm']['usable']) == ('held', False)
    assert (every['b-llm']['state'], every['b-llm']['usable']) == ('wedged', False)


def test_two_ranks_behind_one_url_make_one_entry():
    w = World()
    w.hosts['t1'], w.hosts['t2'] = Host('t1', 'tat', 32), Host('t2', 'tat', 32)
    w.services += [Service('r0', 't1', url='http://tat:8080/v1', roles=['native']),
                   Service('r1', 't2', url='http://tat:8080/v1/', roles=['native'])]
    w.obs['r0'] = w.obs['r1'] = Observation('up', ['qwen'], 0, at=NOW)
    rad = [e for e in entries(w) if e['model'] == 'qwen']
    assert len(rad) == 1 and rad[0]['services'] == ['r0', 'r1'] and rad[0]['hosts'] == ['t1', 't2']


def test_down_or_unverified_service_falls_back_to_declared_models():
    w = World()
    w.obs['a-llm'] = Observation('unverified', detail='HTTP 401 on /models', at=NOW)
    e = next(e for e in entries(w) if e['service'] == 'a-llm')
    assert (e['model'], e['usable'], e['verified']) == ('m', True, False)


def test_job_served_models_appear_once_seen_and_only_while_running():
    w = World()
    j = job(7, RUNNING, serves=['http://a:8001/v1'], serves_caps=['text', 'video'])
    assert [e for e in entries(w, [j]) if e['job']] == []
    starting = [e for e in entries(w, [j], include_all=True) if e['job']]
    assert [(e['model'], e['state'], e['detail']) for e in starting] == [(None, 'starting', 'not checked yet')]
    seen = {(7, 'http://a:8001/v1'): Observation('up', ['new'], 0, at=NOW, served=[{'id': 'new', 'max_model_len': 8192}])}
    e = next(e for e in entries(w, [j], seen) if e['job'])
    assert (e['model'], e['capabilities'], e['capabilities_source'], e['context'], e['job']['id']) == \
        ('new', ['text', 'video'], 'job', 8192, 7)
    j.state = 'done'
    assert [e for e in entries(w, [j], seen, include_all=True) if e['job']] == []


# ---- loads, backfill, reservations ---------------------------------------------------------

def test_download_and_benchmark_coexist_but_a_gpu_job_waits_for_the_benchmark():
    w = World()
    download = job(1, RUNNING, load=['net', 'disk'], expect_s=7200, started_at=NOW)
    bench = job(2, quiet=['gpu', 'cpu'])
    start, _ = w.plan([download, bench])
    assert start == [2]
    bench.state, bench.started_at = RUNNING, NOW
    small = job(3, mem_gb=5)  # loads the GPU, the default
    start, why = w.plan([download, bench, small])
    assert start == [] and why[3] == '#2 "j2" (o) needs gpu quiet on a, and this job loads it'
    # The other way round: the benchmark waits for a GPU job that is already running.
    gpu = job(4, RUNNING, started_at=NOW)
    start, why = w.plan([gpu, job(5, quiet=['gpu'])])
    assert start == [] and why[5] == 'needs gpu quiet on a, but #4 "j4" (o) loads it'


def test_using_a_service_counts_as_gpu_load():
    w = World()
    harness = job(1, RUNNING, load=['net'], uses=['a-llm'])
    start, why = w.plan([harness, job(2, quiet=['gpu'])])
    assert start == [] and 'needs gpu quiet on a, but #1' in why[2]


def test_quiet_gpu_checks_untracked_load_like_exclusive():
    w = World()
    w.obs['a-llm'] = Observation('up', ['m'], 2, at=NOW - 5)
    net = job(1, RUNNING, load=['net'])
    start, why = w.plan([net, job(2, quiet=['gpu'])])
    assert start == [] and 'requests not tracked by fleetq are in flight' in why[2]
    w.obs['a-llm'] = Observation('up', ['m'], 0, at=NOW - 5)
    start, why = core.schedule([net, job(2, quiet=['gpu'])], w.hosts, w.services, w.statuses([net]), w.obs,
                               w.online, {}, {'a': 0.9}, now=NOW)
    assert start == [] and 'GPU is 90% busy' in why[2]
    # Without --quiet gpu, the busy GPU does not matter.
    start, _ = core.schedule([net, job(3, load=['cpu'])], w.hosts, w.services, w.statuses([net]), w.obs,
                             w.online, {}, {'a': 0.9}, now=NOW)
    assert start == [3]


def backfill_world():
    """#1 runs on a for another hour; #2 needs the GPU quiet and waits for it."""
    w = World()
    running = job(1, RUNNING, expect_s=3600, started_at=NOW)
    head = job(2, quiet=['gpu'])
    return w, running, head


def test_backfill_admits_a_short_run_job_with_a_deadline_and_refuses_one_without_expect():
    w, running, head = backfill_world()
    short = job(3, kind='run', expect_s=20 * 60)     # 20 min * 1.2 + 5 min = 29 min <= 60
    long = job(4, kind='run', expect_s=50 * 60)      # 65 min > 60
    blind = job(5, kind='run')                       # no --expect
    lease = job(6, expect_s=10 * 60)                 # leases never take the deadline route
    p = core.plan([running, head, short, long, blind, lease], w.hosts, w.services,
                  w.statuses([running]), w.obs, w.online, {}, now=NOW)
    assert p.to_start == [3]
    assert p.deadlines == {3: NOW + 3600}
    assert p.starts[2] == NOW + 3600 and p.line == [2, 4, 5, 6]
    assert p.reasons[4] == 'in line behind #2 "j2" (o) on a'
    assert p.reasons[5] == 'in line behind #2 "j2" (o) on a; with --expect it may go first if it ends before #2 starts'
    assert p.reasons[6] == 'in line behind #2 "j2" (o) on a'


def test_backfill_needs_a_known_start_and_lets_harmless_jobs_through():
    w, running, head = backfill_world()
    running.expect_s = None  # nobody knows when #1 ends
    short = job(3, kind='run', expect_s=60)
    quiet_one = job(4, load=['net'])  # does not load the GPU: cannot hold #2 up
    p = core.plan([running, head, short, quiet_one], w.hosts, w.services, w.statuses([running]), w.obs,
                  w.online, {}, now=NOW)
    assert p.starts[2] is None and p.to_start == [4] and p.deadlines == {}


def test_backfill_respects_memory_the_head_will_need():
    w = World()  # 60 GB free for jobs on a
    running = job(1, RUNNING, mem_gb=50, expect_s=3600, started_at=NOW)
    head = job(2, mem_gb=40)  # needs #1 gone
    fits = job(3, mem_gb=10)  # 10 + 40 <= 60 once #1 is gone: no harm
    hogs = job(4, mem_gb=10)  # now 20 + 40 > 60: would hold #2 up
    p = core.plan([running, head, fits, hogs], w.hosts, w.services, w.statuses([running]), w.obs,
                  w.online, {}, now=NOW)
    assert p.to_start == [3] and p.reasons[4].startswith('in line behind #2')


def test_reservation_waits_for_its_time_then_goes_first_in_line():
    w = World()
    early = job(1, mem_gb=50)
    booked = job(2, mem_gb=50, start_at=NOW + 600, expect_s=3600)
    p = core.plan([booked, early], w.hosts, w.services, w.statuses([]), w.obs, w.online, {}, now=NOW)
    # #1 expects the default 2 h, which would run into the reservation's start.
    assert p.to_start == [] and p.reasons[1] == 'would still be running when reservation #2 "j2" (o) starts'
    assert p.reasons[2] == 'reserved; waits for its start time' and p.starts[2] == NOW + 600
    early.expect_s = 5 * 60
    assert core.schedule([booked, early], w.hosts, w.services, w.statuses([]), w.obs, w.online, {},
                         now=NOW)[0] == [1]
    # Once due it is first in line, ahead of an older queued job.
    late = NOW + 601
    p = core.plan([job(3, mem_gb=50), booked], w.hosts, w.services, w.statuses([]), w.obs, w.online, {}, now=late)
    assert p.to_start == [2] and p.reasons[3] == 'needs 50 GB on a; 10 GB free of 100'


def test_run_job_before_a_reservation_gets_its_start_as_deadline():
    w = World()
    booked = job(1, exclusive=True, start_at=NOW + 3600, expect_s=3600)
    quick = job(2, kind='run', expect_s=30 * 60)
    p = core.plan([booked, quick], w.hosts, w.services, w.statuses([]), w.obs, w.online, {}, now=NOW)
    assert p.to_start == [2] and p.deadlines[2] == NOW + 3600


def test_reservation_windows_clash_only_when_they_overlap_and_collide():
    w = World()
    a = job(1, quiet=['gpu', 'cpu'], start_at=NOW + 3600, expect_s=3600)
    windows = core.booked_windows([a], NOW)
    st = w.statuses([])
    gpu = job(0, mem_gb=5)
    hit = core.window_clash(gpu, NOW + 5400, 3600, windows, w.hosts, w.services, st)
    assert hit and hit[0] == [a] and hit[1] == '#1 "j1" (o) needs gpu quiet on a, and this job loads it'
    assert core.window_clash(gpu, NOW + 7200, 3600, windows, w.hosts, w.services, st) is None
    assert core.window_clash(job(0, load=['net']), NOW + 5400, 3600, windows, w.hosts, w.services, st) is None
    assert core.window_clash(job(0, hosts=['b']), NOW + 5400, 3600, windows, w.hosts, w.services, st) is None
    free = core.free_windows(gpu, 3600, windows, w.hosts, w.services, st, NOW)
    assert free[0] == (NOW, NOW + 3600) and free[1] == (NOW + 7200, None)
    # Memory adds up across every reservation present at once.
    big = [job(2, mem_gb=30, start_at=NOW, expect_s=3600), job(3, mem_gb=20, start_at=NOW + 600, expect_s=3600)]
    hit = core.window_clash(job(0, mem_gb=15, load=[]), NOW + 1200, 600, core.booked_windows(big, NOW),
                            w.hosts, w.services, st)
    assert hit and 'needs 15 GB on a; 10 GB free' in hit[1]


def test_forecast_estimates_the_start_from_expected_ends():
    w = World()
    running = job(1, RUNNING, exclusive=True, expect_s=3600, started_at=NOW - 600)
    ahead = job(2, exclusive=True, expect_s=1800)
    me = job(3, mem_gb=10)

    def statuses_for(run, t):
        return core.all_statuses(w.services, w.obs, run, {}, w.online, t, 600)
    start, deadline = core.forecast([running, ahead, me], 3, w.hosts, w.services, statuses_for, w.obs,
                                    w.online, {}, {}, NOW)
    assert start == NOW + 3000 + 1800 and deadline is None
    # A down service it needs has no expected end: no estimate.
    w.obs['a-llm'] = Observation('down', at=NOW)
    needs = job(4, uses=['a-llm'])
    assert core.forecast([needs], 4, w.hosts, w.services, statuses_for, w.obs, w.online, {}, {}, NOW) == (None, None)
