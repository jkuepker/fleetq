"""Learned --expect: the estimator, how the scheduler treats it, and the server and CLI end to end."""
import sqlite3

import pytest

from fleetq import cli, core, learn, store
from fleetq.core import RUNNING
from test_core import NOW, World, job
from test_server import env  # noqa: F401  (env is a fixture)

MIN = 60


def row(label='bench-1', owner='me', hosts=('a',), took=600.0, state='done', end=10_000.0):
    return {'label': label, 'owner': owner, 'hosts': list(hosts), 'state': state,
            'started_at': end - took, 'ended_at': end}


def est(rows, label='bench-9', owner='me', hosts=('a',), default_s=7200, **kw):
    return learn.estimate(learn.index(rows).get(learn.label_prefix(label)), owner, hosts, default_s, **kw)


# ---- the estimator ------------------------------------------------------------------------

def test_no_history_or_too_little_stays_the_default():
    assert est([]) == learn.Estimate(None, 'default', 0, '')
    two = [row(took=600), row(took=700)]
    assert est(two).source == 'default'          # min_samples is 3
    assert est(two, min_samples=2).source == 'learned'


def test_quantile_times_factor_floored_and_capped():
    rows = [row(took=t * MIN) for t in (10, 10, 10, 10, 50)]    # p80 = 18 min with linear interpolation
    e = est(rows)
    assert e.source == 'learned' and e.n == 5
    assert e.seconds == pytest.approx(learn.percentile([t * MIN for t in (10, 10, 10, 10, 50)], 0.8) * 1.25)
    assert est(rows, quantile=1.0, factor=1.0).seconds == 50 * MIN
    short = [row(took=20)] * 3
    assert est(short).seconds == learn.FLOOR_S                  # floored at 2 min
    long = [row(took=3 * 3600)] * 3
    capped = est(long)
    assert (capped.seconds, capped.source, capped.n, capped.capped) == (7200, 'learned', 3, True)
    assert not est(rows).capped
    assert est(long, default_s=8 * 3600).seconds == pytest.approx(3 * 3600 * 1.25)


def test_prefers_same_owner_and_hosts_when_there_are_enough_runs():
    mine = [row(owner='me', hosts=('a',), took=10 * MIN)] * 3
    other_owner = [row(owner='you', hosts=('a',), took=40 * MIN)] * 3
    other_host = [row(owner='me', hosts=('b',), took=20 * MIN)] * 3
    everyone = [row(owner='you', hosts=('b',), took=60 * MIN)] * 3
    rows = mine + other_owner + other_host + everyone
    e = est(rows)
    assert (e.scope, e.n, e.seconds) == ('owner+hosts', 3, 10 * MIN * 1.25)
    e = est(other_owner + other_host + everyone)
    assert (e.scope, e.n, e.seconds) == ('owner', 3, 20 * MIN * 1.25)      # me, on b
    e = est(other_owner + everyone, owner='me')
    assert (e.scope, e.n, e.seconds) == ('hosts', 3, 40 * MIN * 1.25)      # on a, by you
    e = est(everyone, owner='me', hosts=('a',))
    assert (e.scope, e.n) == ('all', 3)
    assert est(rows, hosts=('a', 'b')).scope == 'owner'                    # hosts match as a set


def test_only_finished_jobs_with_the_same_kind_of_label_count():
    rows = [row('bench-1'), row('bench_2'), row('Bench 3: x'),             # all prefix "bench"
            row('train-1'), row('bench-4', state='cancelled'),
            {**row('bench-5'), 'started_at': None}]
    assert est(rows, min_samples=4).source == 'default'
    assert est(rows, min_samples=3).n == 3


# ---- how the scheduler uses a learned expect ---------------------------------------------

def learned(id, mins, n=9, **kw):
    return job(id, expect_learned_s=mins * MIN, expect_source='learned', expect_n=n, **kw)


def plan(w, jobs):
    return core.plan(jobs, w.hosts, w.services, w.statuses([j for j in jobs if j.state == RUNNING]),
                     w.obs, w.online, {}, now=NOW)


def test_source_defaults_follow_whether_an_expect_was_given():
    assert job(1, expect_s=60).expect_source == 'given'
    assert job(2).expect_source == 'default'
    assert job(3, expect_learned_s=60, expect_source='learned').expect_source == 'learned'


def test_learned_run_job_never_gets_a_backfill_deadline():
    w = World()
    running = job(1, RUNNING, expect_s=3600, started_at=NOW)
    head = job(2, quiet=['gpu'])
    given = job(3, kind='run', expect_s=10 * MIN)
    p = plan(w, [running, head, given])
    assert p.to_start == [3] and p.deadlines == {3: NOW + 3600}          # the same job, said by its owner
    guessed = learned(3, 10, kind='run')
    p = plan(w, [running, head, guessed])
    assert p.to_start == [] and p.deadlines == {}
    assert p.reasons[3].startswith('in line behind #2')
    assert 'with --expect it may go first' in p.reasons[3]               # the way out is still named


def test_learned_run_job_started_by_other_means_keeps_no_deadline():
    w = World()
    harmless = learned(2, 10, kind='run', load=['net'])                  # holds nobody up: starts at once
    p = plan(w, [job(1, RUNNING, expect_s=3600, started_at=NOW), job(3, quiet=['gpu']), harmless])
    assert p.to_start == [2] and p.deadlines == {}


def test_learned_expect_ends_a_blocker_for_earliest_start():
    w = World()
    head = job(2, quiet=['gpu'])
    for fields in ({'expect_learned_s': 1800, 'expect_source': 'learned'}, {'expect_s': 1800}):
        running = job(1, RUNNING, kind='run', started_at=NOW, **fields)
        assert plan(w, [running, head]).starts[2] == NOW + 1800
    unknown = job(1, RUNNING, kind='run', started_at=NOW)                # default for a run: no estimate
    assert plan(w, [unknown, head]).starts[2] is None


def test_overdue_comes_at_twice_a_learned_expect_and_at_the_expect_otherwise():
    assert learned(1, 10).overdue_after() == 20 * MIN
    assert job(2, expect_s=10 * MIN).overdue_after() == 10 * MIN
    assert job(3, expect_s=7200).overdue_after() == 7200                  # the default
    assert job(4, kind='run').overdue_after() is None


def test_learned_lease_goes_ahead_of_a_reservation_only_if_one_and_a_half_times_it_fits():
    w = World()
    booked = job(1, exclusive=True, start_at=NOW + 3600, expect_s=3600)
    fits = learned(2, 30)                                                # 30 x 1.5 x 1.2 + 5 min = 59 min <= 60
    p = plan(w, [booked, fits])
    assert p.to_start == [2] and p.deadlines == {}
    tight = learned(2, 31)                                               # 60.8 min > 60
    p = plan(w, [booked, tight])
    assert p.to_start == []
    assert p.reasons[2] == 'would still be running when reservation #1 "j1" (o) starts'
    given = job(2, expect_s=41 * MIN)                                    # what was said is taken at its word
    assert plan(w, [booked, given]).to_start == [2]


def test_learned_run_job_before_a_reservation_is_held_to_the_same_margin_and_gets_no_deadline():
    w = World()
    booked = job(1, exclusive=True, start_at=NOW + 3600, expect_s=3600)
    ok = plan(w, [booked, learned(2, 30, kind='run')])
    assert ok.to_start == [2] and ok.deadlines == {}                     # a given 39 min would be stopped at 3600
    assert plan(w, [booked, learned(2, 31, kind='run')]).to_start == []
    assert plan(w, [booked, job(2, kind='run', expect_s=39 * MIN)]).deadlines == {2: NOW + 3600}


# ---- server and CLI ----------------------------------------------------------------------

def history(fleet, clock, label, n, took_s=600, owner='me', hosts=('a',), kind='run'):
    """n finished jobs of one kind, each took took_s."""
    for i in range(n):
        j = fleet.submit({'label': f'{label}-{i}', 'owner': owner, 'hosts': list(hosts), 'kind': kind})
        assert j['state'] == 'running'
        clock.t += took_s
        fleet.release(j['id'], {'owner': owner})


def new_run(fleet, clock, label='train-90', **kw):
    clock.t += 301                                                       # past the five-minute history cache
    return fleet.submit({'label': label, 'owner': 'me', 'hosts': ['a'], 'kind': 'run', **kw})


def test_a_job_without_expect_gets_a_learned_one_and_a_given_one_is_left_alone(env):
    fleet, clock, url = env
    history(fleet, clock, 'train', 4)
    j = new_run(fleet, clock)
    assert (j['expect_source'], j['expect_n'], j['expect_learned_s']) == ('learned', 4, 750.0)   # 600 s * 1.25
    assert j['expect_s'] is None                                         # never in expect_s: see the rollback test
    assert j['state'] == 'running' and j['deadline'] is None
    j = new_run(fleet, clock, label='train-91', expect_min=30)
    assert (j['expect_source'], j['expect_s']) == ('given', 1800.0)
    lease = fleet.submit({'label': 'other-1', 'owner': 'me', 'hosts': ['b']})
    assert (lease['expect_source'], lease['expect_s']) == ('default', 7200.0)    # nothing like it on record
    run = fleet.submit({'label': 'other-2', 'owner': 'me', 'hosts': ['b'], 'kind': 'run'})
    assert (run['expect_source'], run['expect_s']) == ('default', None)          # runs keep "no estimate"


def test_learning_reads_history_once_per_kind_and_five_minutes(env, monkeypatch):
    fleet, clock, url = env
    history(fleet, clock, 'train', 3)
    reads = []
    original = fleet.store.finished_rows
    monkeypatch.setattr(fleet.store, 'finished_rows', lambda *a: reads.append(a) or original(*a))
    new_run(fleet, clock, label='a-1', hosts=['b'])
    clock.t += 299
    fleet.submit({'label': 'a-2', 'owner': 'me', 'hosts': ['b'], 'kind': 'run'})   # inside five minutes: cached
    assert len(reads) == 1
    clock.t += 2
    fleet.submit({'label': 'a-3', 'owner': 'me', 'hosts': ['b'], 'kind': 'run'})
    assert len(reads) == 2
    fleet.submit({'label': 'a-4', 'owner': 'me', 'hosts': ['b']})        # leases are another kind
    assert len(reads) == 3


def test_learn_days_looks_back_that_far_and_zero_turns_it_off(env):
    fleet, clock, url = env
    history(fleet, clock, 'train', 3)
    clock.t += 31 * 86400
    assert new_run(fleet, clock)['expect_source'] == 'default'          # older than learn_days
    clock.t -= 20 * 86400
    fleet.cfg['learn_days'] = 0
    assert new_run(fleet, clock, label='train-92')['expect_source'] == 'default'


def test_a_learned_job_is_flagged_overdue_at_twice_its_estimate(env):
    fleet, clock, url = env
    history(fleet, clock, 'train', 4)
    j = new_run(fleet, clock)                                            # ~12m30s
    clock.t += 1400
    assert not [f for f in fleet.get(j['id'])['flags'] if f.startswith('overdue')]
    fleet.tick()
    assert not [e for e in fleet.store.events(50) if e['kind'] == 'overdue']
    clock.t += 200                                                       # 1600 s > 2 * 750
    flags = [f for f in fleet.get(j['id'])['flags'] if f.startswith('overdue')]
    assert flags == ['overdue: expected 12m (learned from 4 runs), running 26m']
    fleet.tick()
    assert [e for e in fleet.store.events(50) if e['kind'] == 'overdue']


def test_a_learned_run_job_waits_its_turn_where_a_given_one_goes_ahead(env):
    fleet, clock, url = env
    history(fleet, clock, 'train', 5)
    clock.t += 301
    fleet.submit({'label': 'hold', 'owner': 'me', 'hosts': ['a'], 'expect_min': 60})
    fleet.submit({'label': 'bench', 'owner': 'me', 'hosts': ['a'], 'quiet': ['gpu']})
    guessed = fleet.submit({'label': 'train-93', 'owner': 'me', 'hosts': ['a'], 'kind': 'run'})
    assert guessed['state'] == 'queued' and guessed['expect_source'] == 'learned'
    assert 'with --expect it may go first' in guessed['reason']
    said = fleet.submit({'label': 'train-94', 'owner': 'me', 'hosts': ['a'], 'kind': 'run', 'expect_min': 12})
    assert said['state'] == 'running' and said['deadline'] is not None


def test_expect_auto_turns_five_or_more_runs_into_a_given_estimate(env, capsys):
    fleet, clock, url = env
    history(fleet, clock, 'train', 5)
    clock.t += 301
    fleet.submit({'label': 'hold', 'owner': 'me', 'hosts': ['a'], 'expect_min': 60})
    fleet.submit({'label': 'bench', 'owner': 'me', 'hosts': ['a'], 'quiet': ['gpu']})
    j = fleet.submit({'label': 'train-93', 'owner': 'me', 'hosts': ['a'], 'kind': 'run', 'expect_auto': True})
    assert (j['expect_source'], j['expect_s'], j['expect_n']) == ('given', 750.0, 5)
    assert j['expect_learned_s'] is None and 'auto_refused' not in j
    assert j['state'] == 'running' and j['deadline'] is not None         # now it may backfill, and be stopped


def test_expect_auto_never_turns_a_capped_estimate_into_a_given_one(env, capsys):
    fleet, clock, url = env
    history(fleet, clock, 'slow', 5, took_s=3 * 3600)                    # 3 h * 1.25 is over the 2 h default
    clock.t += 301
    fleet.submit({'label': 'hold', 'owner': 'me', 'hosts': ['a'], 'expect_min': 60})
    fleet.submit({'label': 'bench', 'owner': 'me', 'hosts': ['a'], 'quiet': ['gpu']})
    j = fleet.submit({'label': 'slow-93', 'owner': 'me', 'hosts': ['a'], 'kind': 'run', 'expect_auto': True})
    assert (j['expect_source'], j['expect_learned_s'], j['expect_n']) == ('learned', 7200.0, 5)
    assert 'reaches the 120-minute default' in j['auto_refused']
    assert j['state'] == 'queued' and j['deadline'] is None             # a "3 h+" history is not a 2 h promise
    assert cli.main(['acquire', '--host', 'b', '--label', 'slow-94', '--expect', 'auto']) == 0
    assert 'learned from 0 similar' not in capsys.readouterr().err


def test_expect_auto_with_too_few_runs_falls_back_to_learned_or_default(env):
    fleet, clock, url = env
    history(fleet, clock, 'train', 4)
    j = new_run(fleet, clock, expect_auto=True)
    assert (j['expect_source'], j['expect_n']) == ('learned', 4)
    j = new_run(fleet, clock, label='nothing-like-it', expect_auto=True)
    assert j['expect_source'] == 'default'


def test_cli_says_what_it_learned_and_how_auto_went(env, capsys):
    fleet, clock, url = env
    history(fleet, clock, 'train', 3, kind='lease')
    clock.t += 301
    assert cli.main(['acquire', '--host', 'a', '--label', 'train-95']) == 0
    err = capsys.readouterr().err
    assert 'expect ~12m, learned from 3 similar runs; pass --expect to set it' in err
    assert cli.main(['acquire', '--host', 'b', '--label', 'train-96', '--expect', 'auto']) == 0
    err = capsys.readouterr().err
    assert '--expect auto refused: needs 5 finished runs of yours whose label starts with "train" (have 0)' in err
    assert 'learned from 3 similar runs' in err                          # the plain estimate still applies
    assert cli.main(['acquire', '--host', 'b', '--label', 'nothing-like-it', '--expect', 'auto']) == 0
    assert '--expect auto refused: needs 5 finished runs of yours' in capsys.readouterr().err
    assert cli.main(['acquire', '--host', 'b', '--label', 'fixed', '--expect', '45']) == 0
    assert 'expect' not in capsys.readouterr().err
    with pytest.raises(SystemExit):
        cli.main(['acquire', '--host', 'b', '--label', 'x', '--expect', 'soon'])


def test_cli_shows_the_estimate_in_show_and_when(env, capsys):
    fleet, clock, url = env
    history(fleet, clock, 'train', 3)
    j = new_run(fleet, clock)
    assert cli.main(['show', str(j['id'])]) == 0
    assert 'expect ~12m (learned from 3 runs)' in capsys.readouterr().out
    assert cli.main(['when', '--host', 'a', '--label', 'train-97', '--expect', 'auto']) == 0
    assert 'assuming it runs ~12m, learned from 3 similar runs' in capsys.readouterr().out


# ---- old databases and old clients -------------------------------------------------------

def test_old_database_gains_the_columns_and_old_rows_keep_their_meaning(tmp_path):
    path = tmp_path / 'old.db'
    old_schema = store.SCHEMA.replace(
        ",\n  expect_source TEXT NOT NULL DEFAULT '', expect_n INTEGER NOT NULL DEFAULT 0, expect_learned_s REAL", '')
    assert old_schema != store.SCHEMA
    db = sqlite3.connect(path)
    db.executescript(old_schema)
    for expect in (1800.0, None):
        db.execute("INSERT INTO jobs (state, kind, owner, label, hosts, created_at, expect_s) "
                   "VALUES ('queued', 'run', 'me', 'x', '[\"a\"]', 1.0, ?)", (expect,))
    db.commit()
    db.close()
    s = store.Store(str(path))
    given, default = s.job(1), s.job(2)
    assert (given.expect_source, given.expect_s) == ('given', 1800.0)
    assert (default.expect_source, default.expect_s) == ('default', None)
    s.update_job(1, expect_source='learned', expect_n=7, expect_learned_s=90.0)
    assert (s.job(1).expect_source, s.job(1).expect_n, s.job(1).expect_learned_s) == ('learned', 7, 90.0)
    assert s.finished_rows('run', 0) == []


def test_old_clients_send_nothing_new_and_ignore_the_extra_fields(env):
    from test_server import api
    fleet, clock, url = env
    code, body = api(url, 'POST', '/v1/jobs', {'label': 'old-client', 'owner': 'me', 'hosts': ['a'],
                                               'kind': 'run', 'expect_min': 20})
    assert code in (200, 201) and body['expect_s'] == 1200.0 and body['expect_source'] == 'given'


# ---- review fixes ---------------------------------------------------------------------------

def old_scheduler_view(db_row):
    """A Job as a server from before this feature builds it from a row: the columns it knows."""
    known = {k: v for k, v in db_row.items() if k in OLD_FIELDS}
    return core.Job(**known)


OLD_FIELDS = {f for f in core.Job.__dataclass_fields__} - {'expect_source', 'expect_n', 'expect_learned_s'}


def test_a_learned_estimate_never_reaches_expect_s_so_a_rollback_stops_nothing_on_a_guess(env):
    fleet, clock, url = env
    history(fleet, clock, 'bench', 5)
    fleet.submit({'label': 'hold', 'owner': 'x', 'hosts': ['a'], 'expect_min': 60})
    fleet.submit({'label': 'big', 'owner': 'y', 'hosts': ['a'], 'exclusive': True, 'expect_min': 30})
    clock.t += 301
    j = fleet.submit({'label': 'bench-9', 'owner': 'me', 'hosts': ['a'], 'kind': 'run'})
    assert j['expect_source'] == 'learned' and j['expect_learned_s'] == 750.0
    stored = dict(fleet.store.db.execute('SELECT * FROM jobs WHERE id = ?', (j['id'],)).fetchone())
    assert stored['expect_s'] is None and stored['expect_learned_s'] == 750.0
    # The old scheduler reads expect_s alone: a run job with none is no backfill candidate and is never overdue.
    old = old_scheduler_view({**stored, 'hosts': ['a'], 'uses': [], 'takes_down': [], 'mem_map': {}, 'serves': [],
                              'serves_caps': [], 'load': ['gpu'], 'quiet': [], 'exclusive': False, 'watch': False})
    assert old.expect_s is None and old.overdue_after() is None
    lease = fleet.submit({'label': 'bench-lease', 'owner': 'me', 'hosts': ['b']})
    assert lease['expect_s'] == 7200.0                                  # a lease keeps the default, as before


def test_a_learned_job_floored_at_two_minutes_does_not_start_minutes_before_a_reservation():
    w = World()
    tiny = learned(2, 2)                                                 # 2 min learned: 2 x 1.5 x 1.2 + 5 = 8.6 min
    for lead, starts in ((400, []), (520, [2])):
        booked = job(1, exclusive=True, start_at=NOW + lead, expect_s=3600)
        assert plan(w, [booked, tiny]).to_start == starts
    assert tiny.protect_s(7200) == pytest.approx(2 * MIN * 1.5 * 1.2 + 300)
    assert learned(3, 100).protect_s(7200) == 7200                       # never asked more than the default


def test_a_booking_cannot_land_right_behind_a_waiting_learned_job(env):
    from fleetq.server import ApiError
    fleet, clock, url = env
    history(fleet, clock, 'bench', 5, took_s=60)
    clock.t += 400
    fleet.submit({'label': 'hold', 'owner': 'x', 'hosts': ['a'], 'exclusive': True, 'expect_min': 30})
    w = fleet.submit({'label': 'bench-8', 'owner': 'me', 'hosts': ['a'], 'exclusive': True, 'kind': 'run'})
    assert w['state'] == 'queued' and w['expect_source'] == 'learned'
    with pytest.raises(ApiError) as e:                                   # starts after hold (+30 m), runs up to the default
        fleet.submit({'label': 'booked', 'owner': 'y', 'hosts': ['a'], 'exclusive': True,
                      'start_in_s': 34 * 60, 'expect_min': 60})
    assert e.value.code == 409 and f'#{w["id"]}' in str(e.value)
    assert core.Job(1, 'queued', 'run', 'o', 'x', ['a'], expect_learned_s=60, expect_source='learned'
                    ).window_s(7200) == 7200


def samples(n, took, owner='me', hosts=('a',)):
    return [(took, owner, frozenset(hosts))] * n


def test_auto_only_from_the_callers_own_history_with_a_real_prefix_and_steady_durations():
    def auto(rows, prefix='bench', owner='me', hosts=('a',)):
        return learn.auto_estimate(rows, prefix, owner, hosts, 7200)
    est, why = auto(samples(5, 600))
    assert why is None and est.seconds == 750 and est.scope == 'owner+hosts'
    assert auto(samples(5, 600, hosts=('b',)))[0].scope == 'owner'       # same owner elsewhere is fine
    est, why = auto(samples(9, 600, owner='you'))                        # others' runs are not evidence
    assert est is None and 'needs 5 finished runs of yours' in why and '(have 0)' in why
    est, why = auto(samples(4, 600))
    assert est is None and '(have 4)' in why
    for prefix in ('', 'm', 'p', 'ab'):
        est, why = auto(samples(9, 600), prefix=prefix)
        assert est is None and 'too short' in why
    wobbly = samples(8, 60) + samples(2, 3000)                           # p95 / median = 50
    est, why = auto(wobbly)
    assert est is None and 'vary too much' in why
    est, why = auto(samples(9, 3 * 3600))
    assert est is None and 'default' in why


def test_the_server_refuses_auto_with_a_reason_and_still_learns(env):
    fleet, clock, url = env
    history(fleet, clock, 'train', 5, owner='you')
    j = new_run(fleet, clock)
    assert j['expect_source'] == 'learned'                              # others' history informs the plain estimate
    assert 'needs 5 finished runs of yours' in new_run(fleet, clock, expect_auto=True)['auto_refused']
    history(fleet, clock, 'x', 5)
    j = new_run(fleet, clock, label='x-9', expect_auto=True)
    assert j['expect_source'] == 'learned' and 'too short' in j['auto_refused']      # prefix "x"
    j = new_run(fleet, clock, label='#7 retry', expect_auto=True)
    assert 'too short' in j['auto_refused']
    from test_server import api
    code, body = api(url, 'POST', '/v1/when', {'label': 'x-1', 'hosts': ['a'], 'expect_auto': True})
    assert code == 200 and 'too short' in body['auto_refused']


def test_failed_runs_are_not_evidence(env):
    fleet, clock, url = env
    for i in range(4):
        j = fleet.submit({'label': f'flaky-{i}', 'owner': 'me', 'hosts': ['a'], 'kind': 'run'})
        clock.t += 5
        fleet.release(j['id'], {'owner': 'me', 'exit_code': 1})           # crashed early
    history(fleet, clock, 'flaky', 2)                                    # two good ones: below the minimum of 3
    assert new_run(fleet, clock, label='flaky-9')['expect_source'] == 'default'
    history(fleet, clock, 'flaky', 1)
    assert new_run(fleet, clock, label='flaky-9')['expect_n'] == 3


def test_the_history_query_uses_the_kind_state_ended_index(env):
    fleet, clock, url = env
    plan_rows = fleet.store.db.execute('EXPLAIN QUERY PLAN ' + store.FINISHED_SQL, ('run', 0)).fetchall()
    detail = ' '.join(r['detail'] for r in plan_rows)
    assert 'jobs_kind_state_ended' in detail and 'SEARCH' in detail, detail
