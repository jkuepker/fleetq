"""Store atomicity, the jobs_ended index and event retention."""
import sqlite3

import pytest

from fleetq.core import Observation
from fleetq.server import Fleet
from fleetq.store import Store
from test_server import CONFIG, Clock

DAY = 86400


@pytest.fixture
def fleet(tmp_path):
    cfg = tmp_path / 'fleet.toml'
    cfg.write_text(CONFIG.format(db=tmp_path / 'q.db') + '\n')
    clock = Clock()
    f = Fleet(cfg, clock=clock)
    f.apply_probe({'a-llm': Observation('up', ['m'], 0, at=clock.t)}, {'ma': True, 'mb': True}, {}, clock.t)
    return f


def takedown(fleet):
    return fleet.submit({'label': 'bench', 'owner': 'me', 'hosts': ['a'], 'takes_down': ['a-llm']})


def test_failed_release_leaves_no_partial_state(fleet, monkeypatch):
    job = takedown(fleet)
    assert job['state'] == 'running'
    events_before = fleet.store.events(100)

    def boom(restore):
        raise RuntimeError('disk gone')
    monkeypatch.setattr(fleet.store, 'add_restore', boom)
    with pytest.raises(RuntimeError):
        fleet.release(job['id'], {'owner': 'me'})

    assert not fleet.store.db.in_transaction
    assert fleet.store.job(job['id']).state == 'running'
    assert fleet.store.job(job['id']).ended_at is None
    assert fleet.store.restores() == {}
    assert fleet.store.events(100) == events_before

    monkeypatch.undo()
    fleet.release(job['id'], {'owner': 'me'})
    assert fleet.store.job(job['id']).state == 'done'
    assert 'a-llm' in fleet.store.restores()


def test_nested_transaction_rolls_back_as_one(tmp_path):
    store = Store(tmp_path / 's.db')
    with pytest.raises(ValueError):
        with store.transaction():
            store.event(1.0, 'x', 'outer')
            with store.transaction():
                store.event(2.0, 'x', 'inner')
            assert store.db.in_transaction
            raise ValueError
    assert store.events() == []
    with store.transaction():
        with store.transaction():
            store.event(3.0, 'x', 'kept')
    assert [e['text'] for e in store.events()] == ['kept']
    assert not store.db.in_transaction


def test_old_database_without_the_ended_index_still_opens(tmp_path):
    path = tmp_path / 'old.db'
    old = sqlite3.connect(path)
    old.executescript("""CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, state TEXT NOT NULL,
        kind TEXT NOT NULL, owner TEXT NOT NULL, label TEXT NOT NULL, hosts TEXT NOT NULL,
        mem_gb REAL NOT NULL DEFAULT 0, exclusive INTEGER NOT NULL DEFAULT 0, uses TEXT NOT NULL DEFAULT '[]',
        takes_down TEXT NOT NULL DEFAULT '[]', created_at REAL NOT NULL, started_at REAL, ended_at REAL,
        heartbeat_at REAL, expect_s REAL, command TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '',
        exit_code INTEGER, note TEXT NOT NULL DEFAULT '', mem_map TEXT NOT NULL DEFAULT '{}');
        CREATE INDEX jobs_state ON jobs(state);
        INSERT INTO jobs (state, kind, owner, label, hosts, created_at, ended_at)
        VALUES ('done','lease','o','old','["a"]',1,2);""")
    old.commit()
    old.close()
    store = Store(path)
    assert [j.label for j in store.recent_jobs()] == ['old']
    names = {r['name'] for r in store.db.execute("PRAGMA index_list('jobs')")}
    assert 'jobs_ended' in names


def test_events_older_than_the_keep_window_are_pruned_once_a_day(fleet):
    now = fleet.clock.t
    fleet.store.event(now - 200 * DAY, 'old', 'ancient')
    fleet.store.event(now - 10 * DAY, 'old', 'recent')
    fleet.prune_events(now)
    texts = {e['text'] for e in fleet.store.events(100)}
    assert 'ancient' not in texts and 'recent' in texts

    fleet.store.event(now - 200 * DAY, 'old', 'ancient again')
    fleet.prune_events(now + 3600)  # same day: skipped
    assert 'ancient again' in {e['text'] for e in fleet.store.events(100)}
    fleet.prune_events(now + DAY + 1)
    assert 'ancient again' not in {e['text'] for e in fleet.store.events(100)}


def test_zero_keep_days_keeps_every_event(fleet):
    fleet.cfg['events_keep_days'] = 0
    fleet.store.event(fleet.clock.t - 900 * DAY, 'old', 'ancient')
    fleet.prune_events(fleet.clock.t)
    assert 'ancient' in {e['text'] for e in fleet.store.events(100)}


def test_old_database_gains_the_load_and_reservation_columns(tmp_path):
    path = tmp_path / 'old.db'
    old = sqlite3.connect(path)
    old.executescript("""CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, state TEXT NOT NULL,
        kind TEXT NOT NULL, owner TEXT NOT NULL, label TEXT NOT NULL, hosts TEXT NOT NULL,
        mem_gb REAL NOT NULL DEFAULT 0, exclusive INTEGER NOT NULL DEFAULT 0, uses TEXT NOT NULL DEFAULT '[]',
        takes_down TEXT NOT NULL DEFAULT '[]', created_at REAL NOT NULL, started_at REAL, ended_at REAL,
        heartbeat_at REAL, expect_s REAL, command TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '',
        exit_code INTEGER, note TEXT NOT NULL DEFAULT '', mem_map TEXT NOT NULL DEFAULT '{}',
        serves TEXT NOT NULL DEFAULT '[]', serves_caps TEXT NOT NULL DEFAULT '[]',
        watch INTEGER NOT NULL DEFAULT 0, seen_at REAL);
        INSERT INTO jobs (state, kind, owner, label, hosts, created_at, started_at)
        VALUES ('running','lease','o','old','["a"]',1,2);""")
    old.commit()
    old.close()
    store = Store(path)
    cols = {r['name'] for r in store.db.execute('PRAGMA table_info(jobs)')}
    assert {'load', 'quiet', 'start_at', 'deadline'} <= cols
    job = store.active_jobs()[0]
    # A job from before --load counts as loading the GPU, so a new --quiet gpu job still waits for it.
    assert job.load == ['gpu'] and job.quiet == [] and job.start_at is None and job.deadline is None
    store.update_job(job.id, load=['net'])
    assert store.job(job.id).load == ['net']
