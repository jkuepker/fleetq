"""SQLite persistence for jobs, pending restores and the event log."""
from __future__ import annotations

from contextlib import contextmanager
import json
import sqlite3
import sys
from dataclasses import fields

from .core import Job, Restore

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  state TEXT NOT NULL, kind TEXT NOT NULL, owner TEXT NOT NULL, label TEXT NOT NULL,
  hosts TEXT NOT NULL, mem_gb REAL NOT NULL DEFAULT 0, exclusive INTEGER NOT NULL DEFAULT 0,
  uses TEXT NOT NULL DEFAULT '[]', takes_down TEXT NOT NULL DEFAULT '[]',
  created_at REAL NOT NULL, started_at REAL, ended_at REAL, heartbeat_at REAL, expect_s REAL,
  command TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '', exit_code INTEGER,
  note TEXT NOT NULL DEFAULT '', mem_map TEXT NOT NULL DEFAULT '{}',
  serves TEXT NOT NULL DEFAULT '[]', serves_caps TEXT NOT NULL DEFAULT '[]',
  watch INTEGER NOT NULL DEFAULT 0, seen_at REAL,
  load TEXT NOT NULL DEFAULT '["gpu"]', quiet TEXT NOT NULL DEFAULT '[]', start_at REAL, deadline REAL, notify TEXT NOT NULL DEFAULT '');
CREATE INDEX IF NOT EXISTS jobs_state ON jobs(state);
CREATE INDEX IF NOT EXISTS jobs_created ON jobs(created_at);
CREATE INDEX IF NOT EXISTS jobs_ended ON jobs(ended_at);
CREATE TABLE IF NOT EXISTS restores (
  service TEXT PRIMARY KEY, job_id INTEGER NOT NULL, owner TEXT NOT NULL, label TEXT NOT NULL,
  since REAL NOT NULL);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT, at REAL NOT NULL, job_id INTEGER, service TEXT,
  kind TEXT NOT NULL, text TEXT NOT NULL);
"""

JOB_FIELDS = {f.name for f in fields(Job)}
STATS_COLUMNS = ('id', 'state', 'kind', 'owner', 'label', 'hosts', 'created_at', 'started_at', 'ended_at',
                 'expect_s', 'reason', 'start_at', 'deadline')
JSON_FIELDS = ('hosts', 'uses', 'takes_down', 'mem_map', 'serves', 'serves_caps', 'load', 'quiet')
ADDED_COLUMNS = {'mem_map': "TEXT NOT NULL DEFAULT '{}'", 'serves': "TEXT NOT NULL DEFAULT '[]'",
                 'serves_caps': "TEXT NOT NULL DEFAULT '[]'", 'watch': 'INTEGER NOT NULL DEFAULT 0',
                 'seen_at': 'REAL',
                 # Jobs from before --load loaded the GPU, as every job does unless it says otherwise.
                 'load': """TEXT NOT NULL DEFAULT '["gpu"]'""", 'quiet': "TEXT NOT NULL DEFAULT '[]'",
                 'start_at': 'REAL', 'deadline': 'REAL', 'notify': "TEXT NOT NULL DEFAULT ''"}


class Store:
    def __init__(self, path):
        # One connection guarded by the server's lock; check_same_thread off for the HTTP threads.
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self._depth = 0
        self._pending = []   # events written in the open transaction, not yet committed
        self.on_commit = None  # called with a list of event dicts once they are durable
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript(SCHEMA)
        # Databases created before a column existed gain it in place.
        have = {r['name'] for r in self.db.execute('PRAGMA table_info(jobs)')}
        for col, ddl in ADDED_COLUMNS.items():
            if col not in have:
                self.db.execute(f'ALTER TABLE jobs ADD COLUMN {col} {ddl}')

    @contextmanager
    def transaction(self):
        """Run the writes inside as one atomic, single-fsync unit; roll back if anything raises.
        Nests: only the outermost block begins and commits."""
        if self._depth:
            self._depth += 1
            try:
                yield
            finally:
                self._depth -= 1
            return
        self.db.execute('BEGIN IMMEDIATE')
        self._depth = 1
        try:
            yield
            self.db.execute('COMMIT')
        except BaseException:
            if self.db.in_transaction:
                self.db.execute('ROLLBACK')
            self._pending = []
            raise
        finally:
            self._depth = 0
        self._published()

    def _job(self, row):
        data = dict(row)
        for f in JSON_FIELDS:
            data[f] = json.loads(data[f])
        data['exclusive'] = bool(data['exclusive'])
        data['watch'] = bool(data['watch'])
        # Columns a newer version added are ignored, so rolling back never breaks reading jobs.
        return Job(**{k: v for k, v in data.items() if k in JOB_FIELDS})

    def add_job(self, job):
        cur = self.db.execute(
            'INSERT INTO jobs (state, kind, owner, label, hosts, mem_gb, exclusive, uses, takes_down, '
            'created_at, heartbeat_at, expect_s, command, note, mem_map, serves, serves_caps, watch, seen_at, '
            'load, quiet, start_at, deadline, notify) VALUES (' + ','.join('?' * 24) + ')',
            (job.state, job.kind, job.owner, job.label, json.dumps(job.hosts), job.mem_gb,
             int(job.exclusive), json.dumps(job.uses), json.dumps(job.takes_down), job.created_at,
             job.heartbeat_at, job.expect_s, job.command, job.note, json.dumps(job.mem_map),
             json.dumps(job.serves), json.dumps(job.serves_caps), int(job.watch), job.seen_at,
             json.dumps(job.load), json.dumps(job.quiet), job.start_at, job.deadline, job.notify))
        job.id = cur.lastrowid
        return job

    def update_job(self, job_id, **fields):
        for f in JSON_FIELDS:
            if f in fields:
                fields[f] = json.dumps(fields[f])
        cols = ', '.join(f'{k} = ?' for k in fields)
        self.db.execute(f'UPDATE jobs SET {cols} WHERE id = ?', (*fields.values(), job_id))

    def job(self, job_id):
        row = self.db.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
        return self._job(row) if row else None

    def active_jobs(self):
        rows = self.db.execute("SELECT * FROM jobs WHERE state IN ('queued', 'running') ORDER BY id")
        return [self._job(r) for r in rows]

    def recent_jobs(self, limit=10):
        rows = self.db.execute("SELECT * FROM jobs WHERE state NOT IN ('queued', 'running') "
                               'ORDER BY ended_at DESC, id DESC LIMIT ?', (limit,))
        return [self._job(r) for r in rows]

    def stats_rows(self, since):
        """Jobs that overlap [since, now] as dicts: submitted, ended or still active since then.

        Both range terms use an index (jobs_created, jobs_ended), and a job still running has
        ended_at NULL, which jobs_ended also serves. Only the columns the stats read are fetched,
        and those a database predates are left out.
        """
        have = {r['name'] for r in self.db.execute('PRAGMA table_info(jobs)')}
        cols = [c for c in STATS_COLUMNS if c in have]
        rows = self.db.execute(f'SELECT {", ".join(cols)} FROM jobs WHERE created_at >= ? '
                               'OR ended_at >= ? OR ended_at IS NULL', (since, since))
        out = []
        for r in rows:
            d = dict(r)
            d['hosts'] = json.loads(d['hosts'])
            out.append(d)
        return out

    def restores(self):
        return {r['service']: Restore(**dict(r)) for r in self.db.execute('SELECT * FROM restores')}

    def add_restore(self, restore):
        self.db.execute('INSERT OR REPLACE INTO restores (service, job_id, owner, label, since) '
                        'VALUES (?,?,?,?,?)', (restore.service, restore.job_id, restore.owner,
                                               restore.label, restore.since))

    def clear_restore(self, service):
        self.db.execute('DELETE FROM restores WHERE service = ?', (service,))

    def event(self, at, kind, text, job_id=None, service=None):
        self.db.execute('INSERT INTO events (at, job_id, service, kind, text) VALUES (?,?,?,?,?)',
                        (at, job_id, service, kind, text))
        self._pending.append({'at': at, 'kind': kind, 'text': text, 'job': job_id, 'service': service})
        if not self._depth:
            self._published()

    def _published(self):
        """Tell on_commit about events that are now durable. It must not break the write that made them."""
        events, self._pending = self._pending, []
        if events and self.on_commit:
            try:
                self.on_commit(events)
            except Exception as exc:
                sys.stderr.write(f'event hook failed: {exc!r}\n')

    def has_event(self, job_id, kind):
        return self.db.execute('SELECT 1 FROM events WHERE job_id = ? AND kind = ? LIMIT 1',
                               (job_id, kind)).fetchone() is not None

    def last_event_kind(self, service, kinds):
        """The kind of the latest event among `kinds` written for a service, or None."""
        row = self.db.execute(f'SELECT kind FROM events WHERE service = ? AND kind IN ({",".join("?" * len(kinds))}) '
                              'ORDER BY id DESC LIMIT 1', (service, *kinds)).fetchone()
        return row['kind'] if row else None

    def has_event_since(self, kind, since, job_id=None, service=None):
        """Was a kind of event written at or after `since` for this job and/or service?"""
        where, args = ['kind = ?', 'at >= ?'], [kind, since]
        for col, val in (('job_id', job_id), ('service', service)):
            if val is not None:
                where.append(f'{col} = ?')
                args.append(val)
        return self.db.execute(f'SELECT 1 FROM events WHERE {" AND ".join(where)} LIMIT 1', args).fetchone() is not None

    def events(self, limit=30):
        rows = self.db.execute('SELECT * FROM events ORDER BY id DESC LIMIT ?', (limit,))
        return [dict(r) for r in rows]

    def prune_events(self, before):
        return self.db.execute('DELETE FROM events WHERE at < ?', (before,)).rowcount
