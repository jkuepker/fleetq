"""SQLite persistence for jobs, pending restores and the event log."""
from __future__ import annotations

import json
import sqlite3
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
  watch INTEGER NOT NULL DEFAULT 0, seen_at REAL);
CREATE INDEX IF NOT EXISTS jobs_state ON jobs(state);
CREATE TABLE IF NOT EXISTS restores (
  service TEXT PRIMARY KEY, job_id INTEGER NOT NULL, owner TEXT NOT NULL, label TEXT NOT NULL,
  since REAL NOT NULL);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT, at REAL NOT NULL, job_id INTEGER, service TEXT,
  kind TEXT NOT NULL, text TEXT NOT NULL);
"""

JOB_FIELDS = {f.name for f in fields(Job)}
JSON_FIELDS = ('hosts', 'uses', 'takes_down', 'mem_map', 'serves', 'serves_caps')
# Columns added after the first schema; older databases gain them in place.
ADDED = {'mem_map': "TEXT NOT NULL DEFAULT '{}'", 'serves': "TEXT NOT NULL DEFAULT '[]'",
         'serves_caps': "TEXT NOT NULL DEFAULT '[]'", 'watch': 'INTEGER NOT NULL DEFAULT 0',
         'seen_at': 'REAL'}


class Store:
    def __init__(self, path):
        # One connection guarded by the server's lock; check_same_thread off for the HTTP threads.
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript(SCHEMA)
        have = {r['name'] for r in self.db.execute('PRAGMA table_info(jobs)')}
        for col, ddl in ADDED.items():
            if col not in have:
                self.db.execute(f'ALTER TABLE jobs ADD COLUMN {col} {ddl}')

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
            'created_at, heartbeat_at, expect_s, command, note, mem_map, serves, serves_caps, watch, seen_at) '
            'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (job.state, job.kind, job.owner, job.label, json.dumps(job.hosts), job.mem_gb,
             int(job.exclusive), json.dumps(job.uses), json.dumps(job.takes_down), job.created_at,
             job.heartbeat_at, job.expect_s, job.command, job.note, json.dumps(job.mem_map),
             json.dumps(job.serves), json.dumps(job.serves_caps), int(job.watch), job.seen_at))
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

    def has_event(self, job_id, kind):
        return self.db.execute('SELECT 1 FROM events WHERE job_id = ? AND kind = ? LIMIT 1',
                               (job_id, kind)).fetchone() is not None

    def events(self, limit=30):
        rows = self.db.execute('SELECT * FROM events ORDER BY id DESC LIMIT ?', (limit,))
        return [dict(r) for r in rows]
