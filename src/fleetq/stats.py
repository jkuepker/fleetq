"""Queue statistics: pure aggregation over job rows.

No I/O and no clock reads. Rows are plain dicts shaped like the jobs table, with
`hosts` already a list. Columns a database may lack (`start_at` for reservations,
`deadline`) are read with .get, so older and newer schemas both work.
"""
from __future__ import annotations

import heapq
import re
from collections import defaultdict

LONGEST = 5          # longest waits listed per host
MIN_LABEL_RUNS = 3   # a label prefix needs this many finished runs to be reported
MAX_LABELS = 15


def percentile(values, q):
    """q in [0, 1], linear interpolation between ranks; None for no values."""
    if not values:
        return None
    v = sorted(values)
    pos = (len(v) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


def summary(values):
    return {'n': len(values), 'median_s': percentile(values, 0.5), 'p90_s': percentile(values, 0.9)}


def ready_at(row):
    """When the job could first have started: submission, or the booked start of a reservation."""
    return max(row['created_at'], row.get('start_at') or 0.0)


def wait_of(row):
    if row.get('started_at') is None:
        return None
    return max(row['started_at'] - ready_at(row), 0.0)


def interval(row, now):
    """(start, end) the job held its hosts, or None if it never started."""
    if row.get('started_at') is None:
        return None
    end = row.get('ended_at')
    if end is None:
        end = now
    return row['started_at'], max(end, row['started_at'])


def busy_and_shared(intervals, lo, hi):
    """Seconds in [lo, hi] with at least one, and with two or more, overlapping intervals."""
    points = []
    for s, e in intervals:
        s, e = max(s, lo), min(e, hi)
        if e > s:
            points += [(s, 1), (e, -1)]
    points.sort()
    busy = shared = 0.0
    depth, last = 0, None
    for t, d in points:
        if last is not None and depth >= 1:
            busy += t - last
            if depth >= 2:
                shared += t - last
        depth += d
        last = t
    return busy, shared


def backfilled_ids(rows):
    """Ids of jobs that started while a job submitted before them, on a shared host, was ready and waiting.

    One sweep over start times with a heap of waiting jobs keyed by submission order, so it is
    O(n log n) however long the queue was.
    """
    started = sorted((r for r in rows if r.get('started_at') is not None), key=lambda r: r['started_at'])
    by_ready = sorted(rows, key=ready_at)
    out, heap, nxt = set(), [], 0
    for r in started:
        s = r['started_at']
        while nxt < len(by_ready) and ready_at(by_ready[nxt]) <= s:
            k = by_ready[nxt]
            nxt += 1
            end = k.get('started_at') if k.get('started_at') is not None else k.get('ended_at')
            if end is None:
                end = float('inf')
            if end > s:
                heapq.heappush(heap, ((k['created_at'], k['id']), end, k['id']))
        while heap and heap[0][1] <= s:
            heapq.heappop(heap)
        if heap and heap[0][0] < (r['created_at'], r['id']):
            out.add(r['id'])
    return out


def _backfilled(rows):
    """backfilled_ids per host, so only waiters on a shared host count."""
    by_host = defaultdict(list)
    for r in rows:
        for h in r['hosts']:
            by_host[h].append(r)
    flagged = defaultdict(set)
    for h, hrows in by_host.items():
        flagged[h] = backfilled_ids(hrows)
    return flagged


def blocker_of(row, rows, host):
    """The job whose release most plausibly freed the slot: last to end on this host before this one started."""
    best = None
    for k in rows:
        if k['id'] == row['id'] or host not in k['hosts'] or k.get('started_at') is None:
            continue
        end = k.get('ended_at')
        if end is None or end > row['started_at'] or end < ready_at(row):
            continue
        if best is None or end > best['ended_at']:
            best = k
    if best is None:
        return None
    return {'id': best['id'], 'label': best['label'], 'owner': best['owner']}


def label_prefix(label):
    head = re.split(r'[\s:/=#@]', label.strip(), maxsplit=1)[0]
    return head.rstrip('-_.0123456789').lower() or head.lower()


def compute(rows, now, since, hosts=(), only=()):
    """Aggregate rows (see module docstring) over the window [since, now].

    `hosts` are names reported even when idle; `only` limits the report to those hosts.
    """
    if only:
        rows = [r for r in rows if set(only) & set(r['hosts'])]
    names = list(dict.fromkeys(only or [*hosts, *(h for r in rows for h in r['hosts'])]))
    wall = max(now - since, 1.0)
    flagged = _backfilled(rows)
    in_window = [r for r in rows if r.get('started_at') is not None and r['started_at'] >= since]

    per_host = {}
    for h in names:
        mine = [r for r in rows if h in r['hosts']]
        ran = [r for r in in_window if h in r['hosts']]
        busy, shared = busy_and_shared([iv for r in mine if (iv := interval(r, now))], since, now)
        waits = [(w, r) for r in ran if (w := wait_of(r)) is not None]
        longest = sorted(waits, key=lambda wr: -wr[0])[:LONGEST]
        per_host[h] = {
            'jobs': len(ran),
            'busy_h': busy / 3600,
            'busy_pct': 100 * busy / wall,
            'shared_pct': 100 * shared / busy if busy else 0.0,
            'wait': summary([w for w, _ in waits]),
            'longest_waits': [{'id': r['id'], 'label': r['label'], 'owner': r['owner'], 'wait_s': w,
                               'reason': r.get('reason') or '', 'blocker': blocker_of(r, rows, h)}
                              for w, r in longest],
            'backfilled': sum(1 for r in ran if r['id'] in flagged[h]),
            'with_deadline': sum(1 for r in ran if r.get('deadline') is not None),
            'backfilled_with_deadline': sum(1 for r in ran
                                            if r['id'] in flagged[h] and r.get('deadline') is not None),
            'cancelled_waiting': sum(1 for r in mine if r.get('started_at') is None
                                     and r['state'] == 'cancelled' and (r.get('ended_at') or 0) >= since),
            'queued_now': sum(1 for r in mine if r['state'] == 'queued'),
        }

    owners = {}
    for r in in_window:
        o = owners.setdefault(r['owner'], {'jobs': 0, 'held_h': 0.0, '_waits': []})
        o['jobs'] += 1
        s, e = interval(r, now)
        o['held_h'] += (e - s) * len(r['hosts']) / 3600
        if (w := wait_of(r)) is not None:
            o['_waits'].append(w)
    for o in owners.values():
        o['wait'] = summary(o.pop('_waits'))

    expect = {}
    for kind in ('lease', 'run'):
        done = [r for r in rows if r['kind'] == kind and r.get('expect_s') and r.get('started_at') is not None
                and r.get('ended_at') is not None and r['ended_at'] >= since]
        ratios = [(r['ended_at'] - r['started_at']) / r['expect_s'] for r in done]
        expect[kind] = {'n': len(done), 'median_ratio': percentile(ratios, 0.5),
                        'overdue_pct': 100 * sum(1 for x in ratios if x > 1) / len(ratios) if ratios else None}

    by_label = defaultdict(list)
    for r in rows:
        if r.get('started_at') is not None and r.get('ended_at') is not None and r['ended_at'] >= since:
            by_label[label_prefix(r['label'])].append(r['ended_at'] - r['started_at'])
    labels = [{'prefix': p, **summary(d)} for p, d in by_label.items() if len(d) >= MIN_LABEL_RUNS]
    labels.sort(key=lambda x: (-x['n'], x['prefix']))

    return {'since': since, 'until': now, 'days': wall / 86400, 'hosts': per_host, 'owners': owners,
            'expect': expect, 'labels': labels[:MAX_LABELS]}
