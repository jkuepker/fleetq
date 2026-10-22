"""Learned expected durations for jobs that give no --expect.

Pure functions over plain rows: no clock reads, no I/O. A row is a dict with label, owner,
hosts (a list), started_at and ended_at, as the store's finished_rows returns them. Jobs of
one kind with one label prefix (see stats.label_prefix) tend to take about as long as they
did last time, so a high quantile of those durations, padded, makes a better guess than one
flat default for every job.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from .stats import label_prefix, percentile

FLOOR_S = 120          # never estimate less than this
SCOPES = ('owner+hosts', 'owner', 'hosts', 'all')  # most to least specific
# `--expect auto` turns a learned value into a given one, which may set a deadline and so stop the
# job. It is only for history that is clearly the caller's own, of a recognisable kind, and steady.
AUTO_MIN_SAMPLES = 5
AUTO_SCOPES = ('owner+hosts', 'owner')
AUTO_MIN_PREFIX = 3     # characters: shorter label prefixes ('', 'm', 'p') lump unrelated jobs
AUTO_MAX_SPREAD = 3.0   # p95 / median of the durations


@dataclass
class Estimate:
    seconds: float | None  # None: no estimate; use the default
    source: str            # learned | default
    n: int = 0             # runs it is learned from
    scope: str = ''        # which of SCOPES the runs came from
    capped: bool = False   # the history said longer than default_s; seconds is default_s
    spread: float = 0.0    # p95 / median of the group's durations (inf: median 0)


def index(rows):
    """prefix -> [(duration_s, owner, frozenset(hosts))] over finished rows, for estimate()."""
    out = defaultdict(list)
    for r in rows:
        start, end = r.get('started_at'), r.get('ended_at')
        if start is None or end is None or r.get('state', 'done') != 'done':
            continue
        out[label_prefix(r['label'])].append((max(end - start, 0.0), r['owner'], frozenset(r['hosts'])))
    return dict(out)


def estimate(samples, owner, hosts, default_s, min_samples=3, quantile=0.8, factor=1.25, scopes=SCOPES):
    """Expected duration of a job like the ones in samples (index()[prefix], or None).

    Takes the most specific group with at least min_samples runs (same owner and hosts, then
    same owner, then same hosts, then anyone): its quantile times factor, at least FLOOR_S.
    Never more than default_s; an estimate that hit it is marked capped, since the history says
    "at least that long", which is not something to hold a job to.
    """
    samples = samples or []
    hosts = frozenset(hosts)
    groups = (('owner+hosts', [s for s in samples if s[1] == owner and s[2] == hosts]),
              ('owner', [s for s in samples if s[1] == owner]),
              ('hosts', [s for s in samples if s[2] == hosts]),
              ('all', samples))
    for scope, group in groups:
        if scope in scopes and len(group) >= max(min_samples, 1):
            durations = [s[0] for s in group]
            seconds = max(percentile(durations, quantile) * factor, FLOOR_S)
            mid = percentile(durations, 0.5)
            spread = percentile(durations, 0.95) / mid if mid else float('inf')
            return Estimate(min(seconds, default_s), 'learned', len(group), scope, seconds >= default_s, spread)
    return Estimate(None, 'default', 0, '')


def auto_estimate(samples, prefix, owner, hosts, default_s, quantile=0.8, factor=1.25):
    """(Estimate, None) if `--expect auto` may take the estimate as given, else (None, why not)."""
    if len(prefix) < AUTO_MIN_PREFIX:
        return None, (f'the label starts with "{prefix}", too short to tell jobs apart; '
                      f'start it with a descriptive word of {AUTO_MIN_PREFIX}+ characters')
    est = estimate(samples, owner, hosts, default_s, AUTO_MIN_SAMPLES, quantile, factor, AUTO_SCOPES)
    if est.source != 'learned':
        have = sum(1 for s in samples or [] if s[1] == owner)
        return None, (f'needs {AUTO_MIN_SAMPLES} finished runs of yours whose label starts with "{prefix}" '
                      f'(have {have})')
    if est.capped:
        return None, f'the estimate reaches the {default_s / 60:g}-minute default, so there is no tighter figure'
    if est.spread > AUTO_MAX_SPREAD:
        return None, (f'durations vary too much (slowest 5% take {est.spread:.1f}x the median; '
                      f'limit {AUTO_MAX_SPREAD:g}x)')
    return est, None
