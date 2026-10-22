#!/usr/bin/env python3
"""Trace-replay simulator for fleetq's scheduler (dev tool, not shipped in the package).

Replays the jobs that really ran in a production snapshot (created_at order, real run
duration) through a scheduler variant on a simulated clock, and reports waits.
The scheduler is the real fleetq.core (plan/schedule); the baseline is an older
fleetq.core loaded from git (--baseline REV, by default the strict per-host FIFO scheduler).

    uv run python tools/replay.py PATH/fleetq.db [--out report.md] [--json out.json]
        [--capacity gpu-a=96 --capacity gpu-b=24] [--baseline REV]

What is stubbed: every service is up with 0 GB (so service memory never blocks), no
untracked load, hosts always online. uses/takes_down are taken as recorded.
"""
from __future__ import annotations

import argparse
import dataclasses
import heapq
import importlib.util
import json
import math
import random
import re
import sqlite3
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'src'))

from fleetq import core as new_core  # noqa: E402

OLD_REV = 'bc2364a'  # a commit with the strict per-host FIFO scheduler (the public line before backfill)
CAPACITY_GB = {}     # host name -> GB; --capacity fills it. A host never has less than its biggest job.
INF = float('inf')

# --- label inference ---------------------------------------------------------------------
# (rule name, regex, load). Net rules win over cpu rules; benchmark rules win over both.
NOGPU_RULES = [
    ('no GPU', r'no gpu', 'cpu'),
    ('CPU only', r'cpu only', 'cpu'),
    ('CPU/net', r'cpu/net', 'net'),
    ('download', r'download', 'net'),
    ('build', r'\bbuild', 'cpu'),
    ('uv sync', r'uv sync', 'net'),
    ('cargo', r'cargo', 'cpu'),
    ('compile', r'compile', 'cpu'),
    ('NFS', r'\bnfs\b', 'net'),
    ('rsync', r'rsync', 'net'),
]
BENCH_RULES = [
    ('bench', r'bench', None),
    ('speed', r'speed', None),
    ('quality', r'quality', None),
    ('mmlu', r'mmlu', None),
    ('gsm8k', r'gsm8k', None),
]
NOGPU_LOADS = {'cpu': ['cpu'], 'net': ['net', 'disk']}
BENCH_QUIET = ['gpu', 'cpu']


# build / cargo / compile can also mean "build, then test on the GPU"; strict mode drops them.
LOOSE_RULES = {'build', 'cargo', 'compile'}


def classify(label, strict=False):
    """(kind, rules matched): kind is 'bench', 'net', 'cpu' or None."""
    low = label.lower()
    bench = [n for n, rx, _ in BENCH_RULES if re.search(rx, low)]
    nogpu = [(n, ld) for n, rx, ld in NOGPU_RULES
             if re.search(rx, low) and not (strict and n in LOOSE_RULES)]
    rules = bench + [n for n, _ in nogpu]
    if bench:
        return 'bench', rules
    if nogpu:
        return ('net' if any(ld == 'net' for _, ld in nogpu) else 'cpu'), rules
    return None, rules


# --- trace -------------------------------------------------------------------------------
@dataclasses.dataclass
class Rec:
    id: int
    kind: str
    owner: str
    label: str
    hosts: list
    mem_gb: float
    mem_map: dict
    exclusive: bool
    uses: list
    takes_down: list
    created: float
    started: float          # as recorded
    ended: float            # as recorded
    expect_s: float | None = None
    load: list = dataclasses.field(default_factory=lambda: ['gpu'])
    quiet: list = dataclasses.field(default_factory=list)

    @property
    def dur(self):
        return max(self.ended - self.started, 1.0)


def load_trace(db_path):
    con = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    con.row_factory = sqlite3.Row
    rows = con.execute('select * from jobs where started_at is not null and ended_at is not null '
                       'and state="done" order by created_at, id').fetchall()
    recs = []
    for i, r in enumerate(rows, 1):
        recs.append(Rec(id=i, kind=r['kind'], owner=r['owner'], label=r['label'],
                        hosts=json.loads(r['hosts']), mem_gb=r['mem_gb'],
                        mem_map=json.loads(r['mem_map'] or '{}'), exclusive=bool(r['exclusive']),
                        uses=json.loads(r['uses']), takes_down=json.loads(r['takes_down']),
                        created=r['created_at'], started=r['started_at'], ended=r['ended_at'],
                        expect_s=r['expect_s'], load=json.loads(r['load'] or '["gpu"]'),
                        quiet=json.loads(r['quiet'] or '[]')))
    return recs


def load_old_core(rev=OLD_REV):
    src = subprocess.run(['git', '-C', str(ROOT), 'show', f'{rev}:src/fleetq/core.py'],
                         capture_output=True, text=True, check=True).stdout
    path = Path(tempfile.mkdtemp(prefix='fleetq_old_')) / 'fleetq_core_old.py'
    path.write_text(src)
    spec = importlib.util.spec_from_file_location('fleetq_core_old', path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules['fleetq_core_old'] = mod  # dataclasses look the module up by name
    spec.loader.exec_module(mod)
    return mod


# --- variants: transformations of the trace ------------------------------------------------
def copy_recs(recs):
    return [dataclasses.replace(r, hosts=list(r.hosts), load=list(r.load), quiet=list(r.quiet)) for r in recs]


def infer_loads(recs, drop_exclusive=False, strict=False):
    """Apply the label rules. Returns (recs, coverage) with coverage[rule] = [labels]."""
    out = copy_recs(recs)
    cov = defaultdict(list)
    stats = Counter()
    for r in out:
        kind, rules = classify(r.label, strict)
        for n in rules:
            cov[n].append(r.label)
        if kind == 'bench':
            stats['bench'] += 1
            if r.exclusive:
                stats['bench exclusive->quiet'] += 1
                r.exclusive, r.quiet = False, list(BENCH_QUIET)
        elif kind:
            stats['no-GPU'] += 1
            r.load = list(NOGPU_LOADS[kind])
            if r.exclusive:
                stats['no-GPU was exclusive'] += 1
                if drop_exclusive:
                    r.exclusive = False
            if r.uses:
                stats['no-GPU but uses a service (still gpu load)'] += 1
    return out, cov, stats


def drop_all_exclusive(recs):
    """Upper bound: nobody is exclusive, only memory (and takes_down/uses) limits sharing."""
    out = copy_recs(recs)
    for r in out:
        r.exclusive = False
    return out


def perfect_expect(recs, kinds=('run',)):
    out = copy_recs(recs)
    for r in out:
        if r.kind in kinds:
            r.expect_s = r.dur
    return out


def noisy_expect(recs, seed, kinds=('run',)):
    rng = random.Random(seed)
    out = copy_recs(recs)
    for r in out:
        if r.kind in kinds:
            r.expect_s = r.dur * rng.uniform(0.7, 1.5)
    return out


def label_keys(label):
    toks = re.sub(r'[^\w\-./]+', ' ', label.lower()).split()
    return [' '.join(toks[:n]) for n in (3, 2, 1) if toks[:n]]


def pct(xs, p):
    if not xs:
        return 0.0
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100
    lo, hi = math.floor(k), math.ceil(k)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def learned_expect(recs, q=80, min_samples=3):
    """expect = p80 of durations of earlier same-(kind, label prefix) jobs that ended before submit.
    Tries 3-, 2-, then 1-word prefixes, taking the longest with enough samples."""
    out = copy_recs(recs)
    by_key = defaultdict(list)  # key -> [(ended, dur)]
    for r in recs:
        for k in label_keys(r.label):
            by_key[(r.kind, k)].append((r.ended, r.dur))
    for r in out:
        r.expect_s = None
        for k in label_keys(r.label):
            prior = [d for e, d in by_key[(r.kind, k)] if e < r.created]
            if len(prior) >= min_samples:
                r.expect_s = pct(prior, q)
                break
    return out


# --- simulation ----------------------------------------------------------------------------
def make_job(core, r, state):
    names = {f.name for f in dataclasses.fields(core.Job)}
    kw = dict(id=r.id, state=state, kind=r.kind, owner=r.owner, label=r.label, hosts=list(r.hosts),
              mem_gb=r.mem_gb, mem_map=dict(r.mem_map), exclusive=r.exclusive, uses=list(r.uses),
              takes_down=list(r.takes_down), created_at=r.created, expect_s=r.expect_s,
              load=list(r.load), quiet=list(r.quiet))
    return core.Job(**{k: v for k, v in kw.items() if k in names})


def roster(core, recs):
    names = sorted({h for r in recs for h in r.hosts})
    mem = defaultdict(float)
    for r in recs:
        for h in r.hosts:
            mem[h] = max(mem[h], r.mem_map.get(h, r.mem_gb))
    hosts = {h: core.Host(h, h.split('-')[0], max(CAPACITY_GB.get(h, 0), mem[h])) for h in names}
    where = defaultdict(Counter)
    for r in recs:
        for s in r.uses + r.takes_down:
            where[s][r.hosts[0]] += 1
    services = [core.Service(s, c.most_common(1)[0][0], 0.0) for s, c in sorted(where.items())]
    statuses = {s.name: core.ServiceStatus('up', 'up', True, False) for s in services}
    return hosts, services, statuses


@dataclasses.dataclass
class Outcome:
    start: dict           # rec id -> sim start
    end: dict             # rec id -> sim end
    ahead: int = 0        # jobs started with a backfill deadline
    killed: int = 0       # ahead jobs that would have been stopped at their deadline
    lost_s: float = 0.0   # work cut off by those kills
    overrun: int = 0      # ahead jobs that outlived their deadline (not enforced)
    overrun_s: float = 0.0
    stuck: int = 0        # never started


def simulate(recs, core, enforce=True):
    hosts, services, statuses = roster(core, recs)
    online = {h: True for h in hosts}
    is_new = hasattr(core, 'plan')
    by_id = {r.id: r for r in recs}
    order = sorted(recs, key=lambda r: (r.created, r.id))
    out = Outcome({}, {})
    active, running_ends = [], []
    n, i = len(order), 0
    while i < n or running_ends:
        t = min(order[i].created if i < n else INF, running_ends[0][0] if running_ends else INF)
        while running_ends and running_ends[0][0] <= t:
            _, jid = heapq.heappop(running_ends)
            job = next(j for j in active if j.id == jid)
            job.state = core.DONE
            active.remove(job)
        while i < n and order[i].created <= t:
            active.append(make_job(core, order[i], core.QUEUED))
            i += 1
        for _ in range(100):
            if is_new:
                p = core.plan(list(active), hosts, services, statuses, {}, online, {}, None, t)
                to_start, deadlines = p.to_start, p.deadlines
            else:
                to_start, _ = core.schedule(list(active), hosts, services, statuses, {}, online, {})
                deadlines = {}
            if not to_start:
                break
            for jid in to_start:
                job = next(j for j in active if j.id == jid)
                dur = by_id[jid].dur
                end = t + dur
                dl = deadlines.get(jid)
                if dl is not None:
                    out.ahead += 1
                    if end > dl + 1e-6:
                        if enforce:
                            out.killed += 1
                            out.lost_s += end - dl
                            end = dl
                        else:
                            out.overrun += 1
                            out.overrun_s += end - dl
                    if not enforce:
                        dl = None  # learned estimates carry no kill; the scheduler just trusts expect
                job.state, job.started_at, job.deadline = core.RUNNING, t, dl
                out.start[jid], out.end[jid] = t, end
                heapq.heappush(running_ends, (end, jid))
        if i >= n and not running_ends:
            break
    out.stuck = sum(1 for j in active if j.state == core.QUEUED)
    return out


# --- statistics ----------------------------------------------------------------------------
def busy_fraction(intervals, lo, hi):
    ivs = sorted((max(s, lo), min(e, hi)) for s, e in intervals if e > lo and s < hi)
    total, cur_s, cur_e = 0.0, None, None
    for s, e in ivs:
        if cur_e is None or s > cur_e:
            if cur_e is not None:
                total += cur_e - cur_s
            cur_s, cur_e = s, e
        else:
            cur_e = max(cur_e, e)
    if cur_e is not None:
        total += cur_e - cur_s
    return total / (hi - lo)


def summarize(recs, start, end, span):
    """Wait stats per host (multi-host jobs count once per host) and overall."""
    waits = {r.id: start[r.id] - r.created for r in recs if r.id in start}
    per = defaultdict(list)
    ivs = defaultdict(list)
    for r in recs:
        if r.id not in start:
            continue
        for h in r.hosts:
            per[h].append(waits[r.id])
            ivs[h].append((start[r.id], end[r.id]))

    def row(ws, busy=None):
        d = {'n': len(ws), 'median_s': pct(ws, 50), 'p90_s': pct(ws, 90), 'p99_s': pct(ws, 99),
             'total_h': sum(ws) / 3600}
        if busy is not None:
            d['busy'] = busy
        return d
    res = {'overall': row(list(waits.values()),
                          sum(busy_fraction(v, *span) for v in ivs.values()) / max(len(ivs), 1)),
           'hosts': {h: row(w, busy_fraction(ivs[h], *span)) for h, w in sorted(per.items())}}
    top = sorted(((w, rid) for rid, w in waits.items()), reverse=True)[:5]
    by_id = {r.id: r for r in recs}
    res['top'] = [{'wait_min': w / 60, 'label': by_id[rid].label[:70], 'hosts': ','.join(by_id[rid].hosts),
                   'kind': by_id[rid].kind} for w, rid in top]
    return res


def actual_outcome(recs):
    return Outcome({r.id: r.started for r in recs}, {r.id: r.ended for r in recs})


def run_all(recs, noise_seeds=(1, 2, 3), baseline=OLD_REV):
    old, span = load_old_core(baseline), (min(r.created for r in recs), max(r.ended for r in recs))
    results, extra = {}, {}

    def go(name, rs, core=new_core, enforce=True):
        o = simulate(rs, core, enforce)
        results[name] = summarize(rs, o.start, o.end, span)
        results[name]['sim'] = {k: getattr(o, k) for k in ('ahead', 'killed', 'lost_s', 'overrun', 'overrun_s', 'stuck')}
        return o

    ao = actual_outcome(recs)
    results['actual'] = summarize(recs, ao.start, ao.end, span)
    go('a baseline (old FIFO)', recs, old)
    go('b new, historical fields', recs)
    c, cov, stats = infer_loads(recs)
    go('c b + inferred load/quiet', c)
    c2, _, _ = infer_loads(recs, drop_exclusive=True)
    go('c2 c + no-GPU jobs not exclusive', c2)
    c2s, _, _ = infer_loads(recs, drop_exclusive=True, strict=True)
    go('c2s c2 without build/cargo/compile rules', c2s)
    go('x ceiling: c with no job exclusive', drop_all_exclusive(c))
    go('d c + perfect expect (run jobs)', perfect_expect(c))
    go('d2 c + perfect expect (all jobs)', perfect_expect(c, ('run', 'lease')))
    for kinds, tag in ((('run',), 'run jobs'), (('run', 'lease'), 'all jobs')):
        sims = []
        for seed in noise_seeds:
            rs = noisy_expect(c, seed, kinds)
            o = simulate(rs, new_core)
            sims.append((o, summarize(rs, o.start, o.end, span)))
        name = f'd-noise c + expect*U(0.7,1.5) ({tag})'
        results[name] = sims[0][1]
        results[name]['sim'] = {
            'ahead': [o.ahead for o, _ in sims], 'killed': [o.killed for o, _ in sims],
            'lost_s': [o.lost_s for o, _ in sims], 'stuck': [o.stuck for o, _ in sims],
            'p90_s': [s['overall']['p90_s'] for _, s in sims],
            'note': f'seeds {list(noise_seeds)}; table row is seed {noise_seeds[0]}'}
    le = learned_expect(c)
    go('e c + learned expect (p80, no kills)', le, enforce=False)
    base = results['a baseline (old FIFO)']
    ex_ids = {r.id for r in recs if r.exclusive}
    ao_waits = {r.id: (ao.start[r.id] - r.created) for r in recs}
    tot = sum(ao_waits.values()) or 1
    extra['actual_wait_share'] = {
        'exclusive jobs themselves': sum(w for i, w in ao_waits.items() if i in ex_ids) / tot,
        'jobs >10 min wait': sum(w for w in ao_waits.values() if w > 600) / tot,
        'exclusive share of jobs': len(ex_ids) / len(recs)}
    extra['coverage'] = {k: v for k, v in cov.items()}
    extra['inference_stats'] = dict(stats)
    extra['learned_expect_coverage'] = sum(1 for r in le if r.expect_s is not None)
    extra['n_jobs'] = len(recs)
    extra['span_days'] = (span[1] - span[0]) / 86400
    return results, extra


# --- report --------------------------------------------------------------------------------
def fmt_m(s):
    return f'{s / 60:.1f}'


def render(results, extra, sample=20):
    """(tables markdown, label examples markdown)"""
    names = list(results)
    hosts = sorted({h for r in results.values() for h in r['hosts']})
    L = [f"{extra['n_jobs']} jobs replayed over {extra['span_days']:.1f} days.", '',
         '### Overall wait', '',
         '| variant | median s | p90 min | p99 min | total wait h | busy % (mean of hosts) |',
         '|---|---:|---:|---:|---:|---:|']
    for n in names:
        o = results[n]['overall']
        L.append(f"| {n} | {o['median_s']:.0f} | {fmt_m(o['p90_s'])} | {fmt_m(o['p99_s'])} | "
                 f"{o['total_h']:.1f} | {o['busy'] * 100:.1f} |")
    for metric, title in (('p90_s', 'p90 wait (min) per host'), ('median_s', 'median wait (s) per host'),
                          ('p99_s', 'p99 wait (min) per host'), ('total_h', 'total wait (h) per host')):
        L += ['', f'### {title}', '', '| variant | ' + ' | '.join(hosts) + ' |',
              '|---|' + '---:|' * len(hosts)]
        for n in names:
            cells = []
            for h in hosts:
                v = results[n]['hosts'].get(h)
                if not v:
                    cells.append('-')
                elif metric in ('p90_s', 'p99_s'):
                    cells.append(fmt_m(v[metric]))
                elif metric == 'median_s':
                    cells.append(f'{v[metric]:.0f}')
                else:
                    cells.append(f'{v[metric]:.1f}')
            L.append(f'| {n} | ' + ' | '.join(cells) + ' |')
    L += ['', '### Busy % per host', '', '| variant | ' + ' | '.join(hosts) + ' |', '|---|' + '---:|' * len(hosts)]
    for n in names:
        L.append(f'| {n} | ' + ' | '.join(f"{results[n]['hosts'][h]['busy'] * 100:.0f}" if h in results[n]['hosts']
                                           else '-' for h in hosts) + ' |')
    L += ['', 'Share of actual total wait: ' + ', '.join(f'{k} {v:.0%}' for k, v in extra['actual_wait_share'].items())]
    L += ['', '### Backfill with deadline', '', '| variant | started ahead | killed at deadline | work cut off (h) | '
          'overran deadline | never started |', '|---|---:|---:|---:|---:|---:|']
    for n in names:
        s = results[n].get('sim')
        if not s:
            continue
        if isinstance(s['ahead'], list):
            m = lambda k: ' / '.join(f'{x:.0f}' for x in s[k])  # noqa: E731
            L.append(f"| {n} | {m('ahead')} | {m('killed')} | "
                     f"{' / '.join(f'{x / 3600:.1f}' for x in s['lost_s'])} | - | {m('stuck')} |")
        else:
            L.append(f"| {n} | {s['ahead']} | {s['killed']} | {s['lost_s'] / 3600:.1f} | "
                     f"{s['overrun']} ({s['overrun_s'] / 3600:.1f} h) | {s['stuck']} |")
    L += ['', '### Five longest waits', '']
    for n in names:
        if n.split()[0] not in ('actual', 'a', 'c2', 'x', 'd'):
            continue
        L += [f'**{n}**', '']
        for t in results[n]['top']:
            L.append(f"- {t['wait_min']:.0f} min: {t['label']} ({t['kind']}, {t['hosts']})")
        L.append('')
    L += ['### Label-inference coverage', '', f"Stats: {extra['inference_stats']}; "
          f"learned expect available for {extra['learned_expect_coverage']} jobs.", '',
          '| rule | jobs touched |', '|---|---:|']
    for rule, labels in extra['coverage'].items():
        L.append(f'| {rule} | {len(labels)} |')
    ex = [f'## {sample} example labels per rule (evenly spaced through the distinct matches)', '']
    for rule, labels in extra['coverage'].items():
        uniq = list(dict.fromkeys(labels))
        step = max(len(uniq) // sample, 1)
        ex += [f'**{rule}** ({len(labels)} jobs, {len(uniq)} distinct labels)', ''] + \
              [f'- {x[:110]}' for x in uniq[::step][:sample]] + ['']
    return '\n'.join(L), '\n'.join(ex)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('db')
    ap.add_argument('--out', help='write the markdown tables here')
    ap.add_argument('--labels', help='write the example labels per inference rule here')
    ap.add_argument('--json', help='write raw results here')
    ap.add_argument('--capacity', action='append', default=[], metavar='HOST=GB',
                    help='memory of a host; repeat per host (default: its biggest job)')
    ap.add_argument('--baseline', default=OLD_REV, metavar='REV',
                    help=f'git revision whose core.py is the baseline scheduler (default {OLD_REV})')
    args = ap.parse_args()
    for item in args.capacity:
        name, _, gb = item.partition('=')
        CAPACITY_GB[name] = float(gb)
    recs = load_trace(args.db)
    results, extra = run_all(recs, baseline=args.baseline)
    md, examples = render(results, extra)
    if args.out:
        Path(args.out).write_text(md + '\n')
    if args.labels:
        Path(args.labels).write_text(examples + '\n')
    if args.json:
        Path(args.json).write_text(json.dumps({'results': results, 'extra': extra}, indent=1, default=str))
    print(md)


if __name__ == '__main__':
    main()
