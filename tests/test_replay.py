import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'tools'))
import replay  # noqa: E402

try:
    subprocess.run(['git', '-C', str(replay.ROOT), 'cat-file', '-e', f'{replay.OLD_REV}:src/fleetq/core.py'],
                   check=True, capture_output=True)
except (subprocess.CalledProcessError, FileNotFoundError):
    pytest.skip('old scheduler commit not in this checkout', allow_module_level=True)


@pytest.fixture(autouse=True)
def capacity(monkeypatch):
    monkeypatch.setitem(replay.CAPACITY_GB, 'gpu-a', 100)


def rec(i, label, created, dur, excl=False, kind='lease'):
    return replay.Rec(id=i, kind=kind, owner='o', label=label, hosts=['gpu-a'], mem_gb=10, mem_map={},
                      exclusive=excl, uses=[], takes_down=[], created=created, started=created,
                      ended=created + dur)


def trace():
    recs = [rec(1, 'bench quality run', 0, 1000, excl=True),
            rec(2, 'download weights', 10, 50),
            rec(3, 'cargo build', 20, 50),
            rec(4, 'gpu smoke test', 30, 100)]
    recs += [rec(5 + k, f'filler {k}', 5000 + 100 * k, 10) for k in range(6)]
    return recs


def waits(recs, out):
    return {r.id: out.start[r.id] - r.created for r in recs}


def test_classify():
    assert replay.classify('download weights')[0] == 'net'
    assert replay.classify('Build only (no GPU)')[0] == 'cpu'
    assert replay.classify('bench quality run')[0] == 'bench'
    assert replay.classify('bench build')[0] == 'bench'
    assert replay.classify('gpu smoke test')[0] is None


def test_baseline_vs_inferred_load():
    recs = trace()
    base = replay.simulate(recs, replay.load_old_core())
    inferred, cov, stats = replay.infer_loads(recs)
    new = replay.simulate(inferred, replay.new_core)
    wa, wc = waits(recs, base), waits(recs, new)
    assert base.stuck == new.stuck == 0
    # old: the exclusive benchmark holds everything back until it ends at t=1000
    assert (wa[2], wa[3], wa[4]) == (990, 980, 970)
    # new: the benchmark is quiet on gpu+cpu only, so the download (net/disk) runs beside it;
    # the cpu build and the gpu job still wait
    assert wc[2] == 0
    assert (wc[3], wc[4]) == (980, 970)
    assert stats['bench exclusive->quiet'] == 1 and stats['no-GPU'] == 2
    assert set(cov) >= {'bench', 'quality', 'download', 'cargo', 'build'}
    assert all(wa[i] == wc[i] == 0 for i in range(5, 11))


def test_perfect_expect_backfills_with_deadline():
    # a long run job holds the host; a big queued job waits; a short run job may go first
    recs = [rec(1, 'long', 0, 1000, kind='run'), rec(2, 'big', 10, 100, excl=True),
            rec(3, 'short', 20, 100, kind='run')]
    recs[0].exclusive = False
    for r in recs:
        r.hosts = ['gpu-a']
    out = replay.simulate(replay.perfect_expect(recs), replay.new_core)
    assert out.stuck == 0 and out.killed == 0
    # the short job starts at its arrival only if it is not blocked: the big job is exclusive and
    # waits for 'long'; 'short' would hold it up, so it must wait unless its deadline fits in 1000s
    assert out.ahead == 1 and out.start[3] == 20
    assert out.start[2] == 1000
