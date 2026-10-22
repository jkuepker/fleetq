# Replaying a queue history

`tools/replay.py` is a development tool (it is not installed with the package). It reads the
finished jobs from a fleetq database, replays them in submission order through the scheduler on
a simulated clock, and reports how long jobs waited under each variant:

```bash
uv run python tools/replay.py path/to/fleetq.db --out report.md \
    --capacity gpu-a=96 --capacity gpu-b=24 --json raw.json
```

Work on a copy of the database; it is opened read-only, but a copy keeps a live server out of it.

- `--capacity HOST=GB` gives each host its memory (default: the biggest job that ran there, which
  makes the host look fuller than it was).
- `--baseline REV` names the git revision whose `src/fleetq/core.py` is the baseline scheduler
  (default `bc2364a`, the strict per-host first-come scheduler).
- `--out` writes the tables as Markdown, `--labels` the example job labels behind each inference
  rule, `--json` the raw numbers.

Variants compared: the baseline scheduler on the recorded fields; the current scheduler on the
recorded fields; the current scheduler with `--load` / `--quiet` inferred from job labels
(benchmarks become quiet instead of exclusive, downloads and builds stop claiming the GPU);
the same with exact `--expect` for run jobs (the ceiling for backfill); the same with noisy
estimates (each multiplied by a random 0.7 to 1.5, so some backfilled jobs overrun and would
be stopped at their deadline); and estimates learned from earlier jobs with the same label
prefix.

What is stubbed: every service is up and takes 0 GB (service memory never blocks), no
untracked load, hosts always online; `uses` and `takes_down` are taken as recorded. Label
inference is a handful of regular expressions at the top of the script; edit them to match
how your users name jobs.
