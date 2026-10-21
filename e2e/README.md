# Fleetq CLI/HTTP E2E

Run from the repository:

```sh
uv sync --group dev
.venv/bin/python -m pytest -q
cd e2e
npm ci
npm test
```

TesterArmy `e2e` 0.17.0 uses its documented fixture-only CLI engine. Six deterministic scenarios invoke the actual Python CLI as subprocesses against a real threaded HTTP handler, scheduler and SQLite store. Every scenario creates a temporary database/config and two synthetic machines; machine health is injected. No SSH, hardware probe, model request, production queue, force-release or queue bypass occurs. This suite requires loopback binding (sandbox permission when restricted).

Coverage:

1. Status, roster, event log, acquire/show/release, owner rejection, inside covering/noncovering leases.
2. Exclusive blocking, queued cancellation, wait timeout, release promotion; shared memory capacity and strict FIFO preventing smaller work from bypassing exclusive jobs.
3. Run exit propagation, child job ID, nested covering job reuse, multi-host `--each` placeholders/environment and automatic release.
4. Invalid hosts/IDs/memory/config, HTTP malformed JSON/authentication failure, unreachable server exit code.
5. Queued run timeout (command cannot start), SIGTERM of running CLI/child and subsequent exclusive slot availability.

Existing pytest tests additionally cover service URL resolution, takedown/restoration, stale/overdue reporting, orphan queued jobs, real memory/busy GPU observations, untracked requests, per-host memory parsing, parallel rank failure cancellation and keep-going behavior. These are fixture tests, not proof of real fleet reachability, hardware performance or deployed service restoration.

Verified on 2026-10-04: **6/6 TesterArmy scenarios passed in 16.00 seconds**; **47/47 existing pytest tests passed in 15.77 seconds**. No application bug was reproduced. Read-only `fleetq status` could not resolve live hostname `fleetq`; live infrastructure was not changed. No external prerequisites are needed for the synthetic suite beyond Python dev environment, Node and installed npm dependencies.
