# fleetq HTTP API

The CLI is a thin client over this API; harnesses, dashboards and agents can call it
directly. Everything is JSON over HTTP on the server's port (default 8470).

## Auth

Every route except `/healthz` needs `Authorization: Bearer <token>`.

| Token | Allows |
|---|---|
| full (`token_file`) | every route |
| read-only (`readonly_token_file`, optional) | `GET /v1/status`, `/v1/models`, `/v1/roster`, `/v1/events`, `/v1/stats` |

A missing or wrong token answers `401`. The read-only token on any other route answers `403`.
Errors are `{"error": "<message>"}` with a 4xx status.

## Read

| Route | Answer |
|---|---|
| `GET /healthz` | `{"ok": true, "last_probe": <unix time>}`; no token |
| `GET /v1/status` | the whole fleet: hosts, their services, running and queued jobs, warnings |
| `GET /v1/models[?cap=image,text][&all=1]` | models that can take requests now, one per `(url, model)` |
| `GET /v1/roster` | the configured hosts and services, as loaded from the roster |
| `GET /v1/events[?limit=30]` | the most recent log lines (1 to 500) |
| `GET /v1/stats[?days=7][&host=H]` | queue statistics for the last `days` days (default 7, up to 3650), for one host (name, alias or machine) if given; see below |
| `GET /v1/jobs/<id>[?wait=N[&state=queued]]` | one job; queued jobs also carry `position`. With `wait`, a long-poll (below) |

### Statistics: `GET /v1/stats`

Read-only aggregation over the jobs table; one indexed query on `created_at` / `ended_at`
(jobs submitted, ended or still active inside the window). A job counts toward a window
when it *started* inside it; busy time is clipped to the window. Answers `400` for `days`
outside (0, 3650] or an unknown `host`.

```json
{"since": 1.7e9, "until": 1.7e9, "days": 7,
 "hosts": {"gpu-a": {
   "jobs": 210, "busy_h": 95.2, "busy_pct": 56.7, "shared_pct": 12.0,
   "wait": {"n": 210, "median_s": 240, "p90_s": 3720},
   "longest_waits": [{"id": 3401, "label": "...", "owner": "...", "wait_s": 11520,
                      "reason": "", "blocker": {"id": 3390, "label": "...", "owner": "..."}}],
   "backfilled": 14, "with_deadline": 3, "backfilled_with_deadline": 2,
   "cancelled_waiting": 5, "queued_now": 1}},
 "owners": {"alice": {"jobs": 80, "held_h": 40.5, "wait": {"n": 80, "median_s": 60, "p90_s": 900}}},
 "expect": {"lease": {"n": 40, "median_ratio": 0.62, "overdue_pct": 8.0},
            "run":   {"n": 12, "median_ratio": 0.9,  "overdue_pct": 25.0}},
 "labels": [{"prefix": "bench", "n": 31, "median_s": 1740, "p90_s": 3600}]}
```

- `busy_h` / `busy_pct`: the union of the intervals jobs held the host, against the window's
  wall time. `shared_pct`: the share of that busy time with two or more jobs at once.
- `wait`: `started_at` minus submission, or minus the booked start for a reservation
  (`start_at`). A multi-host job counts on each of its hosts.
- `longest_waits` (top 5 per host): `blocker` is inferred from the rows, the job on that
  host that ended last before this one started. The stored `reason` is cleared when a job
  starts, so it is only filled for jobs that never did.
- `backfilled`: started while a job submitted earlier, on a shared host, was ready and still
  waiting. `with_deadline` and `backfilled_with_deadline` count jobs with a `deadline` and
  stay 0 on a database without that column.
- `owners[].held_h`: job hours times hosts held.
- `expect`: finished jobs that had an `expect_s`: `median_ratio` is actual duration over
  expected, `overdue_pct` the share that ran longer. Leases without `--expect` carry the
  server default (`default_expect_min`), which the rows cannot tell apart from a real estimate.
- `labels`: median and p90 duration by label prefix (the label up to the first space, `:`,
  `/`, `=`, `#` or `@`, trailing digits and `-_.` dropped) for prefixes with 3 or more
  finished runs, most frequent first, at most 15.

### Long-poll: `GET /v1/jobs/<id>?wait=N`

Holds the request open for up to `N` seconds (capped at 30) and answers as soon as the
job's `state` or `reason` changes, so a waiting client sees its job start within a moment
of the slot freeing instead of on its next poll.

- `state`: the state the client last saw. If the job is no longer in it, the answer comes
  at once. Always send it: without it the server compares with the state when the request
  arrives, and a job that started between two polls would hold the request the full `N`.
- The answer is the job, plus `"waited": <N as capped>`. A server older than this
  feature ignores `wait` and answers at once without `waited`; fall back to plain polling
  (the CLI polls every 5 s then).
- `wait` that is not a number answers `400`.
- A `run` job still needs `POST /v1/jobs/<id>/heartbeat` every 30 s or so; keep each
  `wait` shorter than the time to the next heartbeat. Set the client's socket timeout
  above `N`.

```sh
curl -H "Authorization: Bearer $FLEETQ_TOKEN" "$FLEETQ_URL/v1/jobs/57?wait=25&state=queued"
```

### `GET /v1/status`

```jsonc
{
  "now": 1761062400.0, "last_probe": 1761062391.2,
  "hosts": [{
    "name": "gpu-a", "machine": "gpu-a", "aliases": ["gpu-a"],
    "online": true,                       // null = not checked yet
    "capacity_gb": 116, "services_gb": 64, "jobs_gb": 0, "free_gb": 52,
    "actual_free_gb": 49.8,               // from a hardware collector, else null
    "gpu_util": 0.03, "collector": true, "untracked_busy": [],
    "services": [{
      "name": "gpu-a-vllm", "roles": ["chat"], "kind": "fleet", "mem_gb": 64,
      "status": "up", "text": "...", "warn": false, "url": "http://gpu-a:8000/v1",
      "served": [{"id": "chat"}], "seen_at": 1761062391.2,
      "in_flight": 0, "stuck_s": null, "note": ""
    }],
    "running": [ /* job */ ],
    "queued":  [ /* job, with "position" (1 = next on this host); not reservations still to come */ ],
    "reservations": [ /* job with start_at, soonest first, due or not */ ],
    "backfill": {                         // null when nothing bounds it
      "before": 61, "label": "...", "owner": "...", "reservation": false,
      "start_in_s": 6300,                 // when #61 is expected to start (or its reserved start)
      "fits_s": 5000                      // a run job with --expect up to this long may start now
    }
  }],
  "warnings": ["..."],
  "recent": [ /* last 8 finished jobs */ ]
}
```

A host with no running job is free. A host whose `online` is `false` is down. A job that
spans several hosts shows up under each of them.

### Job

```jsonc
{
  "id": 57, "state": "queued|running|done|cancelled", "kind": "run|lease",
  "owner": "alice@laptop", "label": "try GLM 5.3", "hosts": ["gpu-b"],
  "mem_gb": 40, "mem_map": {}, "exclusive": false,
  "uses": [], "takes_down": [], "roles_taken_down": [],
  "created_at": 1761062000.0, "started_at": null, "ended_at": null, "heartbeat_at": null,
  "expect_s": 5400, "command": "", "reason": "waiting: needs 40 GB on gpu-b; 30 GB free",
  "exit_code": null, "note": "", "serves": [], "serves_caps": [],
  "watch": false,                        // lease queued by `acquire --wait`; its client heartbeats it
  "seen_at": 1761062000.0,               // last client contact: submit, heartbeat, unwatch, GET
  "load": ["gpu"], "quiet": [],          // see "Loads" below
  "notify": false,                       // true when the job has its own notify URL (never shown)
  "start_at": null, "start_in_s": null,  // reservations: booked start (server clock), and seconds until it
  "deadline": null, "deadline_in_s": null,  // run jobs started ahead of the line: must end by then
  "flags": ["stale: ...", "overdue: ...", "unclaimed: ..."]   // empty when healthy
}
```

Act on the `_in_s` fields, not the absolute times: they are relative to the server's clock
when it answered, so a client with a skewed clock still gets them right.

Flags a running job may carry: `stale` (no heartbeat), `overdue` (past `expect_s`),
`unclaimed`, `no-show: reserved slot started ...`, `past its backfill deadline`,
`overrunning into reservation #X`, and `GPU on <host> is N% busy, but no job there declares
--load gpu`. Every running job's flags are also in the status `warnings`.

### `GET /v1/models`

```jsonc
{"now": ..., "last_probe": ..., "models": [{
  "model": "chat", "url": "http://gpu-a:8000/v1",
  "service": "gpu-a-vllm", "services": ["gpu-a-vllm"],
  "host": "gpu-a", "hosts": ["gpu-a"], "machine": "gpu-a", "roles": ["chat"],
  "state": "up", "usable": true, "verified": true,
  "capabilities": ["text"], "capabilities_source": "config|job|unknown",
  "context": 32768, "root": "/models/...", "seen_at": ..., "age_s": 9,
  "job": null                            // the job id when a job brought this server up
}]}
```

Key on `(url, model)`: two boxes can serve the same model name. Without `all=1`, only usable
services are listed. The README's "Find a model to use" section covers the details.

## Write (full token)

| Route | Body | Does |
|---|---|---|
| `POST /v1/jobs` | job request (below) | queues a job and answers with it; starts it at once if it fits |
| `POST /v1/jobs/<id>/heartbeat` | none | keeps a `run` job or a watched lease from being flagged stale or dropped |
| `POST /v1/jobs/<id>/unwatch` | none | the waiting client stops on purpose: the lease stays queued without heartbeats |
| `POST /v1/jobs/<id>/release` | `{"owner", "note"?, "exit_code"?, "force"?}` | finishes a running job or cancels a queued one; only the owner may, unless `force` |
| `POST /v1/jobs/<id>/update` | `{"owner", "load"?, "quiet"?, "mem_gb"?, "mem_map"?, "force"?}` | lowers a queued or running job's claims at once; raising one answers `409` (queue a new job instead) |
| `POST /v1/when` | job request (`label`, `owner` optional; `kind` defaults to `run`) | answers `{starts_now, reason, start_in_s, deadline_in_s, hosts, now}` without queueing; `start_in_s` is an estimate, `null` when there is none |
| `POST /v1/resolve` | job request | answers `{hosts, uses, takes_down, url_service, mem_map}` without queueing |
| `POST /v1/probe` | none | runs the health checks now and answers with the status |
| `POST /v1/services/<name>/mark-up` | `{"owner", "note"?}` | confirms a service with no health check is back |

Job request (`label` and `owner` are required):

```jsonc
{
  "kind": "lease",                 // "run" = a client process heartbeats it; "lease" = acquire/release
  "label": "try GLM 5.3", "owner": "my-harness",
  "hosts": ["gpu-b"],             // hosts, aliases or machines
  "for_url": "http://gpu-b:8000/v1",  // or place the job by the URL it will call
  "mem_gb": 40, "mem_map": {"gpu-b": 40},
  "exclusive": false,
  "uses": ["review"], "takes_down": [],   // roles or service names
  "expect_min": 90,                 // flagged overdue after this; lets a run job start ahead of the line
  "watch": false,                   // leases: you heartbeat it while queued; stop and it is dropped
  "load": ["gpu"],                  // gpu, cpu, net, disk: what it loads heavily (default ["gpu"]; [] = nothing)
  "quiet": [],                      // what no other job on its hosts may load while it runs
  "start_in_s": 3600,               // reservation: start this many seconds from now ("start_at": unix time also works)
  "serves": ["http://gpu-b:8001/v1"], "serves_caps": ["text", "image"],
  "notify": "https://ntfy.sh/my-topic",  // this URL hears about this job's events (see Notifications)
  "note": "", "command": ""
}
```

## Notifications

The server posts events to the roster's `[[notify]]` URLs (README, "Notifications") and to a
job's own `notify` URL. That is `400` unless the server has `job_notify = true` and the URL's
host matches `job_notify_hosts` (default `["ntfy.sh"]`); it must also be a plain `http`/`https`
URL with a host and no spaces or control characters. The URL is write-only: job views show `"notify": true`.

A generic URL receives `POST` with `Content-Type: application/json`:

```jsonc
{"kind": "overdue", "text": "#57 expected 30m, running 31m: model swap",
 "job": 57,            // null for events about a service alone (wedged)
 "service": null,      // the service for not_restored, wedged, restored
 "at": 1761062000.0}   // server clock
```

A roster entry with `token_file` also sends `Authorization: Bearer <token>`; a job's own URL
never does. An ntfy URL (`ntfy.sh`, or `format = "ntfy"` in the roster, required for a
self-hosted server; see docs/notify.md) receives `text` as a plain-text
body with headers `Title: fleetq <kind> #<job>`, `Tags`, and `Priority: high` for `overdue`,
`stale`, `unclaimed`, `overrun`, `lapsed`, `abandoned`, `not_restored` and `wedged`.

Kinds: `started`, `done`, `overdue`, `stale`, `unclaimed`, `overrun`, `lapsed`, `abandoned`,
`not_restored`, `wedged`, `wedge-cleared`, `restored` (the default set), and `queued`, `reserved`,
`cancelled`, `updated`, `restore-pending`, `marked-up`, `deadline-cleared` on request. A job's own URL gets the default set for
that job. Delivery never affects the API: messages wait in a bounded queue (200; a full queue
drops; roster targets and per-job URLs have separate queues), each attempt cut off after 10 s and
tried three times, then dropped with a log line. Redirects are failures, and a URL that fails
repeatedly is left alone for 1, 10, then 60 minutes.

## Loads, backfill and reservations

**Collisions.** Two jobs on a shared host cannot run at once when one's `quiet` meets the
other's `load`, when either is `exclusive`, when one takes down a service the other uses or
takes down, or when their memory does not fit together. A job that `uses` a service counts
as loading the GPU (its requests do). A job with `quiet` containing `gpu` also waits for load
fleetq cannot see, as an exclusive job does: requests in flight that no job accounts for, a
wedged service, a GPU the collector reports busy.

**Backfill.** Each host keeps a first-come line. A job behind a waiting job may start now if
it does not hold that job up (no collision with it, memory left for it once the jobs expected
to be gone by then are gone), or, for a `run` job with `expect_min`, if
`now + expect * 1.2 + 5 min` is before that job's estimated start. The estimate comes from
running jobs' `started_at + expect_s`; when one in the way has no `expect_s`, or something
other than a job holds it up, there is none and only the first rule applies. A run job let
through by the second rule gets a `deadline`; `fleetq run` stops its command then (SIGTERM,
SIGKILL 30 s later). Leases and run jobs without `expect_min` never get a deadline. The server
clears the deadline (`deadline-cleared` event, `deadline_in_s` back to `null`) once nothing it
went ahead of is still waiting or booked; clients should act on the latest value.

**Reservations.** A job with `start_in_s` / `start_at` (and a length, `expect_min`) waits
until its start, then goes first in line on its hosts. A start less than 60 s ahead (or in the
past) is not a reservation: the job is queued as usual, in line. Until its start a reservation
protects its window:
a colliding run job with `expect_min` starts only if it should end in time (and gets the
start as its deadline); any other colliding job only if its `expect_min` (default 120) ends
before it. Once due, a reservation is held to its own booked window, so back-to-back bookings
each start on time. A reservation whose window overlaps a colliding reservation, or the
estimated window of a colliding job already waiting in line (its estimated start, or now when
there is none, for its `expect_s`), answers `409`:

```jsonc
{"error": "collides with reservation #61 \"bench\" (me): ...", "now": 1761062400.0,
 "conflicts": [61], "free": [{"start": 1761062400.0, "until": 1761066000.0},
                             {"start": 1761069600.0, "until": null}]}
```

A reserved lease must be claimed by its owner within `noshow_after_s` (default 900) of
starting: a heartbeat, `POST .../unwatch`, or a `GET /v1/jobs/<id>` (also with `wait`). A `GET`
with `?owner=` naming someone else, or a heartbeat whose body names someone else
(`{"owner": ...}`), does not claim it; one naming no owner (older clients) does. Unclaimed, it
lapses: it ends as `cancelled` with a `lapsed` event. It never ran anything, so services it
would have taken down are not marked for restore. Running work that
holds up a due reservation is flagged and logged (`overrun`), never stopped. The server runs a
scheduling pass at each reservation's start and each lapse time, so long-polls see them on time.

Long-poll `GET /v1/jobs/<id>?wait=25&state=queued` until `state` is `running`, do the work, then
release. Each GET (long polls included) and heartbeat counts as client contact. A lease that
starts while no client has contacted it for `unclaimed_after_s` (server config, default 600 s)
is flagged `unclaimed` and logged once; the next contact clears the flag, and fleetq never
releases it. With `"watch": true`, heartbeat at least every 180 s (`stale_heartbeat_s`) while
queued, and call `unwatch` before you stop waiting if the lease should keep its place. A harness
that only sends requests to an existing server should hold a shared lease with `uses` on
that service's role, so nobody takes it down mid-run.
