# fleetq HTTP API

The CLI is a thin client over this API; harnesses, dashboards and agents can call it
directly. Everything is JSON over HTTP on the server's port (default 8470).

## Auth

Every route except `/healthz` needs `Authorization: Bearer <token>`.

| Token | Allows |
|---|---|
| full (`token_file`) | every route |
| read-only (`readonly_token_file`, optional) | `GET /v1/status`, `/v1/models`, `/v1/roster`, `/v1/events` |

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
| `GET /v1/jobs/<id>` | one job; queued jobs also carry `position` |

### `GET /v1/status`

```jsonc
{
  "now": 1761062400.0, "last_probe": 1761062391.2,
  "hosts": [{
    "name": "spark1", "machine": "gpu-a", "aliases": ["gpu-a"],
    "online": true,                       // null = not checked yet
    "capacity_gb": 116, "services_gb": 64, "jobs_gb": 0, "free_gb": 52,
    "actual_free_gb": 49.8,               // from a hardware collector, else null
    "gpu_util": 0.03, "collector": true, "untracked_busy": [],
    "services": [{
      "name": "spark1-vllm", "roles": ["chat"], "kind": "fleet", "mem_gb": 64,
      "status": "up", "text": "...", "warn": false, "url": "http://gpu-a:8000/v1",
      "served": [{"id": "chat"}], "seen_at": 1761062391.2,
      "in_flight": 0, "stuck_s": null, "note": ""
    }],
    "running": [ /* job */ ],
    "queued":  [ /* job, with "position" (1 = next on this host) */ ]
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
  "owner": "claude-1a2b", "label": "try GLM 5.3", "hosts": ["spark2"],
  "mem_gb": 40, "mem_map": {}, "exclusive": false,
  "uses": [], "takes_down": [], "roles_taken_down": [],
  "created_at": 1761062000.0, "started_at": null, "ended_at": null, "heartbeat_at": null,
  "expect_s": 5400, "command": "", "reason": "waiting: needs 40 GB on spark2; 30 GB free",
  "exit_code": null, "note": "", "serves": [], "serves_caps": [],
  "flags": ["stale", "overdue"]          // empty when healthy
}
```

### `GET /v1/models`

```jsonc
{"now": ..., "last_probe": ..., "models": [{
  "model": "chat", "url": "http://gpu-a:8000/v1",
  "service": "spark1-vllm", "services": ["spark1-vllm"],
  "host": "spark1", "hosts": ["spark1"], "machine": "gpu-a", "roles": ["chat"],
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
| `POST /v1/jobs/<id>/heartbeat` | none | keeps a `run` job from being flagged stale or dropped |
| `POST /v1/jobs/<id>/release` | `{"owner", "note"?, "exit_code"?, "force"?}` | finishes a running job or cancels a queued one; only the owner may, unless `force` |
| `POST /v1/resolve` | job request | answers `{hosts, uses, takes_down, url_service, mem_map}` without queueing |
| `POST /v1/probe` | none | runs the health checks now and answers with the status |
| `POST /v1/services/<name>/mark-up` | `{"owner", "note"?}` | confirms a service with no health check is back |

Job request (`label` and `owner` are required):

```jsonc
{
  "kind": "lease",                 // "run" = a client process heartbeats it; "lease" = acquire/release
  "label": "try GLM 5.3", "owner": "my-harness",
  "hosts": ["spark2"],             // hosts, aliases or machines
  "for_url": "http://gpu-b:8000/v1",  // or place the job by the URL it will call
  "mem_gb": 40, "mem_map": {"spark2": 40},
  "exclusive": false,
  "uses": ["review"], "takes_down": [],   // roles or service names
  "expect_min": 90,                 // leases are flagged overdue after this
  "serves": ["http://gpu-b:8001/v1"], "serves_caps": ["text", "image"],
  "note": "", "command": ""
}
```

Poll `GET /v1/jobs/<id>` until `state` is `running`, do the work, then release. A harness
that only sends requests to an existing server should hold a shared lease with `uses` on
that service's role, so nobody takes it down mid-run.
