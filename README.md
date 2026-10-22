# fleetq

One queue for every job on a small fleet of GPU boxes. Before an agent, harness or person
starts work on a box, it asks fleetq for a slot. If the box is busy, the job waits in line
and starts when the jobs ahead of it are released. fleetq also health-checks the model
servers on each box, so agents can ask which models are up right now and where.

It is one stdlib-only Python package: a small HTTP server (`fleetqd`) that holds the queue
in SQLite, and a CLI (`fleetq`) that people, scripts and coding agents call. Examples below
use the names of the fleet it was built for (spark1, spark2, tatooine; roles such as
`retro` and `flight`); yours come from your roster.

## Install

Server, on any always-on Linux box or container with Python 3.11+ and systemd:

```bash
cp deploy/fleet.example.toml deploy/fleet.toml   # describe your hosts and services
deploy/install.sh root@your-server
```

The install creates the tokens (`/etc/fleetq/token`, and `/etc/fleetq/readonly_token` if the
roster names one) and keeps them and the database on re-runs.

Client, on every machine that starts work:

```bash
uv tool install git+https://github.com/jkuepker/fleetq
echo http://your-server:8470 > ~/.config/fleetq/url
# copy /etc/fleetq/token from the server to ~/.config/fleetq/token (chmod 600)
```

`FLEETQ_URL` and `FLEETQ_TOKEN` override the files. The HTTP API is in
[docs/api.md](docs/api.md). Keep your own site notes in `deploy/LOCAL.md` (gitignored).


## Check before you start anything

```bash
fleetq status
```

It shows every box's services, their memory, what is running, what is waiting and why,
plus an **Attention** list: roles that are down or serving the wrong model without any
job accounting for it, stale or overdue jobs, and services not restored after a takedown.

## Find a model to use

```bash
fleetq models                 # every model that can take requests now
fleetq models --cap image     # only models that accept images
fleetq models --all --json    # also down, held and starting ones, for a harness
```

Each entry (`GET /v1/models`, same filters as `?cap=image,text&all=1`) has `model`, `url`,
`service`, `host`/`hosts`, `machine`, `roles`, `state`, `usable`, `capabilities`,
`capabilities_source`, `context` (vLLM's `max_model_len`), `root` (the checkpoint behind a
served name) and `age_s` (seconds since the last health check, at most about 30). A
harness should key on `(url, model)`: the same name (`guidance`) can be served by two boxes.

- **Where it comes from.** Every 30 s the health check reads each service's `/models`.
  Without `--all`, only services that are usable right now are listed: a held, restoring,
  wedged or down service is left out even though its last answer is remembered.
  `verified: false` means the server needs a key (HTTP 401/403), so the models shown are the
  roster's `models`, not what it actually serves.
- **Capabilities** are `text`, `image`, `video`, `audio` (what a chat model accepts), `embed`
  and `rerank`. Servers do not report them, so the roster declares them per service
  (`capabilities = ["text", "image"]`) or per model
  (`model_capabilities = { capcom = ["text"] }`). The server refuses to start on an unknown
  word, and `fleetqd --check` reports it; `--cap` and `--caps` reject one too.
  `capabilities_source` is `config`, `job` (from `--caps`) or `unknown` (nothing declared;
  empty list, so a `--cap` filter never picks it).
- **Models a job brings up.** Give the job `--serves URL` (repeatable) and optionally
  `--caps text,image`. While the job runs, the health check reads that URL too, and its models
  appear with `job` set (`state: starting` under `--all` until the server answers). They
  disappear when the job is released. `--serves` also places the job on that URL's box, like
  `--for-url`. Inside a covering `FLEETQ_JOB`, `--serves` is not recorded; give it to the parent.
  The server must listen where the fleetq server can reach it: a loopback URL is rewritten to
  this machine's name, but a server bound to 127.0.0.1 never answers the check and stays
  `starting`.

```bash
fleetq acquire --host spark2 --mem 40 --serves http://spark-fa00:8001/v1 --caps text,image \
  --label "try GLM 5.3V" --expect 90 --wait
```

## Run work

A benchmark or any single command: wait for a slot, run, release on exit (even on a crash).

```bash
fleetq run --host r9700 --exclusive --label "Qwen3.8-27B speed bench" -- uv run localeval speed ...
fleetq run --uses retro --exclusive --label "RETRO decode test" -- ./bench.sh
```

Multi-step work across several tool calls: acquire, work, release.

```bash
fleetq acquire --host spark2 --mem 40 --label "try GLM 5.3" --expect 90 --wait
# ... work ...
fleetq release 57
```

Take a fleet role down to test something else. Anyone who needs the role waits until it is
back; fleetq only counts it back once a health check sees the expected model again.

```bash
fleetq acquire --takes-down retro --mem 90 --label "swap spark2 to GLM 5.3" --wait
# stop the server, test, restart the normal server
fleetq release 58
```

### Several hosts in one job

Work that spans boxes, such as tensor parallelism over both Sparks, is **one** job: it waits
until every host is free, holds them all together, and releases them together. Two jobs
(one per rank) can leave one box held and idle while the other waits.

```bash
fleetq run --host spark1,spark2 --takes-down flight,retro --mem spark1=100,spark2=95 --each \
  --label "GLM-5.3 TP2 bench" -- ssh {machine} ~/tp2/rank.sh {rank} {nhosts}
```

- `--mem` takes one figure for every host, or `host=GB` pairs (or both: `--mem 20,spark1=100`).
- `--each` runs the command once per host, all at once, with `{host}` (fleetq name),
  `{machine}` (ssh name), `{rank}` (0-based, in `--host` order) and `{nhosts}` filled in, and
  `FLEETQ_HOST`/`FLEETQ_MACHINE`/`FLEETQ_RANK`/`FLEETQ_NHOSTS` set. Output lines are tagged
  `[host]` (`--no-prefix` turns that off).
- If one host's command fails, the others are stopped (SIGTERM, then SIGKILL after 30 s),
  since ranks rarely finish without each other. `--keep-going` lets them finish instead. The
  exit code is the first failure's.
- Without `--each`, one command runs once while all the hosts are held (e.g. a launcher that
  starts every rank itself). `fleetq status --host spark1,spark2` shows just those hosts.

A box with several GPUs gets one host (memory pool) per card. A server that spans the cards
(tensor parallelism) is one service per rank, all sharing a role, so `--takes-down <role>`
holds every card it uses. Pin one-card work with `HIP_VISIBLE_DEVICES` /
`CUDA_VISIBLE_DEVICES`.

Use a role for a long-running project, so nobody takes it down or benchmarks on it
underneath you:

```bash
fleetq acquire --uses flight --label "coding loop" --expect 480
```


Work inside a slot you hold: `fleetq run` gives its command `FLEETQ_JOB=<id>`, and any
`fleetq run` (or localeval) started with `FLEETQ_JOB` set to a running job that already
holds the box runs inside it instead of queueing behind it. For leases, prefix each
command yourself: `FLEETQ_JOB=58 uv run localeval speed ...`. `fleetq inside --host X`
exits 0 when `FLEETQ_JOB` covers X.

| Option | Meaning |
|---|---|
| `--host` | host, alias or machine (`spark1`, `spark-4e77`, `r9700`, `tatooine` = both tatooine GPUs) |
| `--for-url URL` | the box serving this URL: a known service's port, else the machine's default pool (loopback = this machine); URLs off the fleet run without a slot |
| `--mem GB` | memory this job adds on each host (default 0: work against an existing server) |
| `--exclusive` | nothing else may run on the host meanwhile, and no untracked requests may be in flight |
| `--uses` | roles/services that must be up; shared with other users |
| `--takes-down` | roles/services this job stops; must come back afterwards |
| `--label` | what it is; everyone waiting sees it |
| `--serves URL` | a model server this job brings up; `fleetq models` lists it while the job runs |
| `--caps` | what the models at `--serves` can do (`text`, `image`, `video`, `audio`, `embed`, `rerank`) |

Exit codes: `run` returns the command's code; 2 = server unreachable (nothing started;
`--no-queue` runs anyway); 3 = gave up waiting (`--wait-timeout`); 4 = the job was released
or cancelled before it started (`fleetq release` on a queued job, or the server dropped it), so
the command never ran. `acquire --wait` also returns 4 then.

## Rules

1. **Strict order per host.** Once the oldest waiting job on a host can't start, nothing
   behind it on that host starts, so a big job is never starved by small ones.
2. **Memory must fit** in the declared budget (capacity minus services minus running
   jobs), and, where a hardware collector is connected, in the memory the box actually
   reports free.
3. **Only the owner releases** a job (`--force` overrides, and the log records it).
   fleetq never releases running work by itself. A `run` job whose client stops
   heartbeating is flagged stale; a lease past `--expect` is flagged overdue. A lease that
   started while no client had looked at it (heartbeat, `fleetq show`/`wait`, or a
   `FLEETQ_JOB` command inside it) for `unclaimed_after_s` (default 600 s) is flagged
   unclaimed, and the log records it once; the first client that looks at it clears the flag.
4. **Waiters that vanish leave the line.** A queued `run` job, or a lease queued by
   `acquire --wait`, whose client stops heartbeating for `stale_heartbeat_s` (default 180 s)
   is dropped, so a killed agent tool call does not later hold a box nobody uses. Ctrl-C or
   `--wait-timeout` on `acquire --wait` keeps the lease in line, as does `acquire` without
   `--wait` (it never heartbeats).
5. **Wedged servers.** A service that reports requests in flight while its engine step
   counter (vLLM `iteration_tokens_total_count`, llama.cpp `n_decode_total`) stays frozen
   for `wedge_after_s` (default 180 s) shows as `WEDGED`. vLLM on GB10 freezes like this
   mid-request; the client times out and the server holds one "running" request forever.
   Its stuck requests no longer count as untracked work, and a job that takes it down
   (to restart it) goes to the front of that host's line. fleetq does not restart it itself.

Other commands: `fleetq show ID`, `fleetq wait ID`, `fleetq log`, `fleetq roster`,
`fleetq probe` (check now), `fleetq mark-up SERVICE` (confirm a service without a health
check is back).

## Integrations

- **Benchmark tools** re-launch themselves under `fleetq run --for-url <target> --exclusive`
  unless `FLEETQ_JOB` already covers the box, so a benchmark never shares a GPU by accident.
- **Coding agents**: a pre-tool hook (e.g. Claude Code's `PreToolUse`) can deny shell
  commands that start, stop or load work on a fleet box unless they run via `fleetq run` or
  with a covering `FLEETQ_JOB`. Give each agent session its own owner name.
- **Agent harnesses** pick a model with `GET /v1/models` and hold a shared lease (`--uses`)
  on the service they send requests to, so nobody takes it down mid-run.
- **Dashboards and status bars** read `GET /v1/status` with the read-only token.

## Change the roster

Edit `deploy/fleet.toml` (start from `deploy/fleet.example.toml`), then run
`deploy/install.sh root@your-server`. The install keeps both tokens and the database.
`fleetqd --config deploy/fleet.toml --check` validates a roster without starting anything.

## Tokens

`token_file` (`/etc/fleetq/token`) grants everything: submit, release, probe, mark-up.
`readonly_token_file` (`/etc/fleetq/readonly_token`, optional) grants only `GET /v1/status`,
`/v1/models`, `/v1/roster` and `/v1/events`; any other request with it answers
403 `{"error": ...}`. Give the read-only one to dashboards and status bars that never queue
work. Both files must be at least 20 characters and readable only by their owner (600), or
the server refuses to start. The install generates either one if it is missing; leave
`readonly_token_file` out of `[server]` to turn the scope off.

## Development

```bash
uv sync --group dev && uv run pytest
cd e2e && npm ci && npm test      # CLI/HTTP end-to-end scenarios against a local server
```

## License

MIT
