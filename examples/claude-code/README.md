# Claude Code guard hook

A [Claude Code hook](https://docs.claude.com/en/docs/claude-code/hooks) that keeps an agent
from starting, stopping or loading work on a fleetq box without going through the queue. It is
one stdlib-only Python file (3.9+), `fleetq-guard.py`, with two modes:

- **PreToolUse (Bash)**: denies a command that changes what a box is running unless it runs
  through `fleetq run` or inside a held slot, and tells the agent how to do it properly.
- **SessionStart**: writes `export FLEETQ_OWNER=claude-<first 8 characters of the session id>`
  to `$CLAUDE_ENV_FILE`, so every Claude session is its own fleetq owner and one chat cannot
  release another chat's job without `--force`.

Any internal error allows the command. The guard never breaks ordinary Bash use.

## Which boxes

The boxes come from your fleetq server, not from the script: it runs `fleetq roster --json`
and treats every machine in the roster as a box. A command names a box by the machine name, any
pool name or alias, or any address. The roster is cached in `~/.cache/fleetq/roster.json` for
10 minutes. If a refresh fails (server down, `fleetq` missing) the stale cache is used, and
with no roster at all the guard allows everything. Delete the cache file to pick up a roster
change at once.

## What it blocks

Over a transport (`ssh`, `DOCKER_HOST=`, `docker -H/--host/--context`) to a roster box:

- `docker run|start|stop|restart|rm|kill|update`, `docker compose up|down|...`
- `systemctl start|stop|restart|reload|kill`, `enable|disable --now`
- `vllm serve`, `llama-server`, `llama-bench`, `llama-cli`, `python -m sglang.launch_server`,
  `python -m mlx_lm.server`
- `pkill`, `killall`, `kill <pid>`, `reboot`, `shutdown`, `poweroff`
- `nvidia-smi` resets and power or clock limits

Without any transport:

- a benchmark script (`bench*.py` or `bench*.sh`, also via `python`, `bash`, `uv run`) or
  `llama-bench` in a command that names a box
- a shell `for`/`while` loop that fires `curl` at `/v1/completions` or `/v1/chat/completions`
  of a box

On the machine the hook runs on, when that machine is itself in the roster (the hostname is
matched against machine names): `launchctl`, `brew services` or `systemctl` acting on a
vllm, llama, mlx, sglang or ollama service, and starting `llama-server`, `vllm serve` or
`mlx_lm.server` directly. These are reported under that machine.

## What it allows

- Everything else: `docker ps`, `docker logs`, `nvidia-smi` queries, `cat`, `ls`, a single
  `curl`, `ls .../llama-server`, `grep x llama-server.log`, and so on. A verb only counts where
  a command starts.
- Version-control commands (`git`, `gh`), whose commit messages and PR text mention hosts and
  verbs.
- `ssh` to a host that is not in the roster is not checked as local model-server control.
- A command wrapped in `fleetq run ... --` before the matching verb.
- A command with `FLEETQ_JOB=<id>` when `fleetq inside --host <machine>` exits 0, or, if
  that fails, `fleetq inside --host <pool>` for any one of the machine's pools.
- A non-numeric `FLEETQ_JOB` only when the same command also runs `fleetq acquire`.

## Install

1. Copy the script and make it executable:

   ```sh
   mkdir -p ~/.claude/hooks
   cp examples/claude-code/fleetq-guard.py ~/.claude/hooks/
   chmod +x ~/.claude/hooks/fleetq-guard.py
   ```

2. Register it in `~/.claude/settings.json` (or a project's `.claude/settings.json`):

   ```json
   {
     "hooks": {
       "PreToolUse": [
         {
           "matcher": "Bash",
           "hooks": [{ "type": "command", "command": "~/.claude/hooks/fleetq-guard.py" }]
         }
       ],
       "SessionStart": [
         {
           "hooks": [{ "type": "command", "command": "~/.claude/hooks/fleetq-guard.py session-start" }]
         }
       ]
     }
   }
   ```

3. `fleetq` must be on the hook's `PATH` (or at `~/.local/bin/fleetq`) and able to reach the
   server (`FLEETQ_URL`, token), because the hook calls `fleetq roster --json` and
   `fleetq inside`. Check with `fleetq roster`.

Try it: `echo '{"tool_input": {"command": "ssh <box> docker restart x"}}' | ./fleetq-guard.py`
prints a deny decision for a box in your roster, and nothing for `docker ps`.

## Known limits

This is a seat belt, not a sandbox.

- It reads the command text with regular expressions. It does not parse shell, so a command
  built from variables, a script that does the work, an unquoted `ssh box vllm serve ...`
  (the verb is only recognised at a command start, so quote the remote command) or a verb hidden
  in `eval` or a heredoc is not seen.
- Only the Bash tool is checked. Other tools, MCP servers and scripts the agent writes are not.
- An agent that wants to get around it can: use another transport, spell the host differently,
  or set `FLEETQ_JOB` to a job that covers the box but is not its own. The guard checks that the
  job covers the box, not who holds it.
- A box is recognised only by the names and addresses in the roster.

Tests: `uv run pytest tests/test_claude_code_hook.py` (no network, no real fleetq).
