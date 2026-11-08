# fleetq for Raycast

Three Raycast commands that read the fleetq server with the read-only token:

- **Fleet Queue in Menu Bar**: running and queued job counts in the macOS menu bar, refreshed
  every minute. The icon is red when a box is offline or a service is in trouble (wedged,
  down), orange for anything else that needs a look (a held or restoring service, the server's
  warnings, such as an overdue job), and grey with "stale" when the numbers are old because the
  server did not answer or stopped checking the fleet. The menu lists what needs attention,
  the running, queued and booked jobs (yours marked), and each box. Click an item to copy it.
- **Fleet Status**: the same as a searchable list, one row per box with its job, queue,
  services that are not up, and free memory.
- **Fleet Models**: every model the fleet serves, per server URL, with its context and
  capabilities. Copy the served name, the base URL, or the `fleetq-<host>-<port>/<model>` name
  that `fleetq sync-agents` gives it in Pi, omp and OpenCode (an alias copies the name it is
  listed under).

## Set up

The commands read `~/.config/fleetq/url` (or the **Server URL** preference) and
`~/.config/fleetq/readonly_token`, the server's `/etc/fleetq/readonly_token` (chmod 600). They
never use the full token and need no `fleetq` CLI.

**Your Owner Names** picks which jobs are marked as yours: a comma-separated list where `*`
matches anything, e.g. `you@laptop, claude-*`. Empty means `user@host`, the CLI's default owner.

## Install

```bash
cd examples/raycast
npm ci
npm run dev    # imports the extension into Raycast; stop with Ctrl-C, it stays installed
```

`npm run build` and `npm run typecheck` check it without Raycast running.
