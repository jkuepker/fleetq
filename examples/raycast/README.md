# fleetq for Raycast

Two Raycast commands that read `fleetq status --json`:

- **Fleet Status**: every box, its running job and its queue.
- **Fleet Queue in Menu Bar**: running and queued job counts in the macOS menu bar, refreshed
  every minute. Click a job to copy its ID.

They need the `fleetq` CLI set up on this Mac (server URL and token, see the main README).
Raycast does not load your shell `PATH`, so the extension's **fleetq Path** preference
defaults to `~/.local/bin/fleetq`; change it if `which fleetq` says otherwise.

## Install

```bash
cd examples/raycast
npm ci
npm run dev    # imports the extension into Raycast; stop with Ctrl-C, it stays installed
```

`npm run build` and `npm run typecheck` check it without Raycast running.
