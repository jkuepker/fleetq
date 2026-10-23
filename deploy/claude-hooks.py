#!/usr/bin/env python3
"""Claude Code hooks that keep a session from sitting on fleetq leases it no longer needs.

  Stop        at the end of a turn, if the session still holds leases, block the stop once and ask
              Claude to release what it is done with, or say in one line why it keeps it.
  SessionEnd  release the session's leases, except exclusive or takes-down ones: freeing a box
              mid-reboot or a service mid-swap is worse than waiting. Those keep their slot; once the
              session's keepalive stops, the server flags them stale and notifies.

The session's owner name is FLEETQ_OWNER (set it per session, e.g. from a SessionStart hook writing
`export FLEETQ_OWNER=claude-<session id>` to CLAUDE_ENV_FILE), else claude-<first 8 characters of
the session id>. Run jobs (`fleetq run`) are left alone: they end with their command.

In ~/.claude/settings.json (adjust the path):

  "hooks": {
    "Stop": [{"hooks": [{"type": "command", "command": "/path/to/claude-hooks.py stop"}]}],
    "SessionEnd": [{"hooks": [{"type": "command", "command": "/path/to/claude-hooks.py session-end"}]}]
  }

Any error lets the session go on: a hook must never get in the way of ordinary work.
Needs only the Python standard library and the fleetq CLI on PATH (or FLEETQ_BIN).
"""
import json
import os
import subprocess
import sys
import time

FLEETQ = os.environ.get('FLEETQ_BIN', 'fleetq')


def owner(payload):
    sid = str(payload.get('session_id') or '')
    return os.environ.get('FLEETQ_OWNER') or (f'claude-{sid[:8]}' if sid else '')


def held(who):
    """The leases `who` holds, running or queued, one entry per job."""
    out = subprocess.run([FLEETQ, 'status', '--json'], capture_output=True, text=True, timeout=20)
    if out.returncode:
        return []
    jobs = {}
    for host in json.loads(out.stdout).get('hosts', []):
        for job in host.get('running', []) + host.get('queued', []):
            if job.get('owner') == who and job.get('kind') == 'lease':
                jobs[job['id']] = job
    return [jobs[i] for i in sorted(jobs)]


def minutes(job, now):
    since = job.get('started_at') or job.get('created_at') or now
    return max(0, round((now - since) / 60))


def stop(payload):
    if payload.get('stop_hook_active'):
        return  # already reminded this turn
    who = owner(payload)
    jobs = held(who) if who else []
    if not jobs:
        return
    now = time.time()
    lines = [f'#{j["id"]} "{j["label"]}" ({j["state"]} {minutes(j, now)}m)' for j in jobs]
    print(json.dumps({'decision': 'block', 'reason': (
        f'You still hold fleetq lease(s) as {who}: ' + '; '.join(lines) + '. Others may be waiting for '
        'them. Release each one you are done with (fleetq release <id>). If you still need one, say in '
        'one line why, then stop.')}))


def session_end(payload):
    who = owner(payload)
    for job in held(who) if who else []:
        if job.get('exclusive') or job.get('takes_down'):
            continue
        subprocess.run([FLEETQ, '--owner', who, 'release', str(job['id']), '--note',
                        f'session ended ({payload.get("reason") or "exit"})'],
                       capture_output=True, timeout=20)


def main():
    try:
        payload = json.load(sys.stdin)
        {'stop': stop, 'session-end': session_end}[sys.argv[1]](payload)
    except Exception as exc:  # never block the session on a hook failure
        sys.stderr.write(f'fleetq claude-hooks: {exc!r}\n')
    return 0


if __name__ == '__main__':
    sys.exit(main())
