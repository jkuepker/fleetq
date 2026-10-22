#!/usr/bin/env python3
"""Claude Code hook: keep heavy fleet work behind the fleetq queue.

PreToolUse (Bash): deny commands that start, stop or load model servers, or run benchmark
loops, on a box in your fleetq roster unless they run through `fleetq run`, or inside a held
slot (FLEETQ_JOB=<id> that fleetq confirms covers the box). Read-only commands (docker ps/logs,
nvidia-smi queries, curl, cat, ...) pass. Commands sent over ssh to a host outside the roster
are not checked as local model-server control.

SessionStart: give each Claude session its own fleetq owner name via CLAUDE_ENV_FILE, so
one chat cannot release another chat's job without --force.

The boxes come from fleetq itself (`fleetq roster --json`), cached for 10 minutes in
~/.cache/fleetq/roster.json. With no roster at all the guard allows everything.

Any internal error allows the command: this guard must never break ordinary Bash use.
Python 3.9 compatible, standard library only.
"""
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time

CACHE = os.path.expanduser('~/.cache/fleetq/roster.json')
CACHE_TTL = 600  # seconds a cached roster counts as fresh
ROSTER_TIMEOUT = 5  # seconds `fleetq roster --json` may take


def fleetq_bin():
    return shutil.which('fleetq') or os.path.expanduser('~/.local/bin/fleetq')


# ---------------------------------------------------------------------------------------
# The roster: which names and addresses are fleet boxes.

def load_roster(run=subprocess.run, now=time.time):
    """The roster's host list: the cache while it is fresh, else `fleetq roster --json`
    (written to the cache), else a stale cache, else []. A failed refresh renews the cache
    file's age, so a dead server costs one slow call per TTL, not one per command."""
    cached = None
    try:
        with open(CACHE) as f:
            cached = json.load(f)['hosts']
        if now() - os.path.getmtime(CACHE) < CACHE_TTL:
            return cached
    except (OSError, ValueError, KeyError, TypeError):
        cached = None
    hosts = None
    try:
        r = run([fleetq_bin(), 'roster', '--json'], capture_output=True, text=True, timeout=ROSTER_TIMEOUT)
        if r.returncode == 0:
            hosts = json.loads(r.stdout)['hosts']
            if not isinstance(hosts, list):
                hosts = None
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError):
        hosts = None
    fresh = hosts is not None
    if not fresh:
        hosts = cached or []
    try:
        os.makedirs(os.path.dirname(CACHE), exist_ok=True)
        tmp = CACHE + f'.{os.getpid()}.tmp'
        with open(tmp, 'w') as f:
            json.dump({'hosts': hosts}, f)
        os.replace(tmp, CACHE)
    except OSError:
        pass
    return hosts


def boxes_from_roster(hosts):
    """[(machine, pools, regex)] one per machine. The regex matches the machine name, any of
    its pool names or aliases, or any of its addresses, as a word of its own."""
    groups = {}
    for h in hosts:
        machine = h.get('machine') or h.get('name')
        if not machine:
            continue
        g = groups.setdefault(machine, {'pools': [], 'words': [machine]})
        if h.get('name') and h['name'] not in g['pools']:
            g['pools'].append(h['name'])
        g['words'] += [w for w in [h.get('name')] + list(h.get('aliases') or [])
                       + list(h.get('addresses') or []) if w]
    boxes = []
    for machine, g in groups.items():
        words = sorted(set(g['words']), key=lambda w: (-len(w), w))
        rx = re.compile(r'(?<![\w.-])(?:' + '|'.join(re.escape(w) for w in words) + r')(?![\w-])', re.I)
        boxes.append((machine, g['pools'], rx))
    return boxes


def local_machine(boxes):
    """The roster machine this process runs on (by hostname, short name too), else None."""
    try:
        full = socket.gethostname().lower()
    except OSError:
        return None
    short = full.split('.')[0]
    for machine, _, _ in boxes:
        name = machine.lower()
        if name in (full, short) or name.split('.')[0] == short:
            return machine
    return None


def box_of(cmd, boxes):
    for machine, _, rx in boxes:
        if rx.search(cmd):
            return machine
    return None


# ---------------------------------------------------------------------------------------
# What counts as changing a box.

# A word counts as a command only where a command starts: line or segment start, after an
# opening quote, $( or a wrapper like sudo/nohup/exec. So `ls .../llama-server` or `last reboot` pass.
# An opening quote follows whitespace, '=', '(' or a backslash (ssh h 'cmd', -c "cmd", \"cmd\"); a
# closing one follows its own text, so in `grep 'x' llama-server.log` the file name is no command.
OPEN_QUOTE = r'(?<![^\s=(\\])[\'"]'
CMD = (r'(?:^|[;&|(\n]|\$\(|' + OPEN_QUOTE + r'|\b(?:exec|nohup|sudo|setsid|time|timeout\s+\S+)\s)\s*'
       r'(?:env(?:\s+\w+=\S+)+\s+)?(?:[\w.~/-]*/)?')
# Commands that change what a box is running.
HEAVY_REMOTE = re.compile(
    r'\bdocker\s+(?:(?:-H|--host|--context)[\s=]\S+\s+)*'
    r'(?:run|start|stop|restart|rm|kill|update|'
    r'compose\s+(?:up|down|restart|stop|start|rm|kill)|container\s+(?:run|start|stop|restart|rm|kill))\b'
    r'|\bsystemctl\s+(?:--user\s+)?(?:start|stop|restart|reload|kill|enable\s+--now|disable\s+--now)\b'
    r'|' + CMD + r'(?:vllm\s+serve|llama-(?:server|bench|batched-bench|cli)|'
    r'python3?\s+-m\s+(?:sglang\.launch_server|mlx_lm\.server)|pkill|killall|kill\s+(?:-\w+\s+)*\d+|'
    r'reboot|shutdown|poweroff)\b'
    r'|\bnvidia-smi\s+(?:-r\b|--gpu-reset|-pl\b|--power-limit|-lgc\b|-rgc\b)', re.M)
# Remote verbs only matter when they travel to the box.
TRANSPORT = re.compile(r'\bssh\b|\bDOCKER_HOST=|\bdocker\s+(?:-H|--host|--context)\b')
# Benchmarks aimed at a box need no ssh: the load arrives over HTTP. Only a bench script being
# run counts (at a command start, maybe via python/bash/uv run), not one being copied, edited or
# grepped: `cp bench.py x`, `sed ... bench.py` or a heredoc that rewrites it pass. After a quote
# (ssh 'python3 bench.py') the interpreter is required, so a quoted path in code is not a run.
START = (r'(?:^|[;&|(\n]|\$\(|\b(?:exec|nohup|sudo|setsid|time|timeout\s+\S+)\s)\s*'
         r'(?:env\s+)?(?:\w+=\S+\s+)*')
INTERP = (r'(?:python3?(?:\.\d+)?|bash|sh|zsh|uvx|uv\s+run(?:\s+--?[\w-]+(?:[ =][^\s-]\S*)?)*)'
          r'\s+(?:-[\w-]+\s+)*')
SCRIPT = r'[\w.~/-]*bench[\w.-]*\.(?:sh|py)\b'
BENCH_SCRIPT = re.compile(START + r'(?:' + INTERP + r')?' + SCRIPT
                          + r'|[\'"]\s*(?:\w+=\S+\s+)*' + INTERP + SCRIPT
                          + r'|\bllama-(?:bench|batched-bench)\b', re.M)
# On the machine the hook runs on, when it is in the roster: model-server control counts
# without any ssh (service managers acting on a model server, or starting one directly).
HEAVY_LOCAL = re.compile(
    r'\b(?:launchctl\s+(?:load|unload|kickstart|bootstrap|bootout|start|stop)'
    r'|brew\s+services\s+(?:start|stop|restart|run)'
    r'|systemctl\s+(?:--user\s+)?(?:start|stop|restart|reload|kill))\b'
    r'[^\n;&|]*(?:vllm|llama|mlx|sglang|ollama)'
    r'|' + CMD + r'(?:llama-(?:server|bench|batched-bench)|vllm\s+serve|'
    r'python3?\s+-m\s+(?:mlx_lm\.server|sglang\.launch_server))\b', re.M)
# A shell loop firing completions at a model endpoint is an ad-hoc benchmark.
LOOP_BENCH = re.compile(r'\b(?:for|while)\b[\s\S]*\bcurl\b[\s\S]*/v1/(?:chat/)?completions')

HOW = ('Wrap it so it waits its turn and frees the box afterwards:\n'
       '  fleetq run --host {host} [--mem GB] [--takes-down ROLE] --label "what this is" -- <command>\n'
       'For several steps, hold a slot and run each command inside it:\n'
       '  fleetq acquire --host {host} --expect MIN [--mem GB] [--takes-down ROLE] --label "..." --wait\n'
       '  FLEETQ_JOB=<id> <command>        (then: fleetq release <id>)\n'
       '--exclusive only when the box must be empty (reboot, driver, swapping a model server).\n'
       'Check first: fleetq status --host {host}\n'
       'If the fleetq server is down and the user accepts the risk: fleetq run --no-queue ... -- <command>.')


# ---------------------------------------------------------------------------------------
# ssh to a host outside the roster is not local control.

# ssh options that take an argument (`ssh -p 22 host`), so the host is the next free word.
SSH_ARG_OPTS = set('BbcDEeFIiJLlmOoPpQRSWw')
SSH_AT = re.compile(r'(?:^|(?<=[\s;&|(\'"]))ssh(?=\s)')


def _ssh_spans(cmd, boxes):
    """(start, end) of each `ssh <host> ...` invocation whose host is not a roster box: from
    the host word to the end of the shell command (an unquoted ; & | newline or `)`), quoted
    remote commands included. HEAVY_LOCAL guards this machine, not hosts reached over ssh."""
    spans = []
    for m in SSH_AT.finditer(cmd):
        words = []  # (start, end, text) of each word until the command ends
        i, n = m.end(), len(cmd)
        while i < n:
            while i < n and cmd[i] in ' \t\\':
                i += 1
            if i >= n or cmd[i] in ';&|\n)':
                break
            start, depth = i, 0  # depth: open $( or ( inside the word, as in gui/$(id -u)
            while i < n and (depth or cmd[i] not in ' \t;&|\n)'):
                if cmd[i] == '(':
                    depth += 1
                    i += 1
                elif cmd[i] == ')':
                    depth -= 1
                    i += 1
                elif cmd[i] == '\n':
                    break
                elif cmd[i] in '\'"':
                    q = cmd[i]
                    close = cmd.find(q, i + 1)
                    if close < 0:
                        return spans  # unbalanced quote: mask nothing more
                    i = close + 1
                elif cmd[i] == '\\':
                    i += 2
                else:
                    i += 1
            words.append((start, i, cmd[start:i]))
        k = 0
        while k < len(words) and words[k][2].startswith('-') and words[k][2] != '--':
            w = words[k][2]
            # -p 22 takes the next word; -p22 or -oKey=val carry it inline.
            k += 2 if len(w) == 2 and w[1] in SSH_ARG_OPTS else 1
        if k < len(words) and words[k][2] == '--':
            k += 1
        if k >= len(words):
            continue
        host = words[k][2].strip('\'"').split('@')[-1]
        start, end = words[k][0], words[-1][1]
        # Never hide a roster box: the target itself, or one named in the remote command
        # (ssh other 'ssh gpu-a launchctl ...').
        if box_of(host, boxes) or box_of(cmd[start:end], boxes):
            continue
        spans.append((start, end))
    return spans


def local_part(cmd, boxes):
    """cmd with the remote parts of ssh invocations to non-roster hosts blanked out."""
    for start, end in reversed(_ssh_spans(cmd, boxes)):
        cmd = cmd[:start] + ' ' * (end - start) + cmd[end:]
    return cmd


# ---------------------------------------------------------------------------------------
# The decision.

def heavy_match(cmd, boxes, local=None):
    """(machine, matched text) for a command that changes fleet state, else None. `local` is
    the roster machine this process runs on, if any."""
    if re.match(r'\s*(?:git|gh)\b', cmd):
        return None  # commit messages and PR text mention hosts and verbs; git never runs fleet work
    machine = box_of(cmd, boxes)
    if machine:
        m = ((TRANSPORT.search(cmd) and HEAVY_REMOTE.search(cmd))
             or BENCH_SCRIPT.search(cmd) or LOOP_BENCH.search(cmd))
        if m:
            return machine, m
    if local:
        # Same length as cmd, so m.start() still lines up for decide()'s fleetq-run check.
        m = HEAVY_LOCAL.search(local_part(cmd, boxes))
        if m:
            return local, m
    return None


def pools_of(machine, boxes):
    for name, pools, _ in boxes:
        if name == machine:
            return pools
    return []


def decide(cmd, boxes, local=None, run=subprocess.run):
    """None to allow, else the reason to deny."""
    hit = heavy_match(cmd, boxes, local)
    if not hit:
        return None
    host, m = hit
    wrapper = re.search(r'\bfleetq\s+run\b', cmd)
    if wrapper and wrapper.start() < m.start():
        return None
    job = re.search(r'\bFLEETQ_JOB=(\S+)', cmd)
    if job:
        value = job.group(1).strip('"\'')
        if not value.isdigit():
            if re.search(r'\bfleetq\s+acquire\b', cmd):
                return None  # slot taken in this same command; fleetq enforces the rest
            return f'FLEETQ_JOB={value} is not a job number fleetq can check.\n' + HOW.format(host=host)
        # A slot on the machine, or on any one of its pools, covers work on it.
        first = None
        for h in [host] + [p for p in pools_of(host, boxes) if p != host]:
            try:
                r = run([fleetq_bin(), 'inside', '--host', h], env={**os.environ, 'FLEETQ_JOB': value},
                        capture_output=True, text=True, timeout=8)
            except (OSError, subprocess.SubprocessError) as exc:
                return f'could not check FLEETQ_JOB={value} with fleetq ({exc}).\n' + HOW.format(host=host)
            if r.returncode == 0:
                return None
            first = first or r.stderr.strip()
        return f'FLEETQ_JOB={value} does not cover {host}: {first}\n' + HOW.format(host=host)
    return (f'This changes what {host} is running ("{m.group(0).strip()[:60]}"), and fleet work must go '
            f'through the fleetq queue so it cannot collide with a benchmark or role another chat '
            f'is running.\n' + HOW.format(host=host))


def load_boxes():
    boxes = boxes_from_roster(load_roster())
    return boxes, local_machine(boxes)


def pre_tool_use(payload):
    cmd = (payload.get('tool_input') or {}).get('command') or ''
    boxes, local = load_boxes()
    if not boxes:
        return  # no roster: nothing to guard
    reason = decide(cmd, boxes, local)
    if reason:
        print(json.dumps({'hookSpecificOutput': {
            'hookEventName': 'PreToolUse', 'permissionDecision': 'deny',
            'permissionDecisionReason': 'fleetq guard: ' + reason}}))


def session_start(payload):
    env_file = os.environ.get('CLAUDE_ENV_FILE')
    sid = re.sub(r'[^A-Za-z0-9]', '', payload.get('session_id') or '')[:8]
    if env_file and sid:
        with open(env_file, 'a') as f:
            f.write(f'export FLEETQ_OWNER=claude-{sid}\n')


def main():
    try:
        payload = json.load(sys.stdin)
        if sys.argv[1:] == ['session-start']:
            session_start(payload)
        else:
            pre_tool_use(payload)
    except Exception:  # never break Bash because the guard failed
        pass
    return 0


if __name__ == '__main__':
    sys.exit(main())
