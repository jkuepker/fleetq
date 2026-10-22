"""The Claude Code guard hook (examples/claude-code/fleetq-guard.py): no network, no real fleetq.
The roster is injected, and `fleetq inside` is a fake that covers only the pools named in COVERED."""
import importlib.util
import io
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import pytest

_PATH = Path(__file__).resolve().parent.parent / 'examples' / 'claude-code' / 'fleetq-guard.py'
_spec = importlib.util.spec_from_file_location('fleetq_guard', _PATH)
guard = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(guard)

# gpu-a: one pool. gpu-b: two pools (one per card). workstation: where the hook "runs".
HOSTS = [
    {'name': 'gpu-a', 'machine': 'gpu-a', 'aliases': ['alpha'], 'addresses': ['192.0.2.11', '100.64.0.11']},
    {'name': 'gpu-b-0', 'machine': 'gpu-b', 'aliases': [], 'addresses': ['192.0.2.12']},
    {'name': 'gpu-b-1', 'machine': 'gpu-b', 'aliases': ['beta-1'], 'addresses': ['192.0.2.12']},
    {'name': 'workstation', 'machine': 'workstation', 'aliases': [], 'addresses': ['100.64.0.20']},
]
BOXES = guard.boxes_from_roster(HOSTS)
LOCAL = 'workstation'
COVERED = set()  # host names `fleetq inside --host X` exits 0 for


def fake_run(argv, **kw):
    assert argv[1] == 'inside' and kw['env']['FLEETQ_JOB'].isdigit()
    ok = argv[argv.index('--host') + 1] in COVERED
    return subprocess.CompletedProcess(argv, 0 if ok else 1, '', '' if ok else 'not inside a held slot')


@pytest.fixture(autouse=True)
def _reset(monkeypatch, tmp_path):
    COVERED.clear()
    monkeypatch.setattr(guard, 'CACHE', str(tmp_path / 'cache' / 'roster.json'))
    monkeypatch.setattr(guard, 'fleetq_bin', lambda: 'fleetq')


def hit(cmd, local=LOCAL):
    h = guard.heavy_match(cmd, BOXES, local)
    return h[0] if h else None


def decide(cmd, local=LOCAL):
    return guard.decide(cmd, BOXES, local, run=fake_run)


# --- what is denied ---------------------------------------------------------------------

@pytest.mark.parametrize('cmd,machine', [
    ("ssh gpu-a 'docker run --rm -p 8000:8000 vllm/vllm-openai'", 'gpu-a'),
    ('ssh gpu-a docker restart vllm', 'gpu-a'),
    ("ssh alpha 'systemctl restart llama'", 'gpu-a'),
    ('ssh root@192.0.2.11 systemctl --user stop llama', 'gpu-a'),
    ("ssh 100.64.0.11 'vllm serve some/model'", 'gpu-a'),
    ("ssh gpu-b-1 'docker compose up -d'", 'gpu-b'),
    ('ssh beta-1 systemctl restart vllm', 'gpu-b'),
    ('ssh 192.0.2.12 "nvidia-smi -pl 250"', 'gpu-b'),
    ("ssh gpu-a 'llama-server -m x.gguf'", 'gpu-a'),
    ("ssh gpu-a 'cd x;llama-server -m y'", 'gpu-a'),
    ('ssh gpu-a "bash -c \'vllm serve m\'"', 'gpu-a'),
    ("ssh gpu-a 'pkill -f vllm'", 'gpu-a'),
    ("ssh gpu-a 'kill -9 4242'", 'gpu-a'),
    ('ssh gpu-a sudo reboot', 'gpu-a'),
    ('DOCKER_HOST=ssh://gpu-a docker run hello-world', 'gpu-a'),
    ('docker -H ssh://gpu-b stop vllm', 'gpu-b'),
    ('docker --context gpu-a compose down', 'gpu-a'),
    ('docker --host=tcp://192.0.2.11:2375 restart vllm', 'gpu-a'),
    ("ssh gpu-a 'python3 -m sglang.launch_server --model-path m'", 'gpu-a'),
])
def test_remote_changes_denied(cmd, machine):
    assert hit(cmd) == machine, cmd
    reason = decide(cmd)
    assert reason and machine in reason and 'fleetq run --host ' + machine in reason


def test_local_control_denied_when_this_machine_is_in_the_roster():
    for cmd in ['brew services restart ollama', 'systemctl --user restart vllm', 'launchctl load ~/x.mlx.plist',
                'llama-server -m x.gguf', 'python3 -m mlx_lm.server --port 8080', 'vllm serve m']:
        assert hit(cmd) == 'workstation', cmd
        assert 'workstation' in decide(cmd)


def test_local_control_ignored_when_this_machine_is_not_in_the_roster():
    for cmd in ['brew services restart ollama', 'llama-server -m x.gguf', 'vllm serve m']:
        assert hit(cmd, local=None) is None, cmd


def test_ssh_to_a_roster_box_is_local_control_on_that_machine():
    assert hit("ssh workstation 'launchctl load ~/x.mlx.plist'") == 'workstation'
    assert hit('ssh 100.64.0.20 brew services restart ollama') == 'workstation'


def test_nested_ssh_to_roster_box_not_hidden():
    assert hit("ssh laptop 'ssh workstation launchctl load ~/x.mlx.plist'") == 'workstation'


def test_local_command_after_ssh_to_other_host_still_denied():
    for cmd in ["ssh laptop uptime; launchctl load ~/x.mlx.plist",
                "ssh laptop 'launchctl load x.mlx.plist' && launchctl load ~/x.mlx.plist",
                "ssh laptop true\nbrew services restart ollama"]:
        assert hit(cmd) == 'workstation', cmd


# --- what passes ------------------------------------------------------------------------

@pytest.mark.parametrize('cmd', [
    'ssh gpu-a docker ps',
    'ssh gpu-a "docker logs --tail 50 vllm"',
    'ssh gpu-a nvidia-smi --query-gpu=memory.used --format=csv',
    'ssh gpu-a cat /etc/os-release',
    'ssh gpu-a uptime',
    'curl -s http://192.0.2.11:8000/v1/models',
    'curl -s http://gpu-a:8000/v1/chat/completions -d @req.json',
    'docker ps',
    'docker run hello-world',  # no transport, no box: a local container is not fleet work
    'ls ~/bin/llama-server',
    'ssh gpu-a "grep \'error\' llama-server.log | tail"',
    'last reboot',
    'launchctl list | grep mlx',
])
def test_read_only_and_unrelated_commands_pass(cmd):
    assert hit(cmd) is None, cmd
    assert decide(cmd) is None


@pytest.mark.parametrize('cmd', [
    "git commit -m 'ssh gpu-a docker restart vllm; run bench.py against 192.0.2.11'",
    'git log --grep "ssh gpu-a systemctl restart llama"',
    'gh pr create --title "gpu-a: docker run fix" --body "ssh gpu-a vllm serve m"',
])
def test_git_and_gh_never_count(cmd):
    assert decide(cmd) is None


@pytest.mark.parametrize('cmd', [
    "ssh laptop 'launchctl bootstrap gui/$(id -u) ~/x.mlx.plist'",
    'ssh laptop "launchctl bootout gui/501 ~/x.mlx.plist"',
    'ssh laptop launchctl bootstrap gui/$(id -u) ~/x.mlx.plist',
    'ssh -o ConnectTimeout=5 -p 22 me@laptop.example.net launchctl load ~/x.mlx.plist',
    'ssh 192.0.2.99 -- launchctl kickstart -k gui/501/local.mlx',
    'ssh 192.0.2.99 python3 -m mlx_lm.server --port 8080',
    "ssh laptop 'launchctl unload a.mlx.plist; launchctl load a.mlx.plist'",
    "cd /tmp && ssh -t laptop 'brew services restart ollama' | tail -5",
])
def test_ssh_to_a_host_outside_the_roster_is_not_checked_as_local_control(cmd):
    assert hit(cmd) is None, cmd
    assert decide(cmd) is None


def test_word_boundaries():
    # gpu-a2 and 192.0.2.110 are not gpu-a and 192.0.2.11
    assert hit('ssh gpu-a2 docker restart vllm') is None
    assert hit('ssh 192.0.2.110 docker restart vllm') is None
    assert hit('ssh gpu-a.example.net docker restart vllm') == 'gpu-a'


def test_masking_keeps_length():
    cmd = "ssh laptop 'launchctl load x.mlx.plist'; echo hi"
    out = guard.local_part(cmd, BOXES)
    assert len(out) == len(cmd) and 'echo hi' in out and 'launchctl' not in out


# --- benchmarks need no ssh -------------------------------------------------------------

@pytest.mark.parametrize('cmd', [
    'python3 bench.py --url http://gpu-a:8000/v1',
    'uv run scripts/bench_latency.py --base-url http://192.0.2.12:8000/v1',
    'BASE=http://gpu-a:8000 bash run-bench.sh',
    'llama-bench -m x.gguf --host gpu-a',
    "ssh gpu-a 'python3 bench.py'",
    'for i in $(seq 20); do curl -s http://gpu-b:8000/v1/chat/completions -d @r.json; done',
    'while true; do curl http://192.0.2.11:8000/v1/completions -d @r.json; done',
])
def test_bench_aimed_at_a_box_denied(cmd):
    assert hit(cmd) in ('gpu-a', 'gpu-b'), cmd
    assert decide(cmd)


@pytest.mark.parametrize('cmd', [
    'cp bench.py http://gpu-a/x',
    'sed -i s/a/b/ bench.py && echo gpu-a',
    'python3 bench.py --url http://laptop:8000/v1',  # not a roster box
    'for f in *.json; do curl -s http://laptop:8000/v1/chat/completions -d @$f; done',
])
def test_bench_lookalikes_pass(cmd):
    assert hit(cmd, local=None) is None, cmd


# --- the ways to be allowed -------------------------------------------------------------

def test_fleetq_run_wrapper_allowed():
    assert decide('fleetq run --host gpu-a --expect 30 --label x -- ssh gpu-a docker restart vllm') is None
    assert decide('fleetq run --host gpu-a --label bench -- python3 bench.py --url http://gpu-a:8000/v1') is None
    assert decide('fleetq run --host workstation --label x -- launchctl load x.mlx.plist') is None


def test_fleetq_run_after_the_verb_does_not_count():
    assert decide('ssh gpu-a docker restart vllm; fleetq run --host gpu-a -- true')


def test_covering_job_allows():
    COVERED.add('gpu-a')
    assert decide('FLEETQ_JOB=7 ssh gpu-a docker restart vllm') is None
    assert decide('FLEETQ_JOB="7" ssh gpu-a docker restart vllm') is None


def test_job_covering_one_pool_allows_the_machine():
    COVERED.add('gpu-b-1')  # the machine name itself is not covered, one of its pools is
    assert decide('FLEETQ_JOB=7 ssh gpu-b docker restart vllm') is None


def test_non_covering_job_denied():
    COVERED.add('gpu-a')
    reason = decide('FLEETQ_JOB=7 ssh gpu-b docker restart vllm')
    assert reason and 'does not cover gpu-b' in reason and 'not inside a held slot' in reason


def test_job_checked_for_the_machine_name_first(monkeypatch):
    asked = []

    def run(argv, **kw):
        asked.append(argv[argv.index('--host') + 1])
        return subprocess.CompletedProcess(argv, 1, '', 'no')
    assert guard.decide('FLEETQ_JOB=3 ssh gpu-b docker stop x', BOXES, LOCAL, run=run)
    assert asked == ['gpu-b', 'gpu-b-0', 'gpu-b-1']


def test_non_numeric_job_needs_acquire_in_the_same_command():
    reason = decide('FLEETQ_JOB=$ID ssh gpu-a docker restart vllm')
    assert reason and 'not a job number' in reason
    assert decide('fleetq acquire --host gpu-a --wait; FLEETQ_JOB=$ID ssh gpu-a docker restart vllm') is None


def test_fleetq_missing_when_checking_a_job_denies_with_the_reason():
    def run(argv, **kw):
        raise FileNotFoundError('fleetq')
    reason = guard.decide('FLEETQ_JOB=7 ssh gpu-a docker restart vllm', BOXES, LOCAL, run=run)
    assert reason and 'could not check FLEETQ_JOB=7' in reason


def test_deny_message_only_names_flags_the_cli_has():
    from fleetq import cli
    subs = cli.parser()._subparsers._group_actions[0].choices
    for line in guard.HOW.splitlines():
        m = re.search(r'fleetq (run|acquire|status)\b', line)
        if not m:
            continue
        known = {o for a in subs[m.group(1)]._actions for o in a.option_strings}
        for flag in re.findall(r'--[a-z-]+', line):
            assert flag in known, (m.group(1), flag)
    assert '--exclusive' in {o for a in subs['run']._actions for o in a.option_strings}


# --- the roster: no roster, cache, refresh ------------------------------------------------

def test_no_roster_allows_everything(monkeypatch, capsys):
    monkeypatch.setattr(guard, 'load_roster', lambda: [])
    guard.pre_tool_use({'tool_input': {'command': 'ssh gpu-a docker restart vllm'}})
    assert capsys.readouterr().out == ''


def roster_run(calls, hosts=None, fail=False):
    def run(argv, **kw):
        calls.append(argv)
        if fail:
            raise FileNotFoundError('fleetq')
        return subprocess.CompletedProcess(argv, 0, json.dumps({'hosts': hosts or HOSTS, 'services': []}), '')
    return run


def test_roster_fetched_and_cached():
    calls = []
    assert guard.load_roster(run=roster_run(calls)) == HOSTS
    assert calls == [['fleetq', 'roster', '--json']]
    assert json.load(open(guard.CACHE))['hosts'] == HOSTS
    assert guard.load_roster(run=roster_run(calls, fail=True)) == HOSTS  # fresh cache: no second call
    assert len(calls) == 1


def test_stale_cache_refreshed():
    guard.load_roster(run=roster_run([]))
    old = time.time() - guard.CACHE_TTL - 5
    os.utime(guard.CACHE, (old, old))
    calls = []
    other = [{'name': 'gpu-z', 'machine': 'gpu-z', 'aliases': [], 'addresses': []}]
    assert guard.load_roster(run=roster_run(calls, other)) == other
    assert len(calls) == 1


def test_stale_cache_used_when_fleetq_fails_and_failure_is_not_retried_at_once():
    guard.load_roster(run=roster_run([]))
    old = time.time() - guard.CACHE_TTL - 5
    os.utime(guard.CACHE, (old, old))
    calls = []
    assert guard.load_roster(run=roster_run(calls, fail=True)) == HOSTS
    assert guard.load_roster(run=roster_run(calls, fail=True)) == HOSTS
    assert len(calls) == 1


@pytest.mark.parametrize('run', [
    lambda argv, **kw: subprocess.CompletedProcess(argv, 1, '', 'server down'),
    lambda argv, **kw: subprocess.CompletedProcess(argv, 0, 'not json', ''),
    lambda argv, **kw: subprocess.CompletedProcess(argv, 0, '{"hosts": 3}', ''),
    lambda argv, **kw: (_ for _ in ()).throw(subprocess.TimeoutExpired(argv, 5)),
])
def test_no_cache_and_fleetq_failing_gives_empty_roster(run):
    assert guard.load_roster(run=run) == []


def test_unreadable_cache_is_ignored():
    os.makedirs(os.path.dirname(guard.CACHE))
    Path(guard.CACHE).write_text('{broken')
    assert guard.load_roster(run=roster_run([])) == HOSTS


def test_local_machine_matches_hostname_and_short_name(monkeypatch):
    for name in ['workstation', 'Workstation.example.net', 'workstation.local']:
        monkeypatch.setattr(guard.socket, 'gethostname', lambda n=name: n)
        assert guard.local_machine(BOXES) == 'workstation', name
    monkeypatch.setattr(guard.socket, 'gethostname', lambda: 'laptop.example.net')
    assert guard.local_machine(BOXES) is None


# --- the hook entry points ------------------------------------------------------------------

def run_main(monkeypatch, capsys, payload, argv=()):
    monkeypatch.setattr(sys, 'stdin', io.StringIO(payload if isinstance(payload, str) else json.dumps(payload)))
    monkeypatch.setattr(sys, 'argv', ['fleetq-guard.py', *argv])
    assert guard.main() == 0
    return capsys.readouterr().out


def test_pre_tool_use_prints_a_deny_decision(monkeypatch, capsys):
    monkeypatch.setattr(guard, 'load_roster', lambda: HOSTS)
    monkeypatch.setattr(guard.socket, 'gethostname', lambda: 'laptop')
    out = run_main(monkeypatch, capsys, {'tool_input': {'command': 'ssh gpu-a docker restart vllm'}})
    d = json.loads(out)['hookSpecificOutput']
    assert d['hookEventName'] == 'PreToolUse' and d['permissionDecision'] == 'deny'
    assert d['permissionDecisionReason'].startswith('fleetq guard: ') and 'gpu-a' in d['permissionDecisionReason']


def test_pre_tool_use_prints_nothing_to_allow(monkeypatch, capsys):
    monkeypatch.setattr(guard, 'load_roster', lambda: HOSTS)
    assert run_main(monkeypatch, capsys, {'tool_input': {'command': 'ssh gpu-a docker ps'}}) == ''


def test_empty_stdin_and_missing_command_allow(monkeypatch, capsys):
    assert run_main(monkeypatch, capsys, '') == ''
    assert run_main(monkeypatch, capsys, {}) == ''


def test_internal_error_allows(monkeypatch, capsys):
    def boom():
        raise RuntimeError('boom')
    monkeypatch.setattr(guard, 'load_roster', boom)
    assert run_main(monkeypatch, capsys, {'tool_input': {'command': 'ssh gpu-a docker restart vllm'}}) == ''
    monkeypatch.setattr(guard, 'load_roster', lambda: HOSTS)
    monkeypatch.setattr(guard, 'decide', boom)
    assert run_main(monkeypatch, capsys, {'tool_input': {'command': 'ssh gpu-a docker restart vllm'}}) == ''


def test_session_start_writes_the_owner_line(monkeypatch, capsys, tmp_path):
    env_file = tmp_path / 'env'
    monkeypatch.setenv('CLAUDE_ENV_FILE', str(env_file))
    assert run_main(monkeypatch, capsys, {'session_id': 'ab12-cd34-ef56'}, ['session-start']) == ''
    assert env_file.read_text() == 'export FLEETQ_OWNER=claude-ab12cd34\n'


def test_session_start_without_env_file_or_id_writes_nothing(monkeypatch, capsys, tmp_path):
    monkeypatch.delenv('CLAUDE_ENV_FILE', raising=False)
    assert run_main(monkeypatch, capsys, {'session_id': 'abcd'}, ['session-start']) == ''
    env_file = tmp_path / 'env'
    monkeypatch.setenv('CLAUDE_ENV_FILE', str(env_file))
    run_main(monkeypatch, capsys, {}, ['session-start'])
    assert not env_file.exists()


def test_script_runs_as_a_hook_and_exits_zero_on_empty_stdin():
    r = subprocess.run([sys.executable, str(_PATH)], input='', capture_output=True, text=True)
    assert r.returncode == 0 and r.stdout == '' and r.stderr == ''
