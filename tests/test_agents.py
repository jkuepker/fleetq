import json
import os
import stat

import pytest

from fleetq import agents, cli


def model(name, url='http://gpu-a1:8000/v1', host='gpu1', root='org/Big-Model-FP8', context=32768,
          usable=True, caps=('text',)):
    return {'model': name, 'url': url, 'host': host, 'machine': url.split('//')[1].split(':')[0],
            'root': root, 'context': context, 'usable': usable, 'capabilities': list(caps)}


FLEET = [model('chat-a'), model('chat-b'),  # two served names for one model: aliases
         model('vision', url='http://gpu-b1:8001/v1', host='gpu2', root='org/See-7B', context=65536,
               caps=('text', 'image'))]

# Hand-written in styles json.dumps would not reproduce: they must come back byte for byte.
PI = '''{
  "providers": {
    "my-box": {
      "baseUrl": "http://gpu1.example.net:8888/v1",
      "api": "openai-completions",
      "apiKey": "sk-secret-pi",
      "models": [
        { "id": "hand-made" }
      ]
    }
  }
}
'''
OPENCODE = '''{
 "$schema" : "https:\\/\\/opencode.ai\\/config.json",
 "model" : "mine\\/m1",
 "provider" : {
  "mine" : {
   "npm" : "@ai-sdk\\/openai-compatible",
   "options" : { "baseURL" : "http:\\/\\/127.0.0.1:9000\\/v1", "apiKey" : "sk-secret-oc" },
   "models" : { "m1" : { "name" : "M1" } }
  }
 }
}'''
OMP = '''providers:
  remote:
    baseUrl: "https://api.example.com/v1"
    apiKey: sk-secret-omp
    api: openai-completions
    models:
      - id: "x/y"

retry:
  max: 3
'''


@pytest.fixture
def home(tmp_path, monkeypatch):
    agent, xdg = tmp_path / 'agent', tmp_path / 'xdg'
    (xdg / 'opencode').mkdir(parents=True)
    agent.mkdir()
    monkeypatch.setenv('PI_CODING_AGENT_DIR', str(agent))
    monkeypatch.setenv('XDG_CONFIG_HOME', str(xdg))
    files = {'pi': agent / 'models.json', 'omp': agent / 'models.yml',
             'opencode': xdg / 'opencode' / 'opencode.json'}
    files['pi'].write_text(PI)
    files['omp'].write_text(OMP)
    files['opencode'].write_text(OPENCODE)
    for p in files.values():
        p.chmod(0o600)
    return files


def run(models=FLEET, **kw):
    lines = []
    code = agents.sync(models, out=lines.append, **kw)
    return code, '\n'.join(lines)


def omp_providers(text):
    """The top-level keys under providers:, in order (enough YAML for these tests)."""
    lines = text.splitlines()
    top = lines.index('providers:')
    out = []
    for l in lines[top + 1:]:
        if l and not l[0].isspace() and not l.startswith('#'):
            break
        if l.startswith('  ') and not l[2].isspace() and not l.lstrip().startswith('#'):
            out.append(l.strip().rstrip(':'))
    return out


def test_add_writes_every_harness(home):
    assert run()[0] == 0
    pi = json.loads(home['pi'].read_text())['providers']
    assert list(pi) == ['my-box', 'fleetq-gpu1-8000', 'fleetq-gpu2-8001']
    one = pi['fleetq-gpu1-8000']
    assert one['baseUrl'] == 'http://gpu-a1:8000/v1' and one['apiKey'] == 'fleetq'
    assert one['models'] == [{'id': 'chat-a', 'name': 'Big-Model-FP8 (gpu1)', 'contextWindow': 32768,
                              'maxTokens': 8192, 'input': ['text']}]
    assert pi['fleetq-gpu2-8001']['models'][0]['input'] == ['text', 'image']

    oc = json.loads(home['opencode'].read_text())['provider']
    assert list(oc) == ['mine', 'fleetq-gpu1-8000', 'fleetq-gpu2-8001']
    m = oc['fleetq-gpu2-8001']['models']['vision']
    assert m['limit'] == {'context': 65536, 'output': 16384}
    assert m['modalities'] == {'input': ['text', 'image'], 'output': ['text']} and m['tool_call']
    assert oc['fleetq-gpu1-8000']['options'] == {'baseURL': 'http://gpu-a1:8000/v1'}

    omp = home['omp'].read_text()
    assert omp_providers(omp) == ['remote', 'fleetq-gpu1-8000', 'fleetq-gpu2-8001']
    assert omp.endswith('\nretry:\n  max: 3\n')
    assert '    auth: none\n' in omp and '        maxTokens: 8192\n' in omp


def test_foreign_entries_keep_their_bytes(home):
    run()
    pi, oc, omp = (home[h].read_text() for h in ('pi', 'opencode', 'omp'))
    assert pi.startswith(PI[:PI.index('\n    }\n  }')])  # the inline { "id": ... } survives
    assert '{ "id": "hand-made" }' in pi
    assert '"options" : { "baseURL" : "http:\\/\\/127.0.0.1:9000\\/v1", "apiKey" : "sk-secret-oc" }' in oc
    assert '"baseURL" : "http:\\/\\/gpu-a1:8000\\/v1"' in oc  # new entries follow the file's style
    assert omp.startswith(OMP.split('\n\nretry')[0] + '\n  # >>> fleetq')
    run(models=[])
    assert home['pi'].read_text() == PI
    assert home['opencode'].read_text() == OPENCODE


def test_second_run_is_a_noop(home):
    run()
    before = {h: (p.read_bytes(), p.stat().st_mtime_ns) for h, p in home.items()}
    code, out = run()
    assert code == 0 and out == ''
    assert {h: (p.read_bytes(), p.stat().st_mtime_ns) for h, p in home.items()} == before


def test_update_and_removal(home):
    run()
    bigger = [dict(m, context=131072) if m['url'].endswith('8000/v1') else m for m in FLEET]
    run(bigger)
    assert json.loads(home['pi'].read_text())['providers']['fleetq-gpu1-8000']['models'][0]['contextWindow'] == 131072
    assert 'contextWindow: 131072' in home['omp'].read_text()
    run(FLEET[2:])  # gpu1 went away
    for h in ('pi', 'opencode'):
        section = 'providers' if h == 'pi' else 'provider'
        assert list(json.loads(home[h].read_text())[section]) == ['mine' if h == 'opencode' else 'my-box', 'fleetq-gpu2-8001']
    assert omp_providers(home['omp'].read_text()) == ['remote', 'fleetq-gpu2-8001']
    run([])
    assert home['omp'].read_text() == OMP


def test_alias_collapse_and_usable_filter():
    want = agents.desired(FLEET + [model('down', url='http://gpu-c1:8000/v1', host='gpu3', usable=False)])
    assert list(want) == ['fleetq-gpu1-8000', 'fleetq-gpu2-8001']
    assert [m['id'] for m in want['fleetq-gpu1-8000']['models']] == ['chat-a']
    assert 'fleetq-gpu3-8000' in agents.desired(FLEET + [model('down', url='http://gpu-c1:8000/v1', host='gpu3',
                                                               usable=False)], include_all=True)
    # without a root nothing says two names are one model, so each stays
    plain = agents.desired([model('a', root=None), model('b', root=None)])
    assert [m['id'] for m in plain['fleetq-gpu1-8000']['models']] == ['a', 'b']


def test_unknown_context_is_left_out(home):
    run([model('m', context=None)])
    entry = json.loads(home['pi'].read_text())['providers']['fleetq-gpu1-8000']['models'][0]
    assert 'contextWindow' not in entry and 'maxTokens' not in entry
    oc = json.loads(home['opencode'].read_text())['provider']['fleetq-gpu1-8000']['models']['m']
    assert 'limit' not in oc  # OpenCode requires both context and output in limit


def test_dry_run_redacts_and_writes_nothing(home):
    home['pi'].write_text(PI.replace('"hand-made" }', '"hand-made2" }'))
    before = {h: p.read_text() for h, p in home.items()}
    code, out = run(dry_run=True)
    assert code == 0
    assert {h: p.read_text() for h, p in home.items()} == before
    assert '+    "fleetq-gpu1-8000": {' in out and '+  fleetq-gpu1-8000:' in out
    assert 'sk-secret' not in out and '"apiKey": "<redacted>"' in out
    assert '"maxTokens": 8192' in out  # a token budget is not a secret
    assert 'hand-made provider my-box points at a fleet machine' in out
    assert not any(p.with_name(p.name + '.fleetq-bak').exists() for p in home.values())


def test_redact_yaml_and_json_lines():
    assert agents.redact('    apiKey: sk-1\n') == ['    apiKey: <redacted>\n']
    assert agents.redact('"api_key" : "a\\"b", "x": 1\n') == ['"api_key" : "<redacted>", "x": 1\n']
    assert agents.redact('  Authorization: Bearer x\n') == ['  Authorization: <redacted>\n']


def test_jsonc_names_are_left_to_jsonc(home):
    (home['opencode'].parent / 'opencode.jsonc').write_text(
        '{\n  // hand-made\n  "provider": {\n    "fleetq-gpu1-8000": {"options": {"baseURL": "http://x/v1"}},\n  },\n}\n')
    code, out = run()
    assert code == 0 and 'fleetq-gpu1-8000 is defined in opencode.jsonc' in out
    assert list(json.loads(home['opencode'].read_text())['provider']) == ['mine', 'fleetq-gpu2-8001']


def test_backup_and_modes(home):
    run()
    for p in home.values():
        bak = p.with_name(p.name + '.fleetq-bak')
        assert stat.S_IMODE(p.stat().st_mode) == 0o600 and stat.S_IMODE(bak.stat().st_mode) == 0o600
    assert home['pi'].with_name('models.json.fleetq-bak').read_text() == PI
    assert not [f for f in os.listdir(home['pi'].parent) if f.endswith('.tmp')]


def test_skip_a_file_the_harness_changed_meanwhile(home):
    new = home['pi'].read_text() + ' '
    assert agents.write_atomic(home['pi'], PI.replace('my-box', 'other'), new) is False
    assert home['pi'].read_text() == PI


def test_missing_files_and_harness_dirs(tmp_path, monkeypatch):
    agent = tmp_path / 'agent'
    agent.mkdir()
    monkeypatch.setenv('PI_CODING_AGENT_DIR', str(agent))
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'none'))  # OpenCode not installed
    assert run()[0] == 0
    assert list(json.loads((agent / 'models.json').read_text())['providers']) == ['fleetq-gpu1-8000', 'fleetq-gpu2-8001']
    assert (agent / 'models.yml').read_text().startswith('providers:\n  # >>> fleetq')
    assert not (tmp_path / 'none').exists()
    run([])
    assert (agent / 'models.yml').read_text() == 'providers: {}\n'


@pytest.mark.parametrize('text', [
    'providers: {remote: {baseUrl: x}}\n',
    'providers:\n  fleetq-gpu1-8000:\n    baseUrl: x\n',
    'providers:\n\tremote: {}\n',
    'providers:\n  # >>> fleetq sync-agents: generated, edits here are overwritten\n  a: 1\n',
])
def test_omp_layouts_we_will_not_edit(home, text):
    home['omp'].write_text(text)
    code, out = run(harnesses=['omp'])
    assert code == 1 and 'not changed' in out
    assert home['omp'].read_text() == text


def test_cli_needs_the_readonly_token(home, monkeypatch, tmp_path):
    monkeypatch.setattr(cli, 'CONFIG_DIR', tmp_path / 'cfg')
    monkeypatch.delenv('FLEETQ_READONLY_TOKEN', raising=False)
    monkeypatch.setenv('FLEETQ_TOKEN', 'admin')
    calls = []
    monkeypatch.setattr(cli.Client, 'call', lambda self, *a, **k: calls.append(self.token))
    assert cli.main(['sync-agents']) == 1
    assert calls == []


def test_cli_leaves_files_alone_when_the_server_is_down(home, monkeypatch):
    monkeypatch.setenv('FLEETQ_READONLY_TOKEN', 'ro')
    before = {h: p.read_text() for h, p in home.items()}

    def down(self, *a, **k):
        assert self.token == 'ro'
        raise cli.Unreachable('down')
    monkeypatch.setattr(cli.Client, 'call', down)
    assert cli.main(['sync-agents']) == cli.EXIT_UNREACHABLE
    monkeypatch.setattr(cli.Client, 'call', lambda self, *a, **k: {'error': 'odd'})
    assert cli.main(['sync-agents']) == 1
    assert {h: p.read_text() for h, p in home.items()} == before


def test_cli_syncs_only_the_named_harnesses(home, monkeypatch):
    monkeypatch.setenv('FLEETQ_READONLY_TOKEN', 'ro')
    paths = []
    monkeypatch.setattr(cli.Client, 'call', lambda self, m, path, *a, **k: paths.append(path) or {'models': FLEET})
    assert cli.main(['sync-agents', '--only', 'pi']) == 0
    assert paths == ['/v1/models?all=1']
    assert 'fleetq-' in home['pi'].read_text()
    assert home['omp'].read_text() == OMP and home['opencode'].read_text() == OPENCODE
    assert cli.main(['sync-agents', '--only', 'codex']) == 1
