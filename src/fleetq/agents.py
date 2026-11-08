"""fleetq sync-agents: keep the local-model lists of agent harnesses in step with the fleet.

Each distinct model URL in GET /v1/models becomes one harness provider named fleetq-<host>-<port>;
the names one URL serves for one root model are aliases and become one model. Only providers named
fleetq-* are ours: everything else in a harness file is left as it is.

  Pi        $PI_CODING_AGENT_DIR or ~/.pi/agent: models.json, providers.<name>
  omp       $PI_CODING_AGENT_DIR or ~/.omp/agent: models.yml, a marker-delimited block in providers
  OpenCode  $XDG_CONFIG_HOME/opencode or ~/.config/opencode: opencode.json, provider.<name>
            (1.18 reads only the legacy `provider` key; it also merges opencode.jsonc, so names
            defined there are left alone)
"""
from __future__ import annotations

import difflib
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import urlparse

PREFIX = 'fleetq-'
HARNESSES = ('pi', 'omp', 'opencode')
MAX_OUTPUT = 32768
BEGIN = '# >>> fleetq sync-agents: generated, edits here are overwritten'
END = '# <<< fleetq sync-agents'
SECRET = re.compile(r'(key|(?<!max)(?<!max_)token|secret|password|authorization)', re.I)


class Unsafe(Exception):
    """The file's layout is one we cannot edit without risking the parts we don't own."""


# ---------------------------------------------------------------------------------------------
# What the fleet serves


def provider_name(entry):
    u = urlparse(entry['url'])
    host = entry.get('host') or (u.hostname or 'local').split('.')[0]
    port = u.port or (443 if u.scheme == 'https' else 80)
    return f'{PREFIX}{host}-{port}'


def output_limit(context):
    """Room left for the prompt: a harness asking for context-sized output gets a 400 from vLLM."""
    return min(context // 4, MAX_OUTPUT)


def desired(models, include_all=False):
    """{provider name: {'url': ..., 'host': ..., 'models': [model, ...]}}, models sorted by id."""
    out, names = {}, {}
    for m in models:
        if not (include_all or m.get('usable')):
            continue
        if m['url'] not in names:
            base, n = provider_name(m), 1
            name = base
            while name in names.values():
                n += 1
                name = f'{base}-{n}'  # two URLs on one host:port, e.g. different paths
            names[m['url']] = name
        name = names[m['url']]
        prov = out.setdefault(name, {'url': m['url'], 'host': m.get('host') or name, 'models': {}})
        key = m.get('root') or m['model']
        if key in prov['models']:
            continue  # another served name for the same model on this URL
        short = (m.get('root') or m['model']).rstrip('/').split('/')[-1]
        prov['models'][key] = {
            'id': m['model'],
            'name': f'{short} ({prov["host"]})',
            'context': m.get('context'),
            'image': 'image' in (m.get('capabilities') or []),
        }
    return {n: {**p, 'models': sorted(p['models'].values(), key=lambda x: x['id'])}
            for n, p in sorted(out.items())}


def fleet_hosts(models):
    return {n.lower() for m in models for n in [m.get('host'), m.get('machine'), urlparse(m['url']).hostname] if n}


# ---------------------------------------------------------------------------------------------
# Rendering one provider per harness


def pi_provider(p):
    models = []
    for m in p['models']:
        e = {'id': m['id'], 'name': m['name']}
        if m['context']:
            e.update(contextWindow=m['context'], maxTokens=output_limit(m['context']))
        e['input'] = ['text', 'image'] if m['image'] else ['text']
        models.append(e)
    return {'baseUrl': p['url'], 'api': 'openai-completions', 'apiKey': 'fleetq', 'models': models}


def opencode_provider(name, p):
    models = {}
    for m in p['models']:
        e = {'name': m['name']}
        if m['context']:
            e['limit'] = {'context': m['context'], 'output': output_limit(m['context'])}
        e['modalities'] = {'input': ['text', 'image'] if m['image'] else ['text'], 'output': ['text']}
        e.update(tool_call=True, attachment=m['image'])
        models[m['id']] = e
    u = urlparse(p['url'])
    return {'name': f'fleetq {p["host"]}:{u.port or u.scheme}', 'npm': '@ai-sdk/openai-compatible',
            'options': {'baseURL': p['url']}, 'models': models}


def omp_block(want, indent):
    """omp lists explicit models: its openai-models-list discovery sets max output to the full
    context, which vLLM rejects."""
    q = json.dumps  # a JSON string is a valid YAML double-quoted scalar
    i1, i2 = indent, indent * 2
    lines = [i1 + BEGIN]
    for name, p in want.items():
        lines += [f'{i1}{name}:', f'{i2}baseUrl: {q(p["url"])}', f'{i2}api: openai-completions',
                  f'{i2}auth: none', f'{i2}models:']
        for m in p['models']:
            lines += [f'{i2}  - id: {q(m["id"])}', f'{i2}    name: {q(m["name"])}']
            if m['context']:
                lines += [f'{i2}    contextWindow: {m["context"]}',
                          f'{i2}    maxTokens: {output_limit(m["context"])}']
            lines.append(f'{i2}    input: {q(["text", "image"] if m["image"] else ["text"])}')
    lines.append(i1 + END)
    return lines


# ---------------------------------------------------------------------------------------------
# JSON files: splice only the fleetq-* members, so every other byte stays as it was


def json_style(text):
    m = re.search(r'\n([ \t]+)"', text)
    indent = (m.group(1) if '\t' in m.group(1) else ' ' * len(m.group(1))) if m else None
    colon = re.search(r'"\s*:\s*', text)
    return {'indent': indent, 'colon': colon.group(0)[1:] if colon else ': ',
            'ascii': bool(re.search(r'\\u[0-9a-fA-F]{4}', text)), 'slashes': '\\/' in text}


def skip_ws(t, i):
    while i < len(t) and t[i] in ' \t\r\n':
        i += 1
    return i


def value_end(t, i):
    """Index just past the JSON value that starts at t[i]."""
    if t[i] == '"':
        i += 1
        while t[i] != '"':
            i += 2 if t[i] == '\\' else 1
        return i + 1
    if t[i] in '{[':
        depth = 0
        while True:
            if t[i] == '"':
                i = value_end(t, i)
                continue
            if t[i] in '{[':
                depth += 1
            elif t[i] in '}]':
                depth -= 1
            i += 1
            if depth == 0:
                return i
    while i < len(t) and t[i] not in ',}] \t\r\n':
        i += 1
    return i


def members(t, start):
    """[(key, start, end)] of the object at t[start] == '{', and the index of its closing brace."""
    out, i = [], skip_ws(t, start + 1)
    while t[i] != '}':
        ks = i
        key = json.loads(t[ks:value_end(t, ks)])
        i = skip_ws(t, value_end(t, ks))
        i = skip_ws(t, i + 1)  # past ':'
        end = value_end(t, i)
        out.append((key, ks, end))
        i = skip_ws(t, end)
        if t[i] == ',':
            i = skip_ws(t, i + 1)
    return out, i


def render_member(key, value, style, depth):
    ind = style['indent']
    text = json.dumps(key) + style['colon'] + json.dumps(
        value, indent=ind, separators=(',', style['colon']) if ind else (', ', style['colon']),
        ensure_ascii=style['ascii'])
    if ind:
        text = text.replace('\n', '\n' + ind * depth)
    if style['slashes']:
        text = text.replace('/', '\\/')  # outside strings JSON has no '/'
    return text


def rebuild(t, start, depth, keep, extra, style):
    """The object at t[start] with members failing keep() dropped and `extra` appended."""
    mem, close = members(t, start)
    kept = [t[s:e] for k, s, e in mem if keep(k)]
    if len(kept) == len(mem) and not extra:
        return t[start:close + 1]
    ind = style['indent']
    if mem:
        lead = t[start + 1:mem[0][1]]
        sep = t[mem[0][2]:mem[1][1]] if len(mem) > 1 else ',' + lead
        trail = t[mem[-1][2]:close]
    else:
        lead = '\n' + ind * depth if ind else ''
        sep, trail = ',' + lead, '\n' + ind * (depth - 1) if ind else ''
    body = kept + extra
    return '{' + (lead + sep.join(body) + trail if body else '') + '}'


def merge_json(text, section, entries):
    """Replace every fleetq-* member of the object `section` with `entries`; touch nothing else."""
    if text is None or not text.strip():
        if not entries:
            return text
        return json.dumps({section: entries}, indent=2) + '\n'
    data = json.loads(text)
    if not isinstance(data, dict):
        raise Unsafe('top level is not an object')
    if section in data and not isinstance(data[section], dict):
        raise Unsafe(f'"{section}" is not an object')
    style = json_style(text)
    start = skip_ws(text, 0)
    mem, close = members(text, start)
    new = [render_member(k, v, style, 2) for k, v in entries.items()]
    hit = [(s, e) for k, s, e in mem if k == section]
    if hit:
        s, e = hit[-1]
        vs = skip_ws(text, skip_ws(text, value_end(text, s)) + 1)
        inner = rebuild(text, vs, 2, lambda k: not k.startswith(PREFIX), new, style)
        return text[:vs] + inner + text[e:]
    if not entries:
        return text
    sect = render_member(section, {}, style, 1)[:-2] + rebuild('{}', 0, 2, None, new, style)
    return text[:start] + rebuild(text, start, 1, lambda k: True, [sect], style) + text[close + 1:]


def strip_jsonc(text):
    out, i, n = [], 0, len(text)
    while i < n:
        c = text[i]
        if c == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == '\\' else 1
            out.append(text[i:j + 1])
            i = j + 1
        elif text.startswith('//', i):
            i = text.find('\n', i) if '\n' in text[i:] else n
        elif text.startswith('/*', i):
            i = text.find('*/', i + 2) + 2 if '*/' in text[i + 2:] else n
        else:
            out.append(c)
            i += 1
    return re.sub(r',(\s*[}\]])', r'\1', ''.join(out))


# ---------------------------------------------------------------------------------------------
# omp's YAML: no library, so we own one marker-delimited block inside `providers:`


def merge_omp(text, want):
    lines = (text or '').splitlines()
    begins = [i for i, l in enumerate(lines) if l.strip() == BEGIN]
    ends = [i for i, l in enumerate(lines) if l.strip() == END]
    if len(begins) > 1 or len(ends) > 1 or len(begins) != len(ends) or (begins and ends[0] < begins[0]):
        raise Unsafe('fleetq marker comments are missing, doubled or out of order')
    if any('\t' in l[:len(l) - len(l.lstrip())] for l in lines):
        raise Unsafe('tab indentation')
    if begins:
        b, e = begins[0], ends[0]
        lines = lines[:b] + lines[e + 1:]
        at = b
    tops = [i for i, l in enumerate(lines) if re.match(r'''^["']?providers["']?\s*:''', l)]
    if len(tops) > 1:
        raise Unsafe('more than one top-level providers key')
    if tops and not re.match(r'^providers:\s*(#.*)?$', lines[tops[0]]) and not re.match(r'^providers:\s*\{\}\s*$', lines[tops[0]]):
        raise Unsafe('providers is not a block mapping')
    if not tops:
        if not want:
            return text
        if lines and lines[-1].strip():
            lines.append('')
        lines.append('providers:')
        tops = [len(lines) - 1]
    top = tops[0]
    if lines[top].rstrip().endswith('{}'):
        lines[top] = 'providers:'
    # the providers mapping runs until the next line at column 0 that is not blank or a comment
    stop = next((i for i in range(top + 1, len(lines))
                 if lines[i] and not lines[i][0].isspace() and not lines[i].startswith('#')), len(lines))
    children = [l for l in lines[top + 1:stop] if l.strip() and not l.lstrip().startswith('#')]
    indent = ' ' * (len(children[0]) - len(children[0].lstrip())) if children else '  '
    names = [l.strip().split(':')[0].strip('"\'') for l in children if l.startswith(indent) and not l[len(indent)].isspace()]
    if any(n.startswith(PREFIX) for n in names):
        raise Unsafe('a fleetq-* provider outside the fleetq block')
    if begins and not top < at <= stop:
        raise Unsafe('the fleetq block is outside providers')
    if not begins:
        at = stop
        while at > top + 1 and not lines[at - 1].strip():
            at -= 1  # keep blank lines that separate providers from the next key after our block
    block = omp_block(want, indent) if want else []
    lines = lines[:at] + block + lines[at:]
    if not want and not children:
        lines[top] = 'providers: {}'  # an empty block mapping would be null
    return '\n'.join(lines) + '\n'


# ---------------------------------------------------------------------------------------------
# Files


def agent_dir(default):
    return Path(os.environ.get('PI_CODING_AGENT_DIR') or Path.home() / default).expanduser()


def target_path(harness):
    if harness == 'pi':
        return agent_dir('.pi/agent') / 'models.json'
    if harness == 'omp':
        return agent_dir('.omp/agent') / 'models.yml'
    xdg = os.environ.get('XDG_CONFIG_HOME') or Path.home() / '.config'
    return Path(xdg).expanduser() / 'opencode' / 'opencode.json'


def read(path):
    try:
        return path.read_text()
    except FileNotFoundError:
        return None


def redact(text):
    out = []
    for line in (text or '').splitlines(keepends=True):
        red = re.sub(r'("[^"]*' + SECRET.pattern + r'[^"]*"\s*:\s*)"(?:[^"\\]|\\.)*"', r'\1"<redacted>"', line, flags=re.I)
        line = red if red != line else re.sub(r'^(\s*-?\s*["\']?[\w.-]*' + SECRET.pattern + r'[\w.-]*["\']?\s*:\s*)(?!\s*$).+', r'\1<redacted>', line, flags=re.I)
        out.append(line)
    return out


def diff(path, old, new):
    return ''.join(difflib.unified_diff(redact(old), redact(new), str(path), str(path) + ' (new)'))


def write_atomic(path, old, new):
    """Write `new` if the file still holds `old`; False when someone changed it meanwhile."""
    if read(path) != old:
        return False
    mode = path.stat().st_mode & 0o777 if old is not None else 0o600
    if old is not None:
        backup = path.with_name(path.name + '.fleetq-bak')
        fd = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
        with os.fdopen(fd, 'w') as f:
            f.write(old)
        os.chmod(backup, mode)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f'.{path.name}.', suffix='.tmp')
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, 'w') as f:
            f.write(new)
            f.flush()
            os.fsync(f.fileno())
        if read(path) != old:
            os.unlink(tmp)
            return False
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
    return True


# ---------------------------------------------------------------------------------------------
# One harness


def foreign(harness, text, hosts):
    """Hand-made providers whose base URL points at a fleet machine: reported, never changed."""
    if not text:
        return []
    if harness == 'omp':
        found, name = [], None
        for line in text.splitlines():
            m = re.match(r'^\s{1,8}([\w.-]+):\s*$', line)
            if m:
                name = m.group(1)
            u = re.match(r'^\s+baseUrl:\s*["\']?([^"\'\s]+)', line)
            if u and name and not name.startswith(PREFIX):
                found.append((name, u.group(1)))
        pairs = found
    else:
        try:
            data = json.loads(strip_jsonc(text))
        except ValueError:
            return []
        section = data.get('providers' if harness == 'pi' else 'provider') or {}
        pairs = [(n, (p.get('baseUrl') or (p.get('options') or {}).get('baseURL') or ''))
                 for n, p in section.items() if isinstance(p, dict) and not n.startswith(PREFIX)]
    return [(n, u) for n, u in pairs
            if u and (urlparse(u).hostname or '').split('.')[0].lower() in hosts]


def jsonc_names(path):
    """Provider names opencode.jsonc defines next to opencode.json; OpenCode merges the two."""
    text = read(path.with_suffix('.jsonc'))
    if not text:
        return set()
    try:
        return set((json.loads(strip_jsonc(text)).get('provider') or {}))
    except (ValueError, AttributeError):
        raise Unsafe(f'cannot parse {path.with_suffix(".jsonc")}') from None


def render(harness, path, old, want):
    if harness == 'pi':
        return merge_json(old, 'providers', {n: pi_provider(p) for n, p in want.items()}), []
    if harness == 'omp':
        return merge_omp(old, want), []
    taken = jsonc_names(path)
    clash = sorted(n for n in want if n in taken)
    entries = {n: opencode_provider(n, p) for n, p in want.items() if n not in taken}
    return merge_json(old, 'provider', entries), [f'{n} is defined in opencode.jsonc; left to it' for n in clash]


def sync(models, harnesses=HARNESSES, include_all=False, dry_run=False, out=print):
    """Bring each harness file in line with `models`. Returns 0, or 1 if a file was unsafe to edit."""
    want = desired(models, include_all)
    hosts = fleet_hosts(models)
    code = 0
    for harness in harnesses:
        path = target_path(harness)
        if not path.parent.exists():
            continue  # harness not installed here
        old = read(path)
        try:
            new, notes = render(harness, path, old, want)
        except (Unsafe, ValueError) as exc:
            out(f'{harness}: {path}: not changed: {exc}')
            code = 1
            continue
        for note in notes:
            out(f'{harness}: {note}')
        if dry_run:
            for name, url in foreign(harness, old, hosts):
                out(f'{harness}: hand-made provider {name} points at a fleet machine ({url}); not touched')
        if new == old:
            continue
        if dry_run:
            out(diff(path, old, new).rstrip('\n'))
            continue
        if write_atomic(path, old, new):
            out(f'{harness}: updated {path} ({", ".join(want) or "no fleet models"})')
        else:
            out(f'{harness}: {path} changed while syncing; next run retries')
    return code
