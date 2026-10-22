"""Read-only health checks: model endpoints, in-flight counters, machine liveness, collectors.

Nothing here sends a completion request or changes a server; every call is a GET or a
TCP connect with a short timeout.
"""
from __future__ import annotations

import json
import re
import socket
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from .core import Observation

GIB = 1024 ** 3
METRIC_RE = re.compile(r'^([a-zA-Z_:][\w:]*)(\{[^}]*\})?\s+([-+0-9.eE]+|NaN)$')
# Prometheus gauges that count requests a server is working on or holding (vLLM, llama.cpp; a native engine exports the vllm: names).
BUSY_GAUGES = ('vllm:num_requests_running', 'vllm:num_requests_waiting',
               'llamacpp:requests_processing', 'llamacpp:requests_deferred')
# Counters that move on every engine step (vLLM: one histogram sample per scheduler iteration,
# prefill chunks included; llama.cpp: llama_decode() calls; a native engine: scheduler steps).
# A wedged engine stops them all. A server exposes one family; if two ever appeared together
# engine_load would sum them, which still moves whenever either does.
PROGRESS_COUNTERS = ('vllm:iteration_tokens_total_count', 'llamacpp:n_decode_total',
                     'native:engine_steps_total')


def http_get(url, headers=None, timeout=5.0):
    """(status code or None, body bytes or error text)."""
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, b''
    except (OSError, ValueError) as exc:
        return None, str(getattr(exc, 'reason', exc)).encode()


def tcp_open(address, timeout=3.0):
    host, _, port = address.rpartition(':')
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except (OSError, ValueError):
        return False


def engine_load(base_url):
    """(requests in progress, engine progress counter), each None when the server does not say.

    In-flight comes from /metrics (vLLM, llama.cpp) or /slots (llama.cpp); progress is the sum
    of PROGRESS_COUNTERS from the same /metrics read.
    """
    root = base_url.rstrip('/')
    if root.endswith('/v1'):
        root = root[:-3]
    active = progress = None
    code, body = http_get(root + '/metrics', timeout=3.0)
    if code == 200:
        for line in body.decode(errors='replace').splitlines():
            m = METRIC_RE.match(line.strip())
            if not m or m.group(3) == 'NaN':
                continue
            if m.group(1) in BUSY_GAUGES:
                active = (active or 0.0) + float(m.group(3))
            elif m.group(1) in PROGRESS_COUNTERS:
                progress = (progress or 0.0) + float(m.group(3))
        if active is not None:
            return int(active), progress
    code, body = http_get(root + '/slots', timeout=3.0)
    if code == 200:
        try:
            return sum(1 for s in json.loads(body) if s.get('is_processing')), progress
        except (ValueError, AttributeError, TypeError):
            pass
    return None, progress


def probe_service(svc, now):
    """Observation for one service, or None when it has nothing to probe."""
    if svc.url:
        code, body = http_get(svc.url.rstrip('/') + '/models')
        if code == 200:
            try:
                data = json.loads(body).get('data', [])
                listing = [{'id': m['id'], 'root': m.get('root'), 'max_model_len': m.get('max_model_len')}
                           for m in data]
            except (ValueError, KeyError, TypeError, AttributeError):
                return Observation('wrong', detail='answers /models with an unreadable body', at=now)
            served = [m['id'] for m in listing]
            missing = [m for m in svc.models if m not in served]
            active, progress = engine_load(svc.url)
            obs = Observation('up' if not missing else 'wrong', served, active, at=now, progress=progress,
                              served=listing)
            if missing:
                obs.detail = f'serving {", ".join(served) or "nothing"}; expected {", ".join(missing)}'
            return obs
        if code in (401, 403):
            active, progress = engine_load(svc.url)
            return Observation('unverified', detail=f'HTTP {code} on /models',
                               in_flight=active, at=now, progress=progress)
        detail = f'HTTP {code} on /models' if code else f'unreachable ({body.decode(errors="replace")[:80]})'
        return Observation('down', detail=detail, at=now)
    if svc.probe_tcp:
        ok = tcp_open(svc.probe_tcp)
        return Observation('up' if ok else 'down', detail='' if ok else f'{svc.probe_tcp} closed', at=now)
    return None


def collector_reading(host, token, now, max_age_s=90.0):
    """{'free_gib', 'util'} from the host's hardware collector, or None when unknown or stale.

    util is the busiest matching GPU's utilisation (0..1), when the driver reports it.
    collector_memory is 'host', or text matched against each device's id (PCI address),
    vendor and name, so one card of several identical ones can be picked by its address.
    """
    code, body = http_get(host['collector'].rstrip('/') + '/snapshot',
                          {'Authorization': 'Bearer ' + token}, timeout=4.0)
    if code != 200:
        return None
    try:
        snap = json.loads(body)
    except ValueError:
        return None
    if abs(now - float(snap.get('collected_at') or 0)) > max_age_s:
        return None
    pick = host.get('collector_memory', 'host')
    devices = snap.get('devices') or []
    if pick != 'host':
        devices = [d for d in devices
                   if pick.lower() in (d.get('id', '') + ' ' + d.get('vendor', '') + ' '
                                       + d.get('name', '')).lower()]
    utils = [d['utilization_fraction'] for d in devices if d.get('utilization_fraction') is not None]
    free = None
    if pick == 'host':
        avail = snap.get('host_memory_available_bytes')
        free = avail / GIB if avail is not None else None
    elif devices:
        total, used = devices[0].get('memory_total_bytes'), devices[0].get('memory_used_bytes')
        if total is not None and used is not None:
            free = (total - used) / GIB
    return {'free_gib': free, 'util': max(utils) if utils else None}


def probe_all(services, machines, collectors, now, port=22):
    """Run every check concurrently.

    machines: {machine: address}; collectors: {host name: (host config dict, token)}.
    Returns (observations by service, online by machine, collector readings by host).
    """
    with ThreadPoolExecutor(max_workers=16) as pool:
        svc_f = {s.name: pool.submit(probe_service, s, now) for s in services}
        mach_f = {m: pool.submit(tcp_open, f'{addr}:{port}') for m, addr in machines.items()}
        coll_f = {h: pool.submit(collector_reading, cfg, tok, now) for h, (cfg, tok) in collectors.items()}
        obs = {name: f.result() for name, f in svc_f.items()}
        online = {m: f.result() for m, f in mach_f.items()}
        readings = {h: f.result() for h, f in coll_f.items()}
    return ({k: v for k, v in obs.items() if v is not None}, online,
            {k: v for k, v in readings.items() if v is not None})
