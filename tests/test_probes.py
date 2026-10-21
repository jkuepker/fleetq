"""Health checks against small fake servers on localhost."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from fleetq import core, probes
from fleetq.core import Service

GIB = 1024 ** 3


def serve(routes):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            code, body = routes.get(self.path, (404, ''))
            data = body if isinstance(body, bytes) else (body if isinstance(body, str) else json.dumps(body)).encode()
            self.send_response(code)
            self.end_headers()
            self.wfile.write(data)

    srv = ThreadingHTTPServer(('127.0.0.1', 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f'http://127.0.0.1:{srv.server_address[1]}'


@pytest.fixture
def fake():
    servers = []

    def start(routes):
        srv, url = serve(routes)
        servers.append(srv)
        return url
    yield start
    for s in servers:
        s.shutdown()


def test_model_server_up_with_in_flight_count(fake):
    url = fake({'/v1/models': (200, {'data': [{'id': 'guidance'}, {'id': 'capcom'}]}),
                '/metrics': (200, 'vllm:num_requests_running{model_name="guidance"} 2.0\n'
                                  'vllm:num_requests_waiting{model_name="guidance"} 1.0\n')})
    obs = probes.probe_service(Service('s', 'h', url=url + '/v1', models=['guidance']), 5.0)
    assert (obs.state, obs.in_flight, obs.at) == ('up', 3, 5.0)


def test_model_listing_keeps_root_and_context_length(fake):
    url = fake({'/v1/models': (200, {'data': [
        {'id': 'guidance', 'owned_by': 'vllm', 'root': 'nvidia/Qwen3.6-35B-A3B-NVFP4', 'max_model_len': 32768},
        {'id': 'bare'}]})})
    obs = probes.probe_service(Service('s', 'h', url=url + '/v1'), 0)
    assert obs.models == ['guidance', 'bare']
    assert obs.served == [{'id': 'guidance', 'root': 'nvidia/Qwen3.6-35B-A3B-NVFP4', 'max_model_len': 32768},
                          {'id': 'bare', 'root': None, 'max_model_len': None}]


def test_engine_progress_counter_is_read_from_the_same_metrics(fake):
    url = fake({'/v1/models': (200, {'data': [{'id': 'guidance'}]}),
                '/metrics': (200, 'vllm:num_requests_running{engine="0",model_name="guidance"} 1.0\n'
                                  'vllm:iteration_tokens_total_count{engine="0",model_name="guidance"} 245824.0\n'
                                  'vllm:iteration_tokens_total_sum{engine="0",model_name="guidance"} 9.9e6\n'
                                  'vllm:generation_tokens_total{engine="0",model_name="guidance"} 246122.0\n')})
    obs = probes.probe_service(Service('s', 'h', url=url + '/v1', models=['guidance']), 0)
    assert (obs.in_flight, obs.progress) == (1, 245824.0)
    url = fake({'/v1/models': (200, {'data': [{'id': 'eecom-chat'}]}),
                '/metrics': (200, 'llamacpp:requests_processing 1\nllamacpp:n_decode_total 77\n')})
    assert probes.engine_load(url + '/v1') == (1, 77.0)
    url = fake({'/v1/models': (200, {'data': [{'id': 'm'}]}),
                '/metrics': (200, 'vllm:num_requests_running{model_name="m"} 0.0\n')})
    assert probes.engine_load(url + '/v1') == (0, None)  # no counter: never judged wedged


NATIVE_METRICS = """\
# HELP vllm:num_requests_running Number of requests currently running on GPU.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="Qwen3.8",engine="0"} %(running)d
# HELP vllm:num_requests_waiting Number of requests waiting to be processed.
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="Qwen3.8",engine="0"} 0
# HELP radiance:engine_steps_total Scheduler steps executed.
# TYPE radiance:engine_steps_total counter
radiance:engine_steps_total{model_name="Qwen3.8",engine="0"} %(steps)d
# HELP radiance:engine_steps_prefill_total Scheduler steps carrying only prefill chunks.
# TYPE radiance:engine_steps_prefill_total counter
radiance:engine_steps_prefill_total{model_name="Qwen3.8",engine="0"} 2389
radiance:engine_steps_decode_total{model_name="Qwen3.8",engine="0"} 17104
radiance:num_requests_preempted{model_name="Qwen3.8",engine="0"} 0
"""


def test_native_radiance_progress_is_engine_steps_total_only(fake):
    # Lines from a real native /metrics scrape (native-radiance-2026-10-07/w3/logs/native-c2-metrics-end.txt).
    # Only radiance:engine_steps_total counts; the prefill/decode sub-counters must not be added in.
    url = fake({'/v1/models': (200, {'data': [{'id': 'Qwen3.8'}]}),
                '/metrics': (200, NATIVE_METRICS % {'running': 2, 'steps': 19493})})
    obs = probes.probe_service(Service('s', 'h', url=url + '/v1', models=['Qwen3.8']), 0)
    assert (obs.state, obs.in_flight, obs.progress) == ('up', 2, 19493.0)


def test_native_radiance_frozen_steps_with_requests_in_flight_read_as_wedged(fake):
    # Same flow the monitor runs: two probes, then carry_stall, then the wedge check.
    routes = {'/v1/models': (200, {'data': [{'id': 'Qwen3.8'}]}),
              '/metrics': (200, NATIVE_METRICS % {'running': 1, 'steps': 500})}
    url = fake(routes)
    svc = Service('s', 'h', url=url + '/v1', models=['Qwen3.8'])
    first = core.carry_stall(None, probes.probe_service(svc, 100.0))
    second = core.carry_stall(first, probes.probe_service(svc, 400.0))
    assert second.stuck_since == 100.0 and core.wedged(second, 400.0, 180)
    routes['/metrics'] = (200, NATIVE_METRICS % {'running': 1, 'steps': 501})
    third = core.carry_stall(second, probes.probe_service(svc, 430.0))
    assert third.stuck_since is None and not core.wedged(third, 430.0, 180)


def test_wrong_model_auth_and_down(fake):
    url = fake({'/v1/models': (200, {'data': [{'id': 'glm-5.3'}]})})
    obs = probes.probe_service(Service('s', 'h', url=url + '/v1', models=['guidance']), 0)
    assert obs.state == 'wrong' and obs.detail == 'serving glm-5.3; expected guidance'
    url = fake({'/v1/models': (401, '')})
    assert probes.probe_service(Service('s', 'h', url=url + '/v1'), 0).state == 'unverified'
    obs = probes.probe_service(Service('s', 'h', url='http://127.0.0.1:9/v1'), 0)
    assert obs.state == 'down' and 'unreachable' in obs.detail


def test_llama_cpp_slots_fallback(fake):
    url = fake({'/v1/models': (200, {'data': [{'id': 'eecom-chat'}]}),
                '/slots': (200, [{'is_processing': True}, {'is_processing': False}])})
    assert probes.probe_service(Service('s', 'h', url=url + '/v1', models=['eecom-chat']), 0).in_flight == 1


def test_collector_host_memory_and_amd_device(fake):
    snap = {'collected_at': 100.0, 'host_memory_available_bytes': 23 * GIB,
            'devices': [{'vendor': 'NVIDIA', 'name': 'GB10', 'utilization_fraction': 0.4},
                        {'vendor': 'AMD', 'name': 'AMD GPU 0x7551', 'memory_total_bytes': 32 * GIB,
                         'memory_used_bytes': 30 * GIB, 'utilization_fraction': 0.97}]}
    url = fake({'/snapshot': (200, snap)})
    host = probes.collector_reading({'collector': url}, 'tok', 110.0)
    assert host == {'free_gib': 23.0, 'util': 0.97}
    amd = probes.collector_reading({'collector': url, 'collector_memory': 'AMD'}, 'tok', 110.0)
    assert amd == {'free_gib': 2.0, 'util': 0.97}
    assert probes.collector_reading({'collector': url}, 'tok', 500.0) is None  # stale sample


def test_collector_picks_one_of_two_identical_cards_by_pci_address(fake):
    card = {'vendor': 'AMD', 'name': 'AMD GPU 0x7551', 'memory_total_bytes': 32 * GIB}
    snap = {'collected_at': 100.0,
            'devices': [card | {'id': '0000:07:00.0', 'memory_used_bytes': 0, 'utilization_fraction': 0.0},
                        card | {'id': '0000:03:00.0', 'memory_used_bytes': 30 * GIB, 'utilization_fraction': 0.9}]}
    url = fake({'/snapshot': (200, snap)})
    first = probes.collector_reading({'collector': url, 'collector_memory': '0000:03:00.0'}, 'tok', 110.0)
    assert first == {'free_gib': 2.0, 'util': 0.9}
    second = probes.collector_reading({'collector': url, 'collector_memory': '0000:07:00.0'}, 'tok', 110.0)
    assert second == {'free_gib': 32.0, 'util': 0.0}
