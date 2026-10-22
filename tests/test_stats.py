"""Queue statistics on synthetic job rows."""
import pytest

from fleetq import stats

NOW, SINCE = 10_000.0, 0.0


def row(id, host, created, started, ended, owner='x', label='job', kind='lease', expect=None, **extra):
    state = 'done' if ended is not None else 'running' if started is not None else 'queued'
    return {'id': id, 'state': state, 'kind': kind, 'owner': owner, 'label': label, 'hosts': [host],
            'created_at': created, 'started_at': started, 'ended_at': ended, 'expect_s': expect,
            'reason': '', **extra}


@pytest.fixture
def rows():
    return [
        row(1, 'a', 0, 0, 1000, label='bench 1', expect=500),
        row(2, 'a', 100, 1000, 2000, label='bench 2', expect=2000),
        row(3, 'a', 200, 1500, 2200, owner='y', label='bench 3', kind='run', expect=1400),
        row(4, 'a', 300, 5000, 6000, owner='y', label='deploy'),
        row(5, 'a', 400, 2000, 2500, owner='y', label='eval', deadline=99_999.0),
        row(6, 'b', 50, 9000, None, owner='y', label='solo'),
    ]


def test_percentile_interpolates_and_handles_nothing():
    assert stats.percentile([], 0.5) is None
    assert stats.percentile([5], 0.9) == 5
    assert stats.percentile([1, 2, 3, 4], 0.5) == 2.5
    assert stats.percentile([0, 10], 0.9) == 9


def test_host_busy_time_is_the_union_of_running_intervals(rows):
    a = stats.compute(rows, NOW, SINCE, ['a', 'b'])['hosts']['a']
    assert a['jobs'] == 5
    assert a['busy_h'] == pytest.approx(3500 / 3600)
    assert a['busy_pct'] == pytest.approx(35)
    assert a['shared_pct'] == pytest.approx(100 * 700 / 3500)


def test_running_job_counts_until_now_and_idle_hosts_are_listed(rows):
    out = stats.compute(rows, NOW, SINCE, ['a', 'b', 'c'])['hosts']
    assert out['b']['busy_h'] == pytest.approx(1000 / 3600)
    assert out['c']['jobs'] == 0 and out['c']['busy_pct'] == 0 and out['c']['wait']['median_s'] is None


def test_busy_time_is_clipped_to_the_window(rows):
    a = stats.compute(rows, NOW, 500.0, ['a'])['hosts']['a']
    assert a['busy_h'] == pytest.approx((2500 - 500 + 1000) / 3600)
    assert a['jobs'] == 4          # only jobs that started inside the window are counted


def test_waits_and_longest_waits_name_the_job_that_freed_the_slot(rows):
    a = stats.compute(rows, NOW, SINCE, ['a'])['hosts']['a']
    assert a['wait']['n'] == 5 and a['wait']['median_s'] == 1300
    first = a['longest_waits'][0]
    assert (first['id'], first['wait_s']) == (4, 4700)
    assert first['blocker']['id'] == 5          # last job to end on 'a' before #4 started
    assert [w['id'] for w in a['longest_waits']] == [4, 5, 3, 2, 1]


def test_reservation_waits_from_its_booked_start(rows):
    booked = row(7, 'c', 0, 600, 700, start_at=500.0)
    c = stats.compute([booked], NOW, SINCE, ['c'])['hosts']['c']
    assert c['wait']['median_s'] == 100


def test_backfilled_means_started_while_an_earlier_job_on_that_host_waited(rows):
    out = stats.compute(rows, NOW, SINCE, ['a', 'b'])['hosts']
    assert out['a']['backfilled'] == 1          # #5 overtook #4
    assert out['a']['with_deadline'] == 1 and out['a']['backfilled_with_deadline'] == 1
    assert out['b']['backfilled'] == 0          # #6 waited alone; #5 is on another host


def test_a_waiter_on_another_host_does_not_make_a_job_backfilled():
    rows = [row(1, 'b', 0, 5000, 6000), row(2, 'a', 10, 20, 30)]
    assert stats.compute(rows, NOW, SINCE, ['a', 'b'])['hosts']['a']['backfilled'] == 0


def test_job_that_gave_up_waiting_stops_blocking_the_line():
    gone = row(1, 'a', 0, None, 100)
    gone['state'] = 'cancelled'
    rows = [gone, row(2, 'a', 50, 200, 300)]
    a = stats.compute(rows, NOW, SINCE, ['a'])['hosts']['a']
    assert a['backfilled'] == 0 and a['cancelled_waiting'] == 1


def test_owners_get_jobs_held_host_hours_and_waits(rows):
    rows[0]['hosts'] = ['a', 'b']
    owners = stats.compute(rows, NOW, SINCE, ['a', 'b'])['owners']
    assert owners['x']['jobs'] == 2
    assert owners['x']['held_h'] == pytest.approx((1000 * 2 + 1000) / 3600)
    assert owners['y']['jobs'] == 4 and owners['y']['wait']['n'] == 4


def test_expect_accuracy_per_kind(rows):
    e = stats.compute(rows, NOW, SINCE, ['a'])['expect']
    assert e['lease']['n'] == 2 and e['lease']['median_ratio'] == pytest.approx(1.25)
    assert e['lease']['overdue_pct'] == 50
    assert e['run']['n'] == 1 and e['run']['median_ratio'] == pytest.approx(0.5)
    assert e['run']['overdue_pct'] == 0


def test_label_prefixes_need_three_finished_runs(rows):
    labels = stats.compute(rows, NOW, SINCE, ['a'])['labels']
    assert labels == [{'prefix': 'bench', 'n': 3, 'median_s': 1000, 'p90_s': 1000}]


def test_label_prefix_drops_run_numbers_and_arguments():
    assert stats.label_prefix('bench-qwen: 3b run 7') == 'bench-qwen'
    assert stats.label_prefix('Eval2 --fast') == 'eval'
    assert stats.label_prefix('42') == '42'


def test_only_filters_to_the_given_hosts_and_missing_columns_are_fine(rows):
    out = stats.compute(rows, NOW, SINCE, ['a', 'b'], only=['b'])
    assert list(out['hosts']) == ['b'] and list(out['owners']) == ['y']
    assert 'deadline' not in rows[0] and 'start_at' not in rows[0]
