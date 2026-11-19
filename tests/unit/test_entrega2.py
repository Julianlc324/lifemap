from datetime import datetime

import pytest
from lifemap.metrics import cutoff_exclusive, risk_label, survival_observation
from streaming.persistence import rebuild_state


def test_calendar_year_includes_leap_day():
    assert risk_label(0, '2020-03-01', '2019-03-01') == 1
    assert risk_label(0, '2020-03-02', '2019-03-01') == 0
    assert risk_label(0, '2021-02-28', '2020-02-29') == 1


def test_cutoff_uses_exclusive_midnight():
    assert cutoff_exclusive('2020-06-30') == datetime(2020, 7, 1)


def test_censored_duration_extends_to_dataset_end():
    open_obs = survival_observation('2010-01-01', '2011-01-01', 0, '2022-01-19', '2012-01-01')
    closed = survival_observation('2010-01-01', '2011-01-01', 1, '2022-01-19', '2012-01-01')
    assert open_obs['duration'] > closed['duration']
    assert open_obs['in_window'] and not closed['in_window']
    assert open_obs['entry'] == 730


def test_replay_rebuilds_without_mutating_baseline():
    baseline = {'n_reviews': 10, 'sent_sum': 2, 'stars_sum': 35, 'monthly': {},
                'first_review': '2020-01-01', 'last_review': '2020-06-30'}
    reviews = [{'date': datetime(2020, 7, 1), 'stars': 1, 'prediction_id': 0}]
    once = rebuild_state(baseline, reviews)
    assert once == rebuild_state(baseline, reviews)
    assert once['n_reviews'] == 11
    assert baseline['n_reviews'] == 10 and baseline['monthly'] == {}


def test_reject_impossible_survival_dates():
    with pytest.raises(ValueError):
        survival_observation('2020-01-02', '2020-01-01', 1, '2022-01-01', '2010-01-01')


def test_hotspot_requires_contiguous_sufficient_windows():
    from streaming.persistence import hotspot_fields
    prev = {'window_end': datetime(2020, 7, 1), 'count': 20, 'pct_negative': 10}
    now = {'window_start': datetime(2020, 7, 1), 'count': 20, 'pct_negative': 25}
    assert hotspot_fields(now, prev)['hotspot'] is True
    assert hotspot_fields(now, None)['hotspot'] is False
    assert hotspot_fields(dict(now, count=14), prev)['hotspot'] is False
    assert hotspot_fields(dict(now, window_start=datetime(2020, 8, 1)), prev)['hotspot'] is False
    # Una actualización tardía de la predecesora debe quitar la alerta anterior.
    assert hotspot_fields(now, dict(prev, pct_negative=20))['hotspot'] is False
