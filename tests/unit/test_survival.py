import pytest
import numpy as np
import pandas as pd
from jobs.survival import at_risk, fit_curve


def test_km_known_censored_example():
    frame = pd.DataFrame({'km_days': [1, 2, 3], 'entry_days': [0, 0, 0],
                          'event': [1, 0, 1], 'in_window': [True] * 3})
    result = fit_curve(frame, 'global', 'fixture')
    values = {p['t_days']: p for p in result['curve']}
    assert values[0]['at_risk'] == 3
    assert values[1]['survival'] == pytest.approx(2/3, abs=0.0001)
    assert values[2]['survival'] == values[1]['survival']
    assert values[3]['survival'] == 0
    assert result['median_days'] == 3
    assert result['survival_5y'] is None


def test_risk_set_respects_late_entry():
    assert at_risk(np.array([10, 10]), np.array([0, 5]), 0) == 1
    assert at_risk(np.array([10, 10]), np.array([0, 5]), 4) == 1
    assert at_risk(np.array([10, 10]), np.array([0, 5]), 6) == 2
