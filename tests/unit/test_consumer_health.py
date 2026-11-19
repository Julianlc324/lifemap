from datetime import datetime, timedelta, timezone
from api.health import consumer_state


def test_missing_or_stale_heartbeat_is_not_healthy():
    now = datetime(2026, 11, 19, tzinfo=timezone.utc)
    assert consumer_state({}, now)[1] is False
    assert consumer_state({'active': True, 'heartbeat_at': now - timedelta(minutes=3)}, now)[1] is False
    assert consumer_state({'active': False, 'heartbeat_at': now}, now)[1] is False
    assert consumer_state({'active': True, 'heartbeat_at': now + timedelta(seconds=5)}, now)[1] is False


def test_bson_naive_utc_heartbeat_is_supported():
    now = datetime(2026, 11, 19, tzinfo=timezone.utc)
    assert consumer_state({'active': True, 'heartbeat_at': now.replace(tzinfo=None)}, now)[1] is True
