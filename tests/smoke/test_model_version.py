from lifemap import models


def test_loaded_model_is_versioned_without_retraining(stream_system):
    status = stream_system['db'].meta.find_one({'_id': 'stream_status'})
    assert status['retrained'] is False
    assert status['sentiment_version'] == models.current('sentiment')['version']
    assert status['risk_version'] == models.current('risk')['version']
    assert models.current('risk')['sentiment_version'] == status['sentiment_version']
