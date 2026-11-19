import json
import time

from kafka import KafkaProducer
from lifemap import config
from streaming.producer import to_message


def wait_for(system, predicate, seconds=240):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if system['process'].poll() is not None:
            system['log'].seek(0)
            raise AssertionError(system['log'].read()[-15000:])
        if predicate():
            return
        time.sleep(2)
    system['log'].seek(0)
    raise AssertionError('Timeout de streaming\n' + system['log'].read()[-15000:])


def test_100_messages_and_replay_are_idempotent(stream_system):
    s = stream_system
    database = s['db']
    producer = KafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP, acks='all',
                             key_serializer=lambda k: k.encode(),
                             value_serializer=lambda v: json.dumps(v).encode())
    try:
        for doc in s['reviews']:
            producer.send(s['topic'], key=doc['business_id'], value=to_message(doc)).get(timeout=30)
        wait_for(s, lambda: database.reviews_stream.count_documents({}) == 100
                 and database.stream_progress.find_one({'_id': s['topic']}) is not None)
        docs = list(database.reviews_stream.find({}))
        assert all(r['prediction'] in ('negativo', 'neutral', 'positivo') for r in docs)
        assert all(0 <= r['probability'] <= 1 and r['processed_at'] and r['model_version'] for r in docs)
        before = {r['_id']: r['n_reviews'] for r in database.risk_state.find({})}
        assert before
        for doc in s['reviews']:
            producer.send(s['topic'], key=doc['business_id'], value=to_message(doc)).get(timeout=30)
        wait_for(s, lambda: sum((database.stream_progress.find_one({'_id': s['topic']}) or {}).get('offsets', {}).values()) >= 200)
        assert database.reviews_stream.count_documents({}) == 100
        assert {r['_id']: r['n_reviews'] for r in database.risk_state.find({})} == before
        assert database.risk_scores.count_documents({}) == len(before)
    finally:
        producer.close()


def test_windows_use_event_time_and_deduplicate(stream_system):
    from datetime import timedelta
    s = stream_system
    database = s['db']
    producer = KafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP, acks='all',
                             key_serializer=lambda k: k.encode(),
                             value_serializer=lambda v: json.dumps(v).encode())
    try:
        # Avanza el watermark con un mensaje exclusivo del topic aislado.
        doc = dict(s['reviews'][-1])
        doc['_id'] = 'watermark-' + doc['_id']
        doc['date'] = doc['date'] + timedelta(days=90)
        producer.send(s['topic'], key=doc['business_id'], value=to_message(doc)).get(timeout=30)
        wait_for(s, lambda: sum(r['count'] for r in database.windows.find({})) == 100)
        for window in database.windows.find({}):
            assert (window['window_end'] - window['window_start']).days == 30
            assert window['geometry']['type'] == 'Polygon'
            assert 0 <= window['negative'] <= window['count']
            assert window['pct_negative'] == round(100 * window['negative'] / window['count'], 2)
    finally:
        producer.close()
