"""Humo aislado: nunca publica ni escribe en las colecciones productivas."""
import os
import subprocess
import tempfile
import time
import uuid
from datetime import timedelta

import pytest
from kafka.admin import KafkaAdminClient
from lifemap import config, db, models
from lifemap.metrics import cutoff_exclusive
from streaming.producer import ensure_topic


@pytest.fixture(scope='session')
def stream_system():
    if os.environ.get('RUN_STREAM_SMOKE') != '1':
        pytest.skip('RUN_STREAM_SMOKE=1 requiere Kafka/Mongo/modelos reales')
    name = 'lifemap_ci_' + uuid.uuid4().hex[:12]
    client = db.client()
    source = client[config.MONGO_DB]
    target = client[name]
    boundary = cutoff_exclusive(config.CUTOFF_T)
    reviews = list(source.reviews.find({'date': {'$gte': boundary}}).sort([('date', 1), ('_id', 1)]).limit(100))
    assert len(reviews) == 100, 'Se requieren 100 reseñas reales reservadas después de T'
    ids = list({r['business_id'] for r in reviews})
    target.businesses.insert_many(list(source.businesses.find({'_id': {'$in': ids}})))
    baseline = list(source.risk_features.find({'_id': {'$in': ids}}))
    if baseline:
        target.risk_features.insert_many(baseline)
    ensure_topic(config.KAFKA_BOOTSTRAP, name, 3)
    with tempfile.TemporaryDirectory(prefix='lifemap-smoke-') as folder:
        env = {**os.environ, 'MONGO_DB': name, 'KAFKA_TOPIC': name,
               'CHECKPOINT_DIR': folder + '/checkpoints', 'SPARK_MASTER': 'local[2]',
               'SPARK_DRIVER_MEMORY': '1g', 'SPARK_EXECUTOR_MEMORY': '1g'}
        with open(folder + '/stream.log', 'w+') as log:
            proc = subprocess.Popen(['python3', '-m', 'streaming.stream_job'], env=env, stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 240
                while time.monotonic() < deadline:
                    if proc.poll() is not None:
                        log.seek(0)
                        pytest.fail(log.read()[-15000:])
                    if target.meta.find_one({'_id': 'stream_status'}):
                        break
                    time.sleep(2)
                else:
                    pytest.fail('El consumidor no cargó modelos en 240 segundos')
                yield {'db': target, 'topic': name, 'reviews': reviews, 'process': proc, 'log': log}
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=10)
                # Solo recursos con UUID creados por esta prueba.
                client.drop_database(name)
                admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP)
                try:
                    admin.delete_topics([name])
                finally:
                    admin.close()
