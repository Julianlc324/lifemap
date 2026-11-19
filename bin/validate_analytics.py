"""Recalcula E1.8/E2.5 en una base temporal; no modifica datos productivos."""
import os
import uuid
from pathlib import Path

from pymongo import MongoClient

source_name = os.environ.get('MONGO_DB', 'lifemap')
name = 'lifemap_validation_' + uuid.uuid4().hex[:12]
client = MongoClient(os.environ.get('MONGO_URI', 'mongodb://mongo:27017'))
source, target = client[source_name], client[name]
try:
    target.businesses.insert_many(list(source.businesses.find({})))
    target.meta.insert_one(source.meta.find_one({'_id': 'dataset'}))
    os.environ['MONGO_DB'] = name
    os.environ['SPARK_MASTER'] = 'local[2]'
    from lifemap import db
    db.ensure_indexes()
    from jobs import survival, load_balancing
    print('Kaplan-Meier:', survival.run(), flush=True)
    print('Benchmark:', load_balancing.run(), flush=True)
    import json
    curves = list(target.survival.find({}, {'_id': 0}))
    Path('/data/reports/survival_curves.json').write_text(json.dumps(curves, default=str))
finally:
    client.drop_database(name)
    client.close()
