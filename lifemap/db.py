"""Acceso a MongoDB y creación de índices."""
from pymongo import ASCENDING, DESCENDING, GEOSPHERE, MongoClient

from . import config

_client = None


def client():
    global _client
    if _client is None:
        _client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=3000, appname="lifemap")
    return _client


def get_db():
    return client()[config.MONGO_DB]


def ensure_indexes(db=None):
    db = db if db is not None else get_db()
    db.businesses.create_index([("location", GEOSPHERE)])
    db.businesses.create_index([("main_category", ASCENDING)])
    db.businesses.create_index([("geohash", ASCENDING)])
    db.reviews.create_index([("business_id", ASCENDING)])
    db.reviews.create_index([("date", ASCENDING)])
    db.closure_rates.create_index([("by", ASCENDING), ("key", ASCENDING)], unique=True)
    db.closure_rates.create_index([("geometry", GEOSPHERE)], sparse=True)
    db.cell_sentiment.create_index([("geometry", GEOSPHERE)])
    db.zones.create_index([("geometry", GEOSPHERE)])
    # Entrega 2
    db.reviews_stream.create_index([("business_id", ASCENDING)])
    db.reviews_stream.create_index([("processed_at", DESCENDING)])
    db.windows.create_index([("geometry", GEOSPHERE)])
    db.windows.create_index([("geohash", ASCENDING), ("window_start", ASCENDING)], unique=True)
    db.windows.create_index([("window_start", DESCENDING)])
    db.alerts.create_index([("location", GEOSPHERE)])
    db.alerts.create_index([("active", ASCENDING), ("probability", DESCENDING)])
    db.risk_features.create_index([("location", GEOSPHERE)])
    db.risk_scores.create_index([("location", GEOSPHERE)])
    db.survival.create_index([("group_type", ASCENDING), ("group", ASCENDING)], unique=True)


def set_meta(key, **values):
    get_db().meta.update_one({"_id": key}, {"$set": values}, upsert=True)


def get_meta(key):
    return get_db().meta.find_one({"_id": key}) or {}
