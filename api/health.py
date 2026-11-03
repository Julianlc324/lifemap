"""Estado de cada servicio para /health (MongoDB, Spark, Dask, Kafka + retraso del consumidor, API)."""
import time

import requests

from lifemap import config, db, models

_cache = {"t": 0.0, "value": None}
TTL = 5


def _timed(fn):
    t0 = time.perf_counter()
    try:
        info = fn() or {}
        info.setdefault("status", "ok")
    except Exception as exc:  # noqa: BLE001
        info = {"status": "down", "error": f"{type(exc).__name__}: {exc}"[:300]}
    info["latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    return info


def mongo():
    database = db.get_db()
    database.command("ping")
    return {"businesses": database.businesses.estimated_document_count(),
            "reviews": database.reviews.estimated_document_count()}


def spark():
    data = requests.get(f"{config.SPARK_MASTER_UI}/json/", timeout=3).json()
    alive = [w for w in data.get("workers", []) if w.get("state") == "ALIVE"]
    return {"status": "ok" if data.get("status") == "ALIVE" and alive else "degraded",
            "master": data.get("status"), "alive_workers": len(alive),
            "cores": data.get("cores"), "running_apps": len(data.get("activeapps", []))}


def dask():
    r = requests.get(f"{config.DASK_DASHBOARD}/health", timeout=3)
    r.raise_for_status()
    workers = None
    try:
        ident = requests.get(f"{config.DASK_DASHBOARD}/json/identity.json", timeout=3).json()
        workers = len(ident.get("workers", {}))
    except Exception:  # noqa: BLE001
        pass
    status = "ok" if workers is None or workers >= 2 else "degraded"
    return {"status": status, "scheduler": r.text.strip(), "workers": workers}


def kafka():
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=config.KAFKA_BOOTSTRAP, request_timeout_ms=4000,
                             api_version_auto_timeout_ms=3000)
    try:
        parts = consumer.partitions_for_topic(config.KAFKA_TOPIC) or set()
        tps = [TopicPartition(config.KAFKA_TOPIC, p) for p in sorted(parts)]
        end = consumer.end_offsets(tps) if tps else {}
    finally:
        consumer.close()
    progress = db.get_db().stream_progress.find_one({"_id": config.KAFKA_TOPIC}) or {}
    committed = progress.get("offsets", {})
    lag = {str(tp.partition): max(0, end[tp] - int(committed.get(str(tp.partition), 0))) for tp in tps}
    return {"status": "ok" if len(tps) >= 3 else "degraded", "topic": config.KAFKA_TOPIC,
            "partitions": len(tps), "end_offsets": {str(tp.partition): end[tp] for tp in tps},
            "consumer_lag": sum(lag.values()), "consumer_lag_by_partition": lag,
            "last_batch_at": progress.get("updated_at"), "stream_model": progress.get("model_versions")}


def report():
    now = time.time()
    if _cache["value"] and now - _cache["t"] < TTL:
        return _cache["value"]
    services = {"api": {"status": "ok"}, "mongodb": _timed(mongo), "spark": _timed(spark),
                "dask": _timed(dask), "kafka": _timed(kafka)}
    statuses = {s["status"] for s in services.values()}
    value = {
        "status": "ok" if statuses == {"ok"} else ("down" if "down" in statuses else "degraded"),
        "commit": config.GIT_COMMIT,
        "services": services,
        "models": {name: (models.current(name) or {}).get("version") for name in ("sentiment", "risk")},
        "scope": {"metro": config.METRO_NAME, "category_family": config.CATEGORY_FAMILY},
        "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    _cache.update(t=now, value=value)
    return value
