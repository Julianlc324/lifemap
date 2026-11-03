"""API Flask de LifeMap.

Entrega 1: /health, /businesses/near, /businesses/within, /zones/summary, /stats/closure, /predict
Entrega 2: /live/hotspots, /alerts, /survival/<categoria>, /risk/near (y /health con Kafka)
Apoyo:     /, /ping, /reports/markdown, /powerbi/* (tablas planas para Power BI)
"""
import math
from datetime import date, datetime, timedelta
from pathlib import Path

from bson import ObjectId
from flask import Flask, Response, jsonify, request, send_from_directory
from flask.json.provider import DefaultJSONProvider
from werkzeug.exceptions import HTTPException

from lifemap import config, db
from lifemap.metrics import closure_rate

from . import health
from .validation import ApiError, date_param, lat_lon, number, polygon, text

EARTH_RADIUS_M = 6378100.0
STATIC = Path(__file__).parent / "static"


class Json(DefaultJSONProvider):
    ensure_ascii = False
    sort_keys = False

    def default(self, o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, float) and not math.isfinite(o):
            return None
        return super().default(o)


app = Flask(__name__)
app.json = Json(app)


def database():
    return db.get_db()


@app.errorhandler(ApiError)
def _api_error(err):
    return jsonify({"error": err.message}), err.status


@app.errorhandler(404)
def _not_found(_):
    return jsonify({"error": "Ruta no encontrada", "see": "/"}), 404


@app.errorhandler(Exception)
def _unexpected(err):
    if isinstance(err, HTTPException):
        return jsonify({"error": err.description}), err.code
    app.logger.exception(err)
    return jsonify({"error": f"{type(err).__name__}: {err}"[:300]}), 500


def args():
    """Parámetros de query string o de cuerpo JSON (POST)."""
    merged = dict(request.args)
    if request.is_json:
        body = request.get_json(silent=True) or {}
        if isinstance(body, dict):
            merged.update(body)
    return merged


def category_filter(category):
    if not category:
        return {}
    return {"$or": [{"main_category": category}, {"categories": category}]}


BUSINESS_FIELDS = {"_id": 0, "business_id": 1, "name": 1, "city": 1, "main_category": 1,
                   "categories": 1, "stars": 1, "review_count": 1, "is_open": 1, "location": 1,
                   "geohash": 1, "first_review": 1, "last_review": 1, "business_days": 1,
                   "event": 1, "censored": 1, "late_entry": 1, "sentiment_mean": 1,
                   "pct_negative_pred": 1}


# ------------------------------------------------------------------------------- básicos
@app.get("/")
def index():
    return send_from_directory(STATIC, "index.html")


@app.get("/ping")
def ping():
    return jsonify({"status": "ok", "commit": config.GIT_COMMIT})


@app.get("/config")
def public_config():
    lat_min, lat_max, lon_min, lon_max = config.METRO_BBOX
    return jsonify({"metro": config.METRO_NAME, "category_family": config.CATEGORY_FAMILY,
                    "center": [(lat_min + lat_max) / 2, (lon_min + lon_max) / 2],
                    "cutoff_T": config.CUTOFF_T.isoformat(), "min_business_days": config.MIN_BUSINESS_DAYS,
                    "commit": config.GIT_COMMIT})


@app.get("/health")
def health_endpoint():
    return jsonify(health.report())


# ------------------------------------------------------------------------------- Entrega 1
@app.get("/businesses/near")
def businesses_near():
    a = args()
    lat, lon = lat_lon(a)
    radius = number(a, "radius", default=1000, lo=1, hi=50000)
    limit = number(a, "limit", default=50, lo=1, hi=500, cast=int)
    category = text(a, "category")
    query = {"location": {"$near": {"$geometry": {"type": "Point", "coordinates": [lon, lat]},
                                    "$maxDistance": radius}}, **category_filter(category)}
    items = list(database().businesses.find(query, BUSINESS_FIELDS).limit(limit))
    return jsonify({"center": [lat, lon], "radius_m": radius, "category": category,
                    "count": len(items), "items": items})


@app.route("/businesses/within", methods=["GET", "POST"])
def businesses_within():
    a = args()
    poly = polygon(a.get("polygon"))
    limit = number(a, "limit", default=200, lo=1, hi=2000, cast=int)
    category = text(a, "category")
    query = {"location": {"$geoWithin": {"$geometry": poly}}, **category_filter(category)}
    coll = database().businesses
    items = list(coll.find(query, BUSINESS_FIELDS).limit(limit))
    return jsonify({"polygon": poly, "category": category, "total": coll.count_documents(query),
                    "count": len(items), "items": items})


@app.get("/zones/summary")
def zones_summary():
    a = args()
    lat, lon = lat_lon(a)
    radius = number(a, "radius", default=2000, lo=50, hi=50000)
    category = text(a, "category")
    min_days = number(a, "min_days", default=config.MIN_BUSINESS_DAYS, lo=0, cast=int)
    geo_near = {"near": {"type": "Point", "coordinates": [lon, lat]}, "distanceField": "distance_m",
                "maxDistance": radius, "spherical": True, "key": "location"}
    if category:
        geo_near["query"] = category_filter(category)
    pipeline = [
        {"$geoNear": geo_near},
        {"$group": {"_id": None, "businesses": {"$sum": 1},
                    "sentiment_mean": {"$avg": "$sentiment_mean"},
                    "pct_negative_pred": {"$avg": "$pct_negative_pred"},
                    "stars_mean": {"$avg": "$stars"},
                    "closures": {"$sum": {"$ifNull": ["$event", 0]}},
                    "business_days": {"$sum": {"$ifNull": ["$business_days", 0]}},
                    "open": {"$sum": "$is_open"}, "mean_distance_m": {"$avg": "$distance_m"}}},
    ]
    res = next(database().businesses.aggregate(pipeline), None)
    if not res:
        return jsonify({"center": [lat, lon], "radius_m": radius, "businesses": 0,
                        "message": "No hay negocios en el radio indicado"})
    res.pop("_id")
    res["closure_rate_pct"] = closure_rate(res["closures"], res["business_days"])
    res["sufficient"] = res["business_days"] >= min_days
    for k in ("sentiment_mean", "pct_negative_pred", "stars_mean", "closure_rate_pct", "mean_distance_m"):
        if res.get(k) is not None:
            res[k] = round(res[k], 3)
    if not res["sufficient"]:
        res["warning"] = (f"Menos de {min_days} días-negocio: la tasa no es comparable "
                          "(grupo pequeño).")
    return jsonify({"center": [lat, lon], "radius_m": radius, "category": category,
                    "min_days": min_days, **res})


@app.get("/stats/closure")
def stats_closure():
    a = args()
    by = text(a, "by") or "category"
    if by not in ("category", "cell"):
        raise ApiError("'by' debe ser 'category' o 'cell'")
    min_days = number(a, "min_days", default=config.MIN_BUSINESS_DAYS, lo=0, cast=int)
    limit = number(a, "limit", default=50, lo=1, hi=1000, cast=int)
    sort = text(a, "sort") or "rate"
    if sort not in ("rate", "days", "closures"):
        raise ApiError("'sort' debe ser 'rate', 'days' o 'closures'")
    sort_field = {"rate": "closure_rate_pct", "days": "business_days", "closures": "closures"}[sort]
    query = {"by": by, "business_days": {"$gte": min_days}}
    key = text(a, "key")
    if key:
        query["key"] = key
    coll = database().closure_rates
    items = list(coll.find(query, {"_id": 0}).sort(sort_field, -1).limit(limit))
    excluded = coll.count_documents({"by": by, "business_days": {"$lt": min_days}})
    return jsonify({"by": by, "min_days": min_days, "formula": "cierres / (días-negocio / 365) * 100",
                    "count": len(items), "excluded_small_groups": excluded, "items": items})


@app.route("/predict", methods=["GET", "POST"])
def predict_endpoint():
    a = args()
    texts = a.get("texts")
    if texts is None:
        single = a.get("text")
        if single in (None, ""):
            raise ApiError("Envíe 'text' (cadena) o 'texts' (lista) con la reseña en inglés")
        texts = [single]
    if not isinstance(texts, list) or not texts or len(texts) > 100:
        raise ApiError("'texts' debe ser una lista de 1 a 100 cadenas")
    clean = []
    for t in texts:
        if not isinstance(t, str) or not t.strip():
            raise ApiError("Cada texto debe ser una cadena no vacía")
        if len(t) > 5000:
            raise ApiError("Cada texto debe tener como máximo 5000 caracteres")
        clean.append(t.strip())
    from .predictor import predict

    try:
        results, model = predict(clean)
    except FileNotFoundError as exc:
        raise ApiError(str(exc), 503) from None
    body = {"model": model, "results": results}
    if len(results) == 1:
        body.update(results[0])
    return jsonify(body)


# ------------------------------------------------------------------------------- Entrega 2
def _circle(lat, lon, radius):
    return {"$geoWithin": {"$centerSphere": [[lon, lat], radius / EARTH_RADIUS_M]}}


@app.get("/live/hotspots")
def live_hotspots():
    a = args()
    lat, lon = lat_lon(a)
    radius = number(a, "radius", default=10000, lo=100, hi=100000)
    window = date_param(a, "window")
    only = str(a.get("only_hotspots", "true")).lower() != "false"
    limit = number(a, "limit", default=200, lo=1, hi=2000, cast=int)
    coll = database().windows
    if window is None:
        latest = coll.find_one(sort=[("window_start", -1)])
        if not latest:
            return jsonify({"window": None, "count": 0, "items": [],
                            "message": "Aún no hay ventanas: inicie el productor"})
        window_q = {"window_start": latest["window_start"]}
    else:
        window_q = {"window_start": {"$lte": window}, "window_end": {"$gt": window}}
    query = {"center": _circle(lat, lon, radius), **window_q}
    if only:
        query["hotspot"] = True
    items = list(coll.find(query, {"_id": 0}).sort("pct_negative", -1).limit(limit))
    return jsonify({"center": [lat, lon], "radius_m": radius, "window": window_q,
                    "rule": {"delta_pp": config.HOTSPOT_DELTA_PP, "min_reviews": config.HOTSPOT_MIN_REVIEWS},
                    "count": len(items), "items": items})


@app.route("/alerts", methods=["GET", "POST"])
def alerts():
    a = args()
    if a.get("polygon"):
        geo_q = {"location": {"$geoWithin": {"$geometry": polygon(a.get("polygon"))}}}
        where = {"polygon": True}
    else:
        lat, lon = lat_lon(a)
        radius = number(a, "radius", default=5000, lo=50, hi=100000)
        geo_q = {"location": _circle(lat, lon, radius)}
        where = {"center": [lat, lon], "radius_m": radius}
    min_prob = number(a, "min_prob", default=0, lo=0, hi=1)
    active = str(a.get("active", "true")).lower() != "false"
    limit = number(a, "limit", default=200, lo=1, hi=2000, cast=int)
    query = {**geo_q, "probability": {"$gte": min_prob}}
    if active:
        query["active"] = True
    items = list(database().alerts.find(query).sort("probability", -1).limit(limit))
    for it in items:
        it["business_id"] = it.pop("_id")
    return jsonify({**where, "active_only": active, "count": len(items), "items": items})


@app.get("/survival")
def survival_groups():
    rows = list(database().survival.find({}, {"curve": 0, "geometry": 0}).sort([("group_type", 1), ("n", -1)]))
    return jsonify({"count": len(rows), "items": rows})


@app.get("/survival/<path:categoria>")
def survival(categoria):
    a = args()
    gtype = text(a, "type") or ("global" if categoria in ("todos", "todos_sin_entrada_tardia") else "category")
    if gtype not in ("category", "zone", "global"):
        raise ApiError("'type' debe ser 'category', 'zone' o 'global'")
    include_small = str(a.get("include_small", "false")).lower() == "true"
    doc = database().survival.find_one({"group_type": gtype, "group": categoria})
    if not doc:
        options = database().survival.distinct("group", {"group_type": gtype})
        raise ApiError(f"No hay curva para {gtype} '{categoria}'. Opciones: {options[:50]}", 404)
    if not doc["sufficient"] and not include_small:
        doc["curve"] = []
        doc["warning"] = (f"Grupo pequeño (n={doc['n']}, eventos={doc['events']}): no se reporta la "
                          "curva para no sacar conclusiones. Use include_small=true para verla.")
    return jsonify(doc)


@app.get("/risk/near")
def risk_near():
    a = args()
    lat, lon = lat_lon(a)
    radius = number(a, "radius", default=2000, lo=50, hi=50000)
    min_prob = number(a, "min_prob", default=0, lo=0, hi=1)
    limit = number(a, "limit", default=100, lo=1, hi=1000, cast=int)
    query = {"location": {"$near": {"$geometry": {"type": "Point", "coordinates": [lon, lat]},
                                    "$maxDistance": radius}},
             "probability": {"$gte": min_prob}}
    items = list(database().risk_scores.find(query).limit(limit))
    for it in items:
        it["business_id"] = it.pop("_id")
    items.sort(key=lambda x: -x["probability"])
    return jsonify({"center": [lat, lon], "radius_m": radius, "count": len(items), "items": items})


# ------------------------------------------------------------------------------- apoyo
@app.get("/reports/markdown")
def report_markdown():
    md = db.get_meta("report").get("markdown", "Aún no hay resultados (pipeline batch en curso).")
    return Response(md, mimetype="text/markdown; charset=utf-8")


@app.get("/powerbi/hotspots")
def pbi_hotspots():
    rows = list(database().windows.find({}, {"_id": 0, "geometry": 0}).sort("window_start", -1).limit(5000))
    for r in rows:
        c = r.pop("center", None) or {"coordinates": [None, None]}
        r["lon"], r["lat"] = c["coordinates"]
    return jsonify(rows)


@app.get("/powerbi/alerts")
def pbi_alerts():
    rows = []
    for r in database().alerts.find({"active": True}).sort("probability", -1).limit(2000):
        lon, lat = r["location"]["coordinates"]
        rows.append({"business_id": r["_id"], "name": r.get("name"), "category": r.get("main_category"),
                     "probability": r["probability"], "threshold": r.get("threshold"),
                     "event_date": r.get("event_date"), "created_at": r.get("created_at"),
                     "lat": lat, "lon": lon})
    return jsonify(rows)


@app.get("/powerbi/survival")
def pbi_survival():
    rows = []
    for d in database().survival.find({"sufficient": True}):
        for p in d["curve"]:
            rows.append({"group_type": d["group_type"], "group": d["group"], **p})
    return jsonify(rows)


@app.get("/powerbi/negative_trend")
def pbi_negative_trend():
    pipeline = [{"$group": {"_id": "$window_start", "reviews": {"$sum": "$count"},
                            "negative": {"$sum": "$negative"}}}, {"$sort": {"_id": 1}}]
    rows = [{"window_start": r["_id"], "reviews": r["reviews"], "negative": r["negative"],
             "pct_negative": round(100.0 * r["negative"] / r["reviews"], 2) if r["reviews"] else None}
            for r in database().windows.aggregate(pipeline)]
    return jsonify(rows)


@app.get("/powerbi/stream_reviews")
def pbi_stream_reviews():
    since = datetime.utcnow() - timedelta(hours=number(args(), "hours", default=24, lo=1, hi=720))
    rows = list(database().reviews_stream.find({"processed_at": {"$gte": since}},
                                               {"_id": 0, "text": 0}).sort("processed_at", -1).limit(5000))
    return jsonify(rows)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
