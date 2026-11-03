"""Variables del modelo de riesgo de cierre.

El mismo código calcula las variables en el entrenamiento (con fecha de referencia T) y en
streaming (con la fecha de evento más reciente), así entrenamiento e inferencia no se desalinean.

El "estado" de un negocio son acumulados que se pueden actualizar reseña a reseña:
  n_reviews, sent_sum, stars_sum, first_review, last_review,
  monthly = {"YYYY-MM": [reseñas, negativas]} (solo los últimos meses),
  competitors_500m (estático, calculado con $geoNear a la fecha T).
"""
import math
from datetime import date, datetime

FEATURE_NAMES = [
    "sentiment_mean",     # sentimiento medio predicho (-1 negativo, 0 neutral, +1 positivo)
    "neg_trend_6m",       # pendiente mensual del % negativo en los últimos 6 meses (pp/mes)
    "stars_mean",         # estrellas medias de las reseñas hasta la fecha de referencia
    "log_n_reviews",      # log(1 + número de reseñas)
    "age_years",          # edad aproximada: desde la primera reseña
    "days_since_last",    # recencia: días desde la última reseña
    "competitors_500m",   # competidores de la misma categoría a 500 m
]
TREND_MONTHS = 6
KEEP_MONTHS = 12


def _d(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)[:19]).date()


def month_key(value):
    d = _d(value)
    return f"{d.year:04d}-{d.month:02d}"


def _month_index(key):
    y, m = key.split("-")
    return int(y) * 12 + int(m) - 1


def trend_slope(monthly, ref_date, months=TREND_MONTHS):
    """Pendiente (mínimos cuadrados) del % negativo por mes en los últimos `months` meses."""
    ref_idx = _month_index(month_key(ref_date))
    pts = []
    for key, (n, neg) in (monthly or {}).items():
        idx = _month_index(key)
        if ref_idx - months < idx <= ref_idx and n > 0:
            pts.append((idx - ref_idx, 100.0 * neg / n))
    if len(pts) < 2:
        return 0.0
    mx = sum(x for x, _ in pts) / len(pts)
    my = sum(y for _, y in pts) / len(pts)
    var = sum((x - mx) ** 2 for x, _ in pts)
    if var == 0:
        return 0.0
    return sum((x - mx) * (y - my) for x, y in pts) / var


def compute_features(state, ref_date):
    n = max(int(state.get("n_reviews") or 0), 0)
    ref = _d(ref_date)
    first = _d(state["first_review"]) if state.get("first_review") else ref
    last = _d(state["last_review"]) if state.get("last_review") else ref
    return {
        "sentiment_mean": (state.get("sent_sum", 0.0) / n) if n else 0.0,
        "neg_trend_6m": trend_slope(state.get("monthly"), ref),
        "stars_mean": (state.get("stars_sum", 0.0) / n) if n else 0.0,
        "log_n_reviews": math.log1p(n),
        "age_years": max((ref - first).days, 0) / 365.0,
        "days_since_last": float(max((ref - last).days, 0)),
        "competitors_500m": float(state.get("competitors_500m") or 0),
    }


def update_state(state, review_date, stars, sentiment_score, is_negative):
    """Agrega una reseña nueva al estado (usado en cada micro-lote del streaming)."""
    d = _d(review_date)
    state["n_reviews"] = int(state.get("n_reviews") or 0) + 1
    state["sent_sum"] = float(state.get("sent_sum") or 0.0) + float(sentiment_score)
    state["stars_sum"] = float(state.get("stars_sum") or 0.0) + float(stars)
    if not state.get("first_review") or d < _d(state["first_review"]):
        state["first_review"] = d.isoformat()
    if not state.get("last_review") or d > _d(state["last_review"]):
        state["last_review"] = d.isoformat()
    monthly = dict(state.get("monthly") or {})
    key = month_key(d)
    n, neg = monthly.get(key, [0, 0])
    monthly[key] = [n + 1, neg + (1 if is_negative else 0)]
    newest = max(_month_index(k) for k in monthly)
    state["monthly"] = {k: v for k, v in monthly.items() if newest - _month_index(k) < KEEP_MONTHS}
    return state
