"""Supervivencia de negocios con Kaplan-Meier (E2.5).

- Tiempo: días desde la primera reseña (edad aproximada). Evento: is_open = 0. Censura: abiertos.
- Entrada tardía (truncamiento por la izquierda): la ventana observada empieza en OBS_START; un
  negocio con primera reseña anterior entra al conjunto en riesgo con edad entry_days, y los que
  murieron antes de OBS_START no se cuentan (nunca estuvieron en riesgo en la ventana).
- Curvas: global, por categoría principal y por zona (zonas consultadas con $geoWithin).
- No se sacan conclusiones de grupos pequeños: cada curva lleva n, eventos y la bandera
  `sufficient` (n >= KM_MIN_N y eventos >= KM_MIN_EVENTS); la API oculta las insuficientes.
- Se guarda además la curva global SIN corregir la entrada tardía, para mostrar el sesgo en el informe.
"""
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from lifelines import KaplanMeierFitter
from lifelines.exceptions import StatError

from lifemap import config, db, geo
from lifemap.metrics import survival_observation

GRID_STEP_DAYS = 90


def load_businesses():
    cur = db.get_db().businesses.find(
        {"business_days": {"$exists": True}},
        {"_id": 1, "main_category": 1, "business_days": 1, "event": 1, "entry_days": 1,
         "in_window": 1, "latitude": 1, "longitude": 1, "first_review": 1, "last_review": 1})
    docs = list(cur)
    if not docs:
        raise ValueError("No hay negocios con vida calculada")
    cutoff = db.get_meta("dataset")["cutoff_date"]
    for d in docs:
        obs = survival_observation(d["first_review"], d["last_review"], d["event"], cutoff, config.OBS_START)
        d.update(km_days=obs["duration"], entry_days=obs["entry"], in_window=obs["in_window"])
    return pd.DataFrame(docs)


def ensure_zones(df):
    """Zonas: config/zones.geojson si existe; si no, rejilla 3x3 sobre la extensión de los negocios."""
    database = db.get_db()
    custom = Path("/app/config/zones.geojson")
    if custom.exists():
        fc = json.loads(custom.read_text())
        zones = [{"_id": f["properties"].get("name", f"Z{i}"), "name": f["properties"].get("name", f"Z{i}"),
                  "geometry": f["geometry"]} for i, f in enumerate(fc["features"])]
    else:
        lat_lo, lat_hi = np.percentile(df["latitude"], [2, 98])
        lon_lo, lon_hi = np.percentile(df["longitude"], [2, 98])
        zones = geo.grid_zones(float(lat_lo), float(lat_hi), float(lon_lo), float(lon_hi))
    database.zones.drop()
    database.zones.insert_many(zones)
    db.ensure_indexes()
    return zones


def zone_members(zone):
    cur = db.get_db().businesses.find(
        {"location": {"$geoWithin": {"$geometry": zone["geometry"]}}}, {"_id": 1})
    return {d["_id"] for d in cur}


def at_risk(durations, entries, t):
    return int(((entries <= t) & (durations >= t)).sum())


def fit_curve(sub, group_type, group, late_entry=True):
    sub = sub[sub["in_window"]] if late_entry else sub
    n, events = len(sub), int(sub["event"].sum()) if len(sub) else 0
    doc = {"_id": f"{group_type}:{group}", "group_type": group_type, "group": group,
           "n": n, "events": events, "censored": n - events, "late_entry_adjusted": late_entry,
           "sufficient": n >= config.KM_MIN_N and events >= config.KM_MIN_EVENTS,
           "min_n": config.KM_MIN_N, "min_events": config.KM_MIN_EVENTS, "curve": [],
           "median_days": None, "median_years": None}
    if n == 0:
        return doc
    durations = sub["km_days"].to_numpy(dtype=float)
    entries = sub["entry_days"].to_numpy(dtype=float) if late_entry else np.zeros(n)
    kmf = KaplanMeierFitter()
    try:
        kmf.fit(durations, event_observed=sub["event"].to_numpy(), entry=entries, label=str(group))
    except StatError as exc:
        doc.update(sufficient=False, fit_error=str(exc)[:300])
        return doc
    median = kmf.median_survival_time_
    if median is not None and math.isfinite(median):
        doc["median_days"] = float(median)
        doc["median_years"] = round(float(median) / 365.0, 2)
    ci = kmf.confidence_interval_survival_function_
    grid = kmf.survival_function_.index.to_numpy()
    surv = kmf.survival_function_at_times(grid).to_numpy()
    low = ci.reindex(grid, method="ffill").iloc[:, 0].to_numpy()
    high = ci.reindex(grid, method="ffill").iloc[:, 1].to_numpy()
    for t, s, lo, hi in zip(grid, surv, low, high):
        doc["curve"].append({"t_days": int(t), "t_years": round(t / 365.0, 2), "survival": round(float(s), 4),
                             "ci_low": round(float(lo), 4), "ci_high": round(float(hi), 4),
                             "at_risk": at_risk(durations, entries, t)})
    for years in (1, 3, 5, 10):
        doc[f"survival_{years}y"] = (round(float(kmf.survival_function_at_times(years * 365).iloc[0]), 4)
                                         if entries.min() <= years * 365 <= durations.max() else None)
    return doc


def run():
    df = load_businesses()
    df["entry_days"] = df["entry_days"].fillna(0)
    df["in_window"] = df["in_window"].fillna(True).astype(bool)
    docs = [fit_curve(df, "global", "todos"),
            fit_curve(df, "global", "todos_sin_entrada_tardia", late_entry=False)]
    for cat, sub in df.groupby("main_category"):
        docs.append(fit_curve(sub, "category", cat))
    for zone in ensure_zones(df):
        members = zone_members(zone)
        doc = fit_curve(df[df["_id"].isin(members)], "zone", zone["name"])
        doc["geometry"] = zone["geometry"]
        docs.append(doc)

    database = db.get_db()
    database.survival.drop()
    database.survival.insert_many(docs)
    db.ensure_indexes()
    Path(config.DATA_DIR, "reports").mkdir(parents=True, exist_ok=True)
    Path(config.DATA_DIR, "reports", "survival.json").write_text(json.dumps(
        [{k: v for k, v in d.items() if k not in ("curve", "geometry")} for d in docs], indent=2, default=str))
    ok = [d for d in docs if d["sufficient"]]
    print(f"Curvas KM: {len(docs)} ({len(ok)} con tamaño suficiente)")
    return {"curves": len(docs), "sufficient": len(ok)}
