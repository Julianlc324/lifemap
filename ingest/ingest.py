"""Ingesta y limpieza con Dask (Entrega 1, criterio E1.4).

1. Descarga automática del Yelp Open Dataset con la API de Kaggle (credencial de Jenkins).
2. Lectura particionada de business.json y review.json (bloques de 64 MB en los workers).
3. Reglas de limpieza en orden, con conteo antes/después de cada una (reporte en MongoDB y JSON).
4. Unión reseña–negocio y carga por lotes en MongoDB (insert_many de 5.000 desde cada worker).
"""
import json
import os
import time
from pathlib import Path

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.distributed import Client, wait

from lifemap import config, db, geo, steps

RAW = Path(config.DATA_DIR) / "raw"
CLEAN = Path(config.DATA_DIR) / "clean"
REPORTS = Path(config.DATA_DIR) / "reports"
BUSINESS_FILE = "yelp_academic_dataset_business.json"
REVIEW_FILE = "yelp_academic_dataset_review.json"
BLOCKSIZE = "64MB"
BATCH = 5000

# (regla, justificación) en el orden en que se aplican
BUSINESS_RULES = [
    ("coordenadas_nulas", "Sin latitud/longitud no hay GeoJSON ni consultas $near/$geoWithin."),
    ("coordenadas_fuera_de_rango", "Latitud fuera de [-90,90] o longitud fuera de [-180,180] rompe el índice 2dsphere."),
    ("fuera_del_area", "Alcance del equipo: solo el área metropolitana asignada (METRO_BBOX)."),
    ("sin_categorias", "Sin categorías no se puede asignar familia ni categoría principal."),
    ("fuera_de_la_familia", "Alcance del equipo: solo la familia de categorías asignada (CATEGORY_FAMILY)."),
    ("is_open_invalido", "is_open define evento/censura; solo se aceptan 0 o 1."),
    ("business_id_duplicado", "Un negocio duplicado contaría dos veces sus días-negocio y su cierre."),
]
REVIEW_RULES = [
    ("negocio_fuera_de_alcance", "La reseña debe unirse a un negocio limpio del área y la familia (unión reseña–negocio)."),
    ("sin_texto", "Sin texto no hay entrada para TF-IDF ni sentimiento."),
    ("estrellas_invalidas", "La etiqueta de sentimiento se deriva de 1 a 5 estrellas."),
    ("fecha_invalida", "La fecha define la vida del negocio, las ventanas y la partición en T."),
    ("review_id_duplicado", "Una reseña duplicada sesga sentimiento y conteos por ventana."),
]


# ----------------------------------------------------------------------------- descarga
def download():
    RAW.mkdir(parents=True, exist_ok=True)
    if (RAW / BUSINESS_FILE).exists() and (RAW / REVIEW_FILE).exists():
        print("Dataset ya descargado en", RAW)
        return
    if not (os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY")):
        raise SystemExit("Faltan KAGGLE_USERNAME/KAGGLE_KEY (credencial 'kaggle' de Jenkins o .env)")
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    t0 = time.time()
    print(f"Descargando {config.KAGGLE_DATASET} ...")
    import zipfile
    try:
        for name in (BUSINESS_FILE, REVIEW_FILE):
            api.dataset_download_file(config.KAGGLE_DATASET, name, path=str(RAW), quiet=False)
            zipped = RAW / f"{name}.zip"
            if zipped.exists():
                with zipfile.ZipFile(zipped) as zf:
                    zf.extractall(RAW)
                zipped.unlink()
    except Exception as exc:  # noqa: BLE001 - algunos archivos grandes solo bajan en el zip completo
        print("Descarga por archivo falló, se baja el dataset completo:", exc)
        api.dataset_download_files(config.KAGGLE_DATASET, path=str(RAW), unzip=True, quiet=False)
    if not ((RAW / BUSINESS_FILE).exists() and (RAW / REVIEW_FILE).exists()):
        raise SystemExit(f"No se encontraron {BUSINESS_FILE} y {REVIEW_FILE} en {RAW}")
    print(f"Descarga terminada en {time.time() - t0:.0f} s")


# ----------------------------------------------------------------------------- utilidades
def split_categories(raw):
    if not isinstance(raw, str) or not raw.strip():
        return []
    return [c.strip() for c in raw.split(",") if c.strip()]


def main_category(cats, family=None, generic=None):
    family = family or config.CATEGORY_FAMILY
    generic = generic if generic is not None else config.GENERIC_CATEGORIES
    for c in cats:
        if c != family and c not in generic:
            return c
    return family


def business_reasons(pdf, bbox, family):
    """Primera regla que incumple cada negocio (None si pasa todas, salvo duplicados)."""
    lat = pd.to_numeric(pdf["latitude"], errors="coerce")
    lon = pd.to_numeric(pdf["longitude"], errors="coerce")
    cats = pdf["categories"].map(split_categories)
    is_open = pd.to_numeric(pdf["is_open"], errors="coerce")
    lat_min, lat_max, lon_min, lon_max = bbox
    conditions = [
        lat.isna() | lon.isna(),
        ~lat.between(-90, 90) | ~lon.between(-180, 180),
        ~lat.between(lat_min, lat_max) | ~lon.between(lon_min, lon_max),
        cats.map(len) == 0,
        ~cats.map(lambda cs: family in cs),
        ~is_open.isin([0, 1]),
    ]
    names = [r for r, _ in BUSINESS_RULES[:-1]]
    return pd.Series(np.select(conditions, names, default=""), index=pdf.index)


def review_reasons(pdf, business_ids):
    text = pdf["text"].fillna("").astype(str).str.strip()
    stars = pd.to_numeric(pdf["stars"], errors="coerce")
    dates = pd.to_datetime(pdf["date"], errors="coerce")
    conditions = [
        ~pdf["business_id"].isin(business_ids),
        text.str.len() == 0,
        ~stars.isin([1, 2, 3, 4, 5]),
        dates.isna(),
    ]
    names = [r for r, _ in REVIEW_RULES[:-1]]
    return pd.Series(np.select(conditions, names, default=""), index=pdf.index)


def sequential_report(dataset, rules, total, reason_counts, dup_removed):
    report, current = [], int(total)
    for rule, why in rules:
        removed = int(dup_removed if rule.endswith("duplicado") else reason_counts.get(rule, 0))
        report.append({"dataset": dataset, "regla": rule, "justificacion": why,
                       "antes": current, "eliminados": removed, "despues": current - removed})
        current -= removed
    return report


# ----------------------------------------------------------------------------- negocios
def clean_business():
    cols = ["business_id", "name", "city", "state", "latitude", "longitude",
            "stars", "review_count", "is_open", "categories"]
    ddf = dd.read_json(str(RAW / BUSINESS_FILE), lines=True, blocksize=BLOCKSIZE)[cols]
    ddf = ddf.assign(_reason=ddf.map_partitions(
        business_reasons, config.METRO_BBOX, config.CATEGORY_FAMILY, meta=("_reason", "object")))
    total, counts = dd.compute(ddf.shape[0], ddf["_reason"].value_counts())
    kept = ddf[ddf["_reason"] == ""].drop(columns="_reason").compute()
    before_dup = len(kept)
    kept = kept.drop_duplicates(subset="business_id")
    report = sequential_report("business", BUSINESS_RULES, total, counts.to_dict(), before_dup - len(kept))

    kept["categories_list"] = kept["categories"].map(split_categories)
    kept["main_category"] = kept["categories_list"].map(main_category)
    kept["geohash"] = [geo.encode(a, b, config.GEOHASH_PRECISION)
                       for a, b in zip(kept["latitude"], kept["longitude"])]
    kept["is_open"] = kept["is_open"].astype(int)
    print(f"Negocios: {total} leídos -> {len(kept)} limpios ({ddf.npartitions} particiones)")
    return kept.reset_index(drop=True), report


# ----------------------------------------------------------------------------- reseñas
def clean_reviews(business):
    cols = ["review_id", "business_id", "stars", "text", "date"]
    ddf = dd.read_json(str(RAW / REVIEW_FILE), lines=True, blocksize=BLOCKSIZE)[cols]
    ids = set(business["business_id"])
    ddf = ddf.assign(_reason=ddf.map_partitions(review_reasons, ids, meta=("_reason", "object")))

    kept = ddf[ddf["_reason"] == ""].drop(columns="_reason")
    kept = kept.assign(
        text=kept["text"].str.strip(),
        stars=kept["stars"].astype(int),
        date=dd.to_datetime(kept["date"], errors="coerce"),
    )
    # Un solo recorrido del archivo: conteos por regla + reseñas válidas en memoria de los workers
    kept, total_d, counts_d = dask.persist(kept, ddf.shape[0], ddf["_reason"].value_counts())
    total, counts = dask.compute(total_d, counts_d)
    print(f"Reseñas leídas: {total} en {ddf.npartitions} particiones")
    before_dup = int(kept.shape[0].compute())
    kept = kept.drop_duplicates(subset="review_id", split_out=max(1, ddf.npartitions // 8)).persist()
    after_dup = int(kept.shape[0].compute())
    report = sequential_report("review", REVIEW_RULES, total, counts.to_dict(), before_dup - after_dup)

    if config.MAX_REVIEWS and after_dup > config.MAX_REVIEWS:
        frac = config.MAX_REVIEWS / after_dup
        kept = kept.sample(frac=frac, random_state=42)
        report.append({"dataset": "review", "regla": "muestra_aleatoria",
                       "justificacion": f"MAX_REVIEWS={config.MAX_REVIEWS} (la guía permite >= 1.000.000)",
                       "antes": after_dup, "eliminados": after_dup - int(after_dup * frac),
                       "despues": int(after_dup * frac)})

    # Unión reseña–negocio: cada reseña hereda ubicación (celda) y categoría de su negocio
    lookup = business[["business_id", "geohash", "main_category", "city"]]
    kept = kept.merge(lookup, on="business_id", how="inner")
    kept = kept.assign(
        sentiment_label=kept["stars"].map(lambda s: 0 if s <= 2 else (1 if s == 3 else 2), meta=("stars", "int64")),
        year_month=kept["date"].dt.strftime("%Y-%m"),
    )
    return kept, report


# ----------------------------------------------------------------------------- carga a MongoDB
def _insert_partition(pdf, uri, dbname, collection):
    from pymongo import MongoClient
    from pymongo.errors import BulkWriteError

    if pdf.empty:
        return pd.Series([0])
    client = MongoClient(uri)
    coll = client[dbname][collection]
    docs = pdf.rename(columns={"review_id": "_id"}).to_dict("records")
    for d in docs:
        d["review_id"] = d["_id"]
        d["date"] = d["date"].to_pydatetime()
    inserted = 0
    for i in range(0, len(docs), BATCH):
        try:
            res = coll.insert_many(docs[i:i + BATCH], ordered=False)
            inserted += len(res.inserted_ids)
        except BulkWriteError as exc:  # duplicados si se reintenta una partición
            inserted += exc.details.get("nInserted", 0)
    client.close()
    return pd.Series([inserted])


def load_business(business):
    coll = db.get_db().businesses
    coll.drop()
    docs = []
    for r in business.itertuples(index=False):
        docs.append({
            "_id": r.business_id, "business_id": r.business_id, "name": r.name,
            "city": r.city, "state": r.state,
            "location": geo.point(r.latitude, r.longitude),
            "latitude": float(r.latitude), "longitude": float(r.longitude),
            "stars": float(r.stars), "review_count": int(r.review_count), "is_open": int(r.is_open),
            "categories": list(r.categories_list), "main_category": r.main_category,
            "geohash": r.geohash,
        })
    for i in range(0, len(docs), BATCH):
        coll.insert_many(docs[i:i + BATCH], ordered=False)
    return len(docs)


def load_reviews(reviews):
    database = db.get_db()
    database.reviews.drop()
    counts = reviews.map_partitions(
        _insert_partition, config.MONGO_URI, config.MONGO_DB, "reviews", meta=(None, "int64"))
    return int(counts.sum().compute())


# ----------------------------------------------------------------------------- main
def main():
    steps.wait_for_mongo()
    if not steps.should_run("ingest"):
        return
    t0 = time.time()
    download()
    client = Client(config.DASK_SCHEDULER, timeout="60s")
    print("Dask:", client)
    try:
        business, rep_b = clean_business()
        reviews, rep_r = clean_reviews(business)
        CLEAN.mkdir(parents=True, exist_ok=True)
        business.drop(columns=["categories_list"]).to_parquet(CLEAN / "business.parquet", index=False)
        reviews = reviews.persist()
        wait(reviews)
        reviews.to_parquet(str(CLEAN / "reviews"), write_index=False, overwrite=True,
                           coerce_timestamps="us", allow_truncated_timestamps=True)  # Spark no lee ns

        n_b = load_business(business)
        n_r = load_reviews(reviews)
        db.ensure_indexes()
        dist = reviews["sentiment_label"].value_counts().compute().sort_index()
    finally:
        client.close()

    report = rep_b + rep_r
    database = db.get_db()
    database.cleaning_report.drop()
    database.cleaning_report.insert_many([dict(r, orden=i) for i, r in enumerate(report)])
    REPORTS.mkdir(parents=True, exist_ok=True)
    (REPORTS / "cleaning_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
    sentiment_dist = {config.SENTIMENT_LABELS[int(k)]: int(v) for k, v in dist.items()}
    steps.mark_done("ingest", businesses=n_b, reviews=n_r, seconds=round(time.time() - t0, 1),
                    sentiment_distribution=sentiment_dist,
                    metro=config.METRO_NAME, family=config.CATEGORY_FAMILY)
    for r in report:
        print(f"{r['dataset']:8s} {r['regla']:28s} {r['antes']:>10,d} -> {r['despues']:>10,d}")
    print(f"Cargados {n_b} negocios y {n_r} reseñas en {time.time() - t0:.0f} s; sentimiento: {sentiment_dist}")


if __name__ == "__main__":
    main()
